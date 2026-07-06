from __future__ import annotations

import logging
import os
import zlib
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

logger = logging.getLogger(__name__)

SCHEMAS = ['bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis', 'sms']

# A read-only role for reporting/dashboard tools, so they don't need the same
# DDL/write privileges as the ETL's own connection. See init_readonly_role().
READONLY_ROLE = 'ibis_readonly'


class PipelineLockError(RuntimeError):
    """Raised when a pipeline advisory lock is already held by another run."""


def create_db_engine(config) -> Engine:
    """Create a SQLAlchemy engine from the 'db' config block.
    Uses URL.create() to safely handle special characters in the password.
    """
    db = config.get('db')
    with open(db['password_secret_file']) as f:
        password = f.read().strip()
    url = URL.create(
        drivername='postgresql+psycopg2',
        username=db['user'],
        password=password,
        host=db['host'],
        port=db['port'],
        database=db['name'],
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "options": "-c statement_timeout=300000 -c lock_timeout=30000"
        },
    )


def init_schemas(engine: Engine) -> None:
    """Create all medallion schemas if they do not already exist."""
    with engine.connect() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema}'))
            logger.debug('Schema ready: %s', schema)
        conn.commit()
    logger.info('Initialised schemas: %s', SCHEMAS)


def run_migrations(engine: Engine) -> None:
    """Run all SQL migration files in sql/migrations/ in filename order (idempotent)."""
    migrations_dir = Path(__file__).parent.parent / 'sql' / 'migrations'
    sql_files = sorted(migrations_dir.glob('*.sql'))
    with engine.begin() as conn:
        for path in sql_files:
            conn.execute(text(path.read_text()))
            logger.debug('Applied migration: %s', path.name)
    if sql_files:
        logger.info('Ran %d migration(s).', len(sql_files))


def init_sms_tables(engine: Engine) -> None:
    """
    Create SMS tables from sql/sms/init_sms_schema.sql (idempotent — IF NOT
    EXISTS). sms.message_status (a view, sql/sms/message_status_view.sql)
    additionally depends on ibis.baseline, which doesn't exist until
    promote_ibis has run at least once — so it's created separately and
    conditionally. On a fresh database this is skipped with a warning; it
    self-heals automatically on a later pipeline run once ibis.baseline
    exists, since init_sms_tables runs on every startup.
    """
    sql_path = Path(__file__).parent.parent / 'sql' / 'sms' / 'init_sms_schema.sql'
    with engine.begin() as conn:
        conn.execute(text(sql_path.read_text()))

    with engine.begin() as conn:
        ibis_baseline_exists = conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'ibis' AND table_name = 'baseline'"
        )).fetchone()
        if ibis_baseline_exists:
            view_path = Path(__file__).parent.parent / 'sql' / 'sms' / 'message_status_view.sql'
            conn.execute(text(view_path.read_text()))
        else:
            logger.warning(
                "ibis.baseline does not exist yet — skipping sms.message_status "
                "view creation; it will be created automatically once "
                "promote_ibis has run at least once."
            )
    logger.debug('SMS tables ready.')


def init_readonly_role(engine: Engine, password: str) -> None:
    """
    Create (or update the password of) a read-only Postgres role with SELECT
    access to every medallion schema, plus SELECT on any table created in
    those schemas from now on. Reporting/dashboard tools should connect as
    this role instead of the ETL's own role, which otherwise is the only
    credential available and carries full DDL/write privileges everywhere —
    a least-privilege gap for anything that only ever needs to read.

    Idempotent — safe to re-run on every pipeline start (e.g. after a
    password rotation in the mounted secret file).
    """
    with engine.begin() as conn:
        exists = conn.execute(
            text('SELECT 1 FROM pg_roles WHERE rolname = :role'),
            {'role': READONLY_ROLE},
        ).scalar()
        if exists:
            conn.execute(
                text(f'ALTER ROLE "{READONLY_ROLE}" WITH LOGIN PASSWORD :pw'),
                {'pw': password},
            )
        else:
            conn.execute(
                text(f'CREATE ROLE "{READONLY_ROLE}" WITH LOGIN PASSWORD :pw'),
                {'pw': password},
            )

        for schema in SCHEMAS:
            conn.execute(text(f'GRANT USAGE ON SCHEMA {schema} TO "{READONLY_ROLE}"'))
            conn.execute(text(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{READONLY_ROLE}"'
            ))
            # Applies to tables the connecting role (the ETL's own role)
            # creates in this schema from now on, so newly promoted/snapshot
            # tables don't need a manual re-grant on every pipeline run.
            conn.execute(text(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} '
                f'GRANT SELECT ON TABLES TO "{READONLY_ROLE}"'
            ))

    logger.info("Read-only role '%s' ready with SELECT access to: %s", READONLY_ROLE, SCHEMAS)


def _lock_key(lock_name: str) -> int:
    """Deterministically map a lock name to a bigint for pg_advisory_lock.
    Must not use Python's built-in hash() — it is randomised per-process."""
    return zlib.crc32(lock_name.encode('utf-8'))


@contextmanager
def pipeline_lock(engine: Engine, lock_name: str):
    """
    Hold a Postgres session-level advisory lock named *lock_name* for the
    duration of the block, so overlapping cron invocations of the same
    pipeline (e.g. a run that overruns into the next scheduled window) can't
    execute concurrently against the same schemas.

    Uses pg_try_advisory_lock (non-blocking) rather than the blocking variant:
    a second invocation should fail fast and be retried on the next schedule,
    not queue up and pile on top of an already-slow run.

    The lock is held on a dedicated connection for the life of the block and
    is released automatically if the process dies (session-scoped), so a
    crashed run can never leave the lock stuck.

    Raises PipelineLockError if the lock is already held elsewhere.
    """
    key = _lock_key(lock_name)
    conn = engine.connect()
    try:
        acquired = conn.execute(
            text('SELECT pg_try_advisory_lock(:key)'), {'key': key}
        ).scalar()
        if not acquired:
            raise PipelineLockError(
                f"Could not acquire pipeline lock '{lock_name}' — "
                f"another run already holds it."
            )
        logger.debug("Acquired pipeline lock '%s'.", lock_name)
        try:
            yield
        finally:
            conn.execute(text('SELECT pg_advisory_unlock(:key)'), {'key': key})
            logger.debug("Released pipeline lock '%s'.", lock_name)
    finally:
        conn.close()
