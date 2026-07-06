from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from modules.db import SCHEMAS, init_schemas, init_sms_tables, run_migrations


@pytest.fixture(scope='session')
def postgres_container():
    """One throwaway Postgres 16 container for the whole integration test
    session — container startup is the slow part, so this is started once,
    not per test."""
    with PostgresContainer('postgres:16') as container:
        yield container


@pytest.fixture
def clean_engine(postgres_container):
    """
    A SQLAlchemy engine pointed at the shared container, with every schema
    this pipeline owns dropped and rebuilt from scratch via the pipeline's
    own real bootstrap (init_schemas / run_migrations / init_sms_tables) —
    the same sequence ibis.py's main() runs on every real startup. Each
    test using this fixture starts from a known-clean, fully-migrated
    schema; drop-and-recreate is used instead of a rolled-back transaction
    because several stages (promote_ibis, store_ibis) run their own
    multi-statement engine.begin() transactions and DDL swaps internally,
    which don't compose with an outer test-level transaction.
    """
    engine = create_engine(postgres_container.get_connection_url())
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
    init_schemas(engine)
    run_migrations(engine)
    init_sms_tables(engine)
    yield engine
    engine.dispose()
