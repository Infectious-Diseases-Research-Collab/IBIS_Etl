import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


def test_bootstrap_creates_expected_schemas_and_tables(clean_engine):
    """init_schemas + run_migrations + init_sms_tables must succeed against
    a real Postgres and leave the expected schemas/tables in place — this
    is the pipeline's own real startup sequence (see ibis.py's main()),
    never before exercised against a real database in this test suite."""
    with clean_engine.connect() as conn:
        schemas = {
            row[0] for row in conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN "
                "('bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis', 'sms')"
            )).fetchall()
        }
        assert schemas == {
            'bronze_ibis', 'silver_ibis', 'gold_ibis', 'ibis', 'store_ibis', 'sms',
        }

        sms_tables = {
            row[0] for row in conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'sms'"
            )).fetchall()
        }
        assert 'queue' in sms_tables
        assert 'log' in sms_tables


def test_bootstrap_is_idempotent(clean_engine):
    """Running the bootstrap sequence twice against the same database must
    not raise — this is exactly what happens on every real pipeline restart."""
    from modules.db import init_schemas, init_sms_tables, run_migrations
    init_schemas(clean_engine)
    run_migrations(clean_engine)
    init_sms_tables(clean_engine)


def test_sms_message_status_view_self_heals_once_ibis_baseline_exists(clean_engine):
    """sms.message_status joins ibis.baseline, which doesn't exist until
    promote_ibis has run at least once — so on a genuinely fresh database
    (exactly what clean_engine just produced) init_sms_tables() must skip
    creating the view rather than raise UndefinedTable. Once ibis.baseline
    later appears, the very next init_sms_tables() call (run on every real
    pipeline startup) must create the view for real, not just avoid
    crashing."""
    from modules.db import init_sms_tables

    with clean_engine.connect() as conn:
        view_exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views "
            "WHERE table_schema = 'sms' AND table_name = 'message_status'"
        )).fetchone()
    assert view_exists is None, (
        "message_status should be skipped, not created, before ibis.baseline exists"
    )

    with clean_engine.begin() as conn:
        conn.execute(text(
            'CREATE TABLE ibis.baseline (subjid TEXT, health_facility_ug TEXT)'
        ))

    init_sms_tables(clean_engine)

    with clean_engine.connect() as conn:
        view_exists = conn.execute(text(
            "SELECT 1 FROM information_schema.views "
            "WHERE table_schema = 'sms' AND table_name = 'message_status'"
        )).fetchone()
    assert view_exists is not None, (
        "message_status must be created once ibis.baseline exists"
    )
