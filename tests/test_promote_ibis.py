import pytest
from unittest.mock import MagicMock

from stages.promote_ibis import PromoteIbis


def _make_conn(tables, dep_views_by_table=None):
    """A connection mock that returns different fetchall() results depending
    on which query is being run — the real code issues two structurally
    different SELECTs (table listing, dependent-view lookup) per stage run,
    and a single fixed return_value can't represent both correctly."""
    dep_views_by_table = dep_views_by_table or {}
    conn = MagicMock()

    def fake_execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [(t,) for t in tables]
        elif 'pg_depend' in sql:
            result.fetchall.return_value = dep_views_by_table.get((params or {}).get('table'), [])
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = fake_execute
    return conn


def test_promote_copies_all_gold_tables():
    engine = MagicMock()
    mock_conn = _make_conn(['d_participant', 'd_enrollment'])
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    config = MagicMock()
    stage = PromoteIbis(config=config, engine=engine)
    result = stage.run()

    assert result.success
    assert result.rows_written == 2

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]

    # Identifiers must be double-quoted in every SQL statement
    assert any('ibis."_new_d_participant"' in s for s in executed_sql)
    assert any('RENAME TO "d_participant"' in s for s in executed_sql)
    assert any('ibis."_new_d_enrollment"' in s for s in executed_sql)
    assert any('RENAME TO "d_enrollment"' in s for s in executed_sql)

    # Verify the DROP _new_ precedes the CREATE (essential for idempotency)
    drop_new_idx = next(i for i, s in enumerate(executed_sql) if 'DROP TABLE IF EXISTS ibis."_new_d_participant"' in s)
    create_new_idx = next(i for i, s in enumerate(executed_sql) if 'CREATE TABLE ibis."_new_d_participant"' in s)
    assert drop_new_idx < create_new_idx, "DROP _new_ must come before CREATE _new_"


def test_promote_rejects_invalid_table_name():
    """
    A table name containing characters outside [a-z0-9_] must fail the stage
    (surfaced as a StageResult error, not an uncaught exception — an
    uncaught ValueError here would only be caught and re-stringified one
    level up by ibis.py's generic handler, losing the fact that promotion
    got partway through before failing).
    """
    engine = MagicMock()
    mock_conn = _make_conn(['d_participant; DROP TABLE ibis.baseline--'])
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    stage = PromoteIbis(config=MagicMock(), engine=engine)
    result = stage.run()

    assert not result.success
    assert any('Invalid table name' in e for e in result.errors)


def test_promote_reports_formatted_error_not_bare_exception():
    """A failure during promotion of one table must produce the formatted
    "Failed to promote '<table>': ..." message in result.errors — not be
    silently discarded by the collect-then-raise pattern this replaced."""
    engine = MagicMock()
    mock_conn = _make_conn(['d_participant'])
    mock_conn.execute.side_effect = None  # override the fixture's side_effect
    call_count = {'n': 0}

    def flaky_execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'pg_depend' in sql:
            result.fetchall.return_value = []
        elif 'CREATE TABLE' in sql:
            raise RuntimeError('disk full')
        return result

    mock_conn.execute.side_effect = flaky_execute
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    stage = PromoteIbis(config=MagicMock(), engine=engine)
    result = stage.run()

    assert not result.success
    assert result.rows_written == 1  # one table was discovered, even though it failed
    assert len(result.errors) == 1
    assert "Failed to promote 'd_participant'" in result.errors[0]
    assert 'disk full' in result.errors[0]


def test_promote_recreates_multi_column_dependent_view_only_once():
    """A view that references a promoted table through more than one column
    (e.g. sms.message_status joining on ibis.baseline.subjid and also
    selecting ibis.baseline.health_facility_ug) gets one pg_depend row per
    dependent column, not one per view. Without de-duplication, the same
    view is dropped-and-recreated by CASCADE once but CREATE OR REPLACE
    VIEW runs once per row — redundant, harmless work that should still be
    collapsed to a single recreation."""
    engine = MagicMock()
    dup_view_row = MagicMock(schema='sms', viewname='message_status', definition='SELECT 1')
    mock_conn = _make_conn(
        ['baseline'],
        dep_views_by_table={'baseline': [dup_view_row, dup_view_row]},
    )
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    stage = PromoteIbis(config=MagicMock(), engine=engine)
    result = stage.run()

    assert result.success

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    recreate_count = sum(1 for s in executed_sql if 'CREATE OR REPLACE VIEW sms."message_status"' in s)
    assert recreate_count == 1
