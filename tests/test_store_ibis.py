from unittest.mock import MagicMock, patch
from datetime import date

from stages.store_ibis import StoreIbis


def _make_engine(execute_side_effect):
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.side_effect = execute_side_effect
    return engine, mock_conn


def test_store_ibis_appends_snapshot_with_date():
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',), ('d_enrollment',)]
        elif 'COUNT(*)' in sql and 'snapshot_date' in sql:
            result.scalar.return_value = 0
        elif 'COUNT(*)' in sql:
            result.scalar.return_value = 100
        else:
            result.fetchall.return_value = []
        return result

    engine, mock_conn = _make_engine(execute_side_effect)
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert result.success
    assert result.rows_written == 2

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    # Identifiers must be double-quoted
    assert any('store_ibis."d_participant"' in s for s in executed_sql)
    assert any("'2026-04-13'" in s for s in executed_sql)
    assert any('INSERT INTO store_ibis."d_participant"' in s for s in executed_sql)


def test_store_ibis_skips_already_snapshotted_today():
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'COUNT(*)' in sql:
            result.scalar.return_value = 50
        else:
            result.fetchall.return_value = []
        return result

    engine, mock_conn = _make_engine(execute_side_effect)
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert not any('INSERT INTO store_ibis."d_participant"' in s for s in executed_sql)


def test_store_ibis_retries_partial_snapshot():
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'COUNT(*)' in sql and 'snapshot_date' in sql:
            result.scalar.return_value = 10
        elif 'COUNT(*)' in sql:
            result.scalar.return_value = 100
        else:
            result.fetchall.return_value = []
        return result

    engine, mock_conn = _make_engine(execute_side_effect)
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any('DELETE FROM store_ibis."d_participant"' in s for s in executed_sql)
    assert any('INSERT INTO store_ibis."d_participant"' in s for s in executed_sql)


def test_store_ibis_rejects_invalid_table_name():
    """
    A table name containing characters outside [a-z0-9_] must fail the stage
    (surfaced as a StageResult error, not an uncaught exception — an
    uncaught ValueError here would only be caught and re-stringified one
    level up by ibis.py's generic handler, losing the "which table, how
    many discovered" context).
    """
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = [
        ('bad-name!',)
    ]

    stage = StoreIbis(config=MagicMock(), engine=engine)
    result = stage.run()

    assert not result.success
    assert any('Invalid table name' in e for e in result.errors)


def test_store_ibis_reports_formatted_error_not_bare_exception():
    """A failure snapshotting one table must produce the formatted
    "Failed to snapshot '<table>': ..." message in result.errors — not be
    silently discarded by the collect-then-raise pattern this replaced."""
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'CREATE TABLE IF NOT EXISTS' in sql:
            raise RuntimeError('disk full')
        return result

    engine, mock_conn = _make_engine(execute_side_effect)
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert not result.success
    assert result.rows_written == 1  # one table was discovered, even though it failed
    assert len(result.errors) == 1
    assert "Failed to snapshot 'd_participant'" in result.errors[0]
    assert 'disk full' in result.errors[0]
