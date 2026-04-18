import pytest
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
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = [
        ('bad-name!',)
    ]

    stage = StoreIbis(config=MagicMock(), engine=engine)
    with pytest.raises(ValueError, match="Invalid table name"):
        stage.run()
