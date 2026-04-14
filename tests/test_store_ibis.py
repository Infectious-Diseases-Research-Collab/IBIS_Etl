import pytest
from unittest.mock import MagicMock, patch
from datetime import date

from stages.store_ibis import StoreIbis


def test_store_ibis_appends_snapshot_with_date():
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql:
            result.fetchall.return_value = [('d_participant',), ('d_enrollment',)]
        elif 'snapshot_date' in sql and 'LIMIT 1' in sql:
            result.fetchone.return_value = None  # not yet snapshotted today
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    mock_conn.execute.side_effect = execute_side_effect

    config = MagicMock()
    stage = StoreIbis(config=config, engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert result.success
    assert result.rows_written == 2

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any('CREATE TABLE IF NOT EXISTS store_ibis.d_participant' in s for s in executed_sql)
    assert any("'2026-04-13'" in s for s in executed_sql)
    assert any('INSERT INTO store_ibis.d_participant' in s for s in executed_sql)


def test_store_ibis_skips_already_snapshotted_today():
    """If a table already has a snapshot for today, that table is not inserted again."""
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # One table in ibis schema
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql and 'ibis' in sql and 'store_ibis' not in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'snapshot_date' in sql and 'LIMIT 1' in sql:
            # Simulate: already snapshotted today
            result.fetchone.return_value = (1,)
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    mock_conn.execute.side_effect = execute_side_effect

    config = MagicMock()
    stage = StoreIbis(config=config, engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    # INSERT should NOT have been called since today is already snapshotted
    assert not any('INSERT INTO store_ibis.d_participant' in s for s in executed_sql)
