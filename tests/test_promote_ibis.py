import pytest
from unittest.mock import MagicMock, patch, call

from stages.promote_ibis import PromoteIbis


def test_promote_copies_all_gold_tables():
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # Simulate two tables in gold_ibis
    mock_conn.execute.return_value.fetchall.return_value = [
        ('d_participant',), ('d_enrollment',)
    ]

    config = MagicMock()
    stage = PromoteIbis(config=config, engine=engine)
    result = stage.run()

    assert result.success
    assert result.rows_written == 2

    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any('DROP TABLE IF EXISTS ibis.d_participant' in s for s in executed_sql)
    assert any('CREATE TABLE ibis.d_participant' in s for s in executed_sql)
    assert any('DROP TABLE IF EXISTS ibis.d_enrollment' in s for s in executed_sql)
    assert any('CREATE TABLE ibis.d_enrollment' in s for s in executed_sql)
