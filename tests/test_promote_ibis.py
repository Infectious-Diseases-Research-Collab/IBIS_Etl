import pytest
from unittest.mock import MagicMock

from stages.promote_ibis import PromoteIbis


def test_promote_copies_all_gold_tables():
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mock_conn.execute.return_value.fetchall.return_value = [
        ('d_participant',), ('d_enrollment',)
    ]

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
    """Table names containing characters outside [a-z0-9_] must raise ValueError."""
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mock_conn.execute.return_value.fetchall.return_value = [
        ('d_participant; DROP TABLE ibis.baseline--',)
    ]

    stage = PromoteIbis(config=MagicMock(), engine=engine)
    with pytest.raises(ValueError, match="Invalid table name"):
        stage.run()
