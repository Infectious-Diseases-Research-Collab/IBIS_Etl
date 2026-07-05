from unittest.mock import MagicMock, patch
from datetime import date

from stages.store_ibis import StoreIbis, _cutoff_month


def _make_engine(execute_side_effect):
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.side_effect = execute_side_effect
    return engine, mock_conn


def _base_side_effect(store_exists, snapshot_count=0, source_count=100, columns=None):
    """Shared fixture builder: one table 'd_participant' in ibis, configurable
    store_ibis existence/counts."""
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql and "table_schema = 'ibis'" in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'information_schema.tables' in sql and "table_schema = 'store_ibis'" in sql:
            result.scalar.return_value = 1 if store_exists else None
        elif 'information_schema.columns' in sql:
            result.fetchall.return_value = columns or [('uniqueid', 'text'), ('value', 'text')]
        elif 'COUNT(*)' in sql and 'snapshot_date' in sql:
            result.scalar.return_value = snapshot_count
        elif 'COUNT(*)' in sql:
            result.scalar.return_value = source_count
        return result
    return execute_side_effect


def test_store_ibis_creates_partitioned_table_on_first_snapshot():
    """A table with no store_ibis.<table> yet is created directly as a
    partitioned table, not the old plain CTAS trick."""
    engine, mock_conn = _make_engine(_base_side_effect(store_exists=False))
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.ensure_month_partition') as mock_ensure:
            result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert any(
        'CREATE TABLE store_ibis."d_participant"' in s and 'PARTITION BY RANGE' in s
        for s in executed_sql
    )
    mock_ensure.assert_called_once_with(mock_conn, 'store_ibis', 'd_participant', date(2026, 4, 13))


def test_store_ibis_migrates_existing_unpartitioned_table():
    engine, mock_conn = _make_engine(_base_side_effect(store_exists=True))
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=False), \
             patch('stages.store_ibis.migrate_to_partitioned') as mock_migrate, \
             patch('stages.store_ibis.ensure_month_partition'):
            result = stage.run()

    assert result.success
    mock_migrate.assert_called_once_with(mock_conn, 'store_ibis', 'd_participant', 'snapshot_date')


def test_store_ibis_skips_migration_when_already_partitioned():
    engine, mock_conn = _make_engine(_base_side_effect(store_exists=True))
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.migrate_to_partitioned') as mock_migrate, \
             patch('stages.store_ibis.ensure_month_partition') as mock_ensure:
            result = stage.run()

    assert result.success
    mock_migrate.assert_not_called()
    mock_ensure.assert_called_once()


def test_store_ibis_appends_snapshot_with_bound_date_param():
    engine, mock_conn = _make_engine(_base_side_effect(store_exists=True))
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.ensure_month_partition'):
            result = stage.run()

    assert result.success
    assert result.rows_written == 1

    insert_calls = [
        c for c in mock_conn.execute.call_args_list
        if 'INSERT INTO store_ibis."d_participant"' in str(c.args[0])
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1] == {'d': date(2026, 4, 13)}


def test_store_ibis_skips_already_snapshotted_today():
    engine, mock_conn = _make_engine(
        _base_side_effect(store_exists=True, snapshot_count=50, source_count=50)
    )
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.ensure_month_partition'):
            result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    assert not any('INSERT INTO store_ibis."d_participant"' in s for s in executed_sql)


def test_store_ibis_retries_partial_snapshot():
    engine, mock_conn = _make_engine(
        _base_side_effect(store_exists=True, snapshot_count=10, source_count=100)
    )
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.ensure_month_partition'):
            result = stage.run()

    assert result.success
    executed_sql = [str(c.args[0]) for c in mock_conn.execute.call_args_list]
    delete_calls = [
        c for c in mock_conn.execute.call_args_list
        if 'DELETE FROM store_ibis."d_participant"' in str(c.args[0])
    ]
    assert len(delete_calls) == 1
    assert delete_calls[0].args[1] == {'d': date(2026, 4, 13)}
    assert any('INSERT INTO store_ibis."d_participant"' in s for s in executed_sql)


def test_store_ibis_rejects_invalid_table_name():
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchall.return_value = [('bad-name!',)]

    stage = StoreIbis(config=MagicMock(), engine=engine)
    result = stage.run()

    assert not result.success
    assert any('Invalid table name' in e for e in result.errors)


def test_store_ibis_reports_formatted_error_not_bare_exception():
    def execute_side_effect(stmt, *args, **kwargs):
        sql = str(stmt)
        result = MagicMock()
        if 'information_schema.tables' in sql and "table_schema = 'ibis'" in sql:
            result.fetchall.return_value = [('d_participant',)]
        elif 'information_schema.tables' in sql and "table_schema = 'store_ibis'" in sql:
            result.scalar.return_value = 1
        elif 'COUNT(*)' in sql:
            raise RuntimeError('disk full')
        return result

    engine, mock_conn = _make_engine(execute_side_effect)
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.ensure_month_partition'):
            result = stage.run()

    assert not result.success
    assert result.rows_written == 1
    assert len(result.errors) == 1
    assert "Failed to snapshot 'd_participant'" in result.errors[0]
    assert 'disk full' in result.errors[0]


def test_cutoff_month_twelve_months_back():
    assert _cutoff_month(date(2026, 4, 13), 12) == date(2025, 4, 1)


def test_cutoff_month_handles_year_rollover():
    assert _cutoff_month(date(2026, 3, 5), 12) == date(2025, 3, 1)
    assert _cutoff_month(date(2026, 1, 5), 12) == date(2025, 1, 1)


def test_store_ibis_retires_partitions_past_retention_window():
    engine, mock_conn = _make_engine(_base_side_effect(store_exists=True))
    stage = StoreIbis(config=MagicMock(), engine=engine)

    with patch('stages.store_ibis.date') as mock_date:
        mock_date.today.return_value = date(2026, 4, 13)
        with patch('stages.store_ibis.is_partitioned', return_value=True), \
             patch('stages.store_ibis.ensure_month_partition'), \
             patch('stages.store_ibis.retire_old_partitions', return_value=['d_participant_y2025_m03']) as mock_retire:
            result = stage.run()

    assert result.success
    mock_retire.assert_called_once_with(
        mock_conn, 'store_ibis', 'd_participant', date(2025, 4, 1), '/app/backups/store_ibis_archive'
    )
