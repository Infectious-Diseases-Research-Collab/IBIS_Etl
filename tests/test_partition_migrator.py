from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from modules.partition_migrator import (
    archive_partition,
    ensure_month_partition,
    is_partitioned,
    migrate_to_partitioned,
    retire_old_partitions,
)


# ---------------------------------------------------------------------------
# is_partitioned
# ---------------------------------------------------------------------------

def test_is_partitioned_true_for_partitioned_table():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = True
    assert is_partitioned(conn, 'store_ibis', 'baseline') is True


def test_is_partitioned_false_for_plain_table():
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = False
    assert is_partitioned(conn, 'store_ibis', 'baseline') is False


# ---------------------------------------------------------------------------
# ensure_month_partition
# ---------------------------------------------------------------------------

def test_ensure_month_partition_creates_correct_boundaries():
    conn = MagicMock()
    ensure_month_partition(conn, 'store_ibis', 'baseline', date(2026, 3, 15))

    executed_sql = str(conn.execute.call_args[0][0])
    assert 'store_ibis."baseline_y2026_m03"' in executed_sql
    assert 'PARTITION OF store_ibis."baseline"' in executed_sql
    assert "FOR VALUES FROM ('2026-03-01') TO ('2026-04-01')" in executed_sql


def test_ensure_month_partition_handles_december_year_rollover():
    conn = MagicMock()
    ensure_month_partition(conn, 'store_ibis', 'baseline', date(2026, 12, 10))

    executed_sql = str(conn.execute.call_args[0][0])
    assert 'store_ibis."baseline_y2026_m12"' in executed_sql
    assert "FOR VALUES FROM ('2026-12-01') TO ('2027-01-01')" in executed_sql


def test_ensure_month_partition_rejects_invalid_table_name():
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        ensure_month_partition(conn, 'store_ibis', 'bad;name', date(2026, 1, 1))


def test_ensure_month_partition_rejects_invalid_schema_name():
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        ensure_month_partition(conn, 'bad;schema', 'baseline', date(2026, 1, 1))


def test_ensure_month_partition_name_as_overrides_partition_naming_only():
    """name_as changes what the partition is NAMED, not what it's attached
    to — PARTITION OF must still target the table actually passed in."""
    conn = MagicMock()
    ensure_month_partition(conn, 'store_ibis', '_new_baseline', date(2026, 1, 15), name_as='baseline')

    executed_sql = str(conn.execute.call_args[0][0])
    assert '"baseline_y2026_m01"' in executed_sql
    assert 'PARTITION OF store_ibis."_new_baseline"' in executed_sql
    assert '_new_baseline_y2026_m01' not in executed_sql


def test_ensure_month_partition_rejects_invalid_name_as():
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        ensure_month_partition(conn, 'store_ibis', 'baseline', date(2026, 1, 1), name_as='bad;name')


# ---------------------------------------------------------------------------
# migrate_to_partitioned
# ---------------------------------------------------------------------------

def _fake_execute_for_migration(columns, months, old_count, new_count):
    def fake_execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if 'information_schema.columns' in sql:
            result.fetchall.return_value = columns
        elif "date_trunc('month'" in sql:
            result.fetchall.return_value = [(m,) for m in months]
        elif 'COUNT(*)' in sql and '_new_baseline' in sql:
            result.scalar.return_value = new_count
        elif 'COUNT(*)' in sql:
            result.scalar.return_value = old_count
        return result
    return fake_execute


def test_migrate_to_partitioned_creates_partitions_copies_and_swaps():
    conn = MagicMock()
    conn.execute.side_effect = _fake_execute_for_migration(
        columns=[('uniqueid', 'text'), ('value', 'text'), ('snapshot_date', 'text')],
        months=[date(2026, 1, 1), date(2026, 2, 1)],
        old_count=10, new_count=10,
    )

    migrate_to_partitioned(conn, 'store_ibis', 'baseline', 'snapshot_date')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any(
        'CREATE TABLE store_ibis."_new_baseline"' in s and 'PARTITION BY RANGE ("snapshot_date")' in s
        for s in executed_sql
    )
    assert any('"snapshot_date" DATE NOT NULL' in s for s in executed_sql)
    # Exact quoted-identifier match, not a loose substring check: the
    # naive `'baseline_y2026_m01' in s` would also match inside
    # `"_new_baseline_y2026_m01"` and silently pass even if partitions
    # were (wrongly) named after the temporary _new_ table.
    assert any('"baseline_y2026_m01"' in s for s in executed_sql)
    assert any('"baseline_y2026_m02"' in s for s in executed_sql)
    assert any('INSERT INTO store_ibis."_new_baseline"' in s for s in executed_sql)
    assert any('RENAME TO "_old_baseline"' in s for s in executed_sql)
    assert any('"_new_baseline" RENAME TO "baseline"' in s for s in executed_sql)
    assert any('DROP TABLE store_ibis."_old_baseline"' in s for s in executed_sql)


def test_migrate_to_partitioned_names_partitions_after_final_table_not_temp_table():
    """Partitions created during migration must be named using the FINAL
    table name (e.g. "baseline_y2026_m01"), never the temporary
    "_new_baseline" table they're attached to during migration — Postgres
    does not rename child partitions when the parent is renamed in the
    blue-green swap, so a wrong name here would be permanent and would
    make the very next ordinary ensure_month_partition(..., 'baseline',
    ...) call for that month fail with a Postgres partition-overlap error
    against the wrongly-named leftover."""
    conn = MagicMock()
    conn.execute.side_effect = _fake_execute_for_migration(
        columns=[('uniqueid', 'text'), ('snapshot_date', 'text')],
        months=[date(2026, 1, 1)],
        old_count=5, new_count=5,
    )

    migrate_to_partitioned(conn, 'store_ibis', 'baseline', 'snapshot_date')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any(
        '"baseline_y2026_m01"' in s and 'PARTITION OF store_ibis."_new_baseline"' in s
        for s in executed_sql
    )
    assert not any('_new_baseline_y2026_m01' in s for s in executed_sql)


def test_migrate_to_partitioned_aborts_without_swapping_on_row_count_mismatch():
    conn = MagicMock()
    conn.execute.side_effect = _fake_execute_for_migration(
        columns=[('uniqueid', 'text'), ('snapshot_date', 'text')],
        months=[date(2026, 1, 1)],
        old_count=10, new_count=5,  # mismatch
    )

    with pytest.raises(RuntimeError, match='row-count mismatch'):
        migrate_to_partitioned(conn, 'store_ibis', 'baseline', 'snapshot_date')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert not any('RENAME TO "baseline"' in s for s in executed_sql)
    assert not any('DROP TABLE store_ibis."_old_baseline"' in s for s in executed_sql)


def test_migrate_to_partitioned_handles_zero_existing_rows():
    """An empty old table (e.g. created but never snapshotted) has no
    distinct months — migration must still succeed with zero partitions
    created and zero rows copied, not error on an empty months list."""
    conn = MagicMock()
    conn.execute.side_effect = _fake_execute_for_migration(
        columns=[('uniqueid', 'text'), ('snapshot_date', 'text')],
        months=[],
        old_count=0, new_count=0,
    )

    migrate_to_partitioned(conn, 'store_ibis', 'baseline', 'snapshot_date')

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('"_new_baseline" RENAME TO "baseline"' in s for s in executed_sql)


def test_migrate_to_partitioned_rejects_invalid_identifiers():
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        migrate_to_partitioned(conn, 'store_ibis', 'bad;name', 'snapshot_date')


# ---------------------------------------------------------------------------
# archive_partition
# ---------------------------------------------------------------------------

def test_archive_partition_writes_gzip_csv_via_copy(tmp_path):
    conn = MagicMock()
    cursor = MagicMock()
    conn.connection.cursor.return_value = cursor

    archive_dir = str(tmp_path)
    file_path = archive_partition(conn, 'store_ibis', 'baseline_y2025_m01', archive_dir)

    assert file_path == os.path.join(archive_dir, 'store_ibis.baseline_y2025_m01.csv.gz')
    assert os.path.exists(file_path)
    cursor.copy_expert.assert_called_once()
    copy_sql = cursor.copy_expert.call_args[0][0]
    assert 'COPY store_ibis."baseline_y2025_m01" TO STDOUT WITH CSV HEADER' in copy_sql


def test_archive_partition_creates_archive_dir_if_missing(tmp_path):
    conn = MagicMock()
    conn.connection.cursor.return_value = MagicMock()

    archive_dir = str(tmp_path / 'nested' / 'archive')
    file_path = archive_partition(conn, 'store_ibis', 'baseline_y2025_m01', archive_dir)

    assert os.path.exists(file_path)


def test_archive_partition_rejects_invalid_identifiers(tmp_path):
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        archive_partition(conn, 'store_ibis', 'bad;name', str(tmp_path))


# ---------------------------------------------------------------------------
# retire_old_partitions
# ---------------------------------------------------------------------------

def test_retire_old_partitions_archives_and_drops_only_out_of_window_months():
    conn = MagicMock()

    def fake_execute(clause, params=None):
        sql = str(clause)
        result = MagicMock()
        if 'pg_inherits' in sql:
            result.fetchall.return_value = [
                ('baseline_y2024_m06',),  # older than cutoff -> retire
                ('baseline_y2025_m01',),  # exactly at cutoff -> keep
                ('baseline_custom',),     # doesn't match naming convention -> ignore
            ]
        return result

    conn.execute.side_effect = fake_execute

    with patch('modules.partition_migrator.archive_partition') as mock_archive:
        retired = retire_old_partitions(
            conn, 'store_ibis', 'baseline', date(2025, 1, 1), '/app/backups/store_ibis_archive'
        )

    assert retired == ['baseline_y2024_m06']
    mock_archive.assert_called_once_with(
        conn, 'store_ibis', 'baseline_y2024_m06', '/app/backups/store_ibis_archive'
    )

    executed_sql = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any('DROP TABLE store_ibis."baseline_y2024_m06"' in s for s in executed_sql)
    assert not any('DROP TABLE store_ibis."baseline_y2025_m01"' in s for s in executed_sql)
    assert not any('DROP TABLE store_ibis."baseline_custom"' in s for s in executed_sql)


def test_retire_old_partitions_returns_empty_list_when_nothing_eligible():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [('baseline_y2026_m01',)]

    with patch('modules.partition_migrator.archive_partition') as mock_archive:
        retired = retire_old_partitions(
            conn, 'store_ibis', 'baseline', date(2025, 1, 1), '/app/backups/store_ibis_archive'
        )

    assert retired == []
    mock_archive.assert_not_called()


def test_retire_old_partitions_archives_before_dropping():
    """The archive call must happen before the DROP TABLE for the same
    partition — never the reverse order."""
    conn = MagicMock()
    call_order = []
    conn.execute.side_effect = lambda clause, params=None: (
        call_order.append('pg_inherits') if 'pg_inherits' in str(clause)
        else call_order.append('drop') if 'DROP TABLE' in str(clause)
        else None
    ) or MagicMock(fetchall=MagicMock(return_value=[('baseline_y2024_m06',)]))

    def fake_archive(conn_arg, schema, table, archive_dir):
        call_order.append('archive')
        return '/fake/path.csv.gz'

    with patch('modules.partition_migrator.archive_partition', side_effect=fake_archive):
        retire_old_partitions(conn, 'store_ibis', 'baseline', date(2025, 1, 1), '/app/backups/store_ibis_archive')

    assert call_order.index('archive') < call_order.index('drop')


def test_retire_old_partitions_rejects_invalid_identifiers():
    conn = MagicMock()
    with pytest.raises(ValueError, match="Invalid table name"):
        retire_old_partitions(conn, 'store_ibis', 'bad;name', date(2025, 1, 1), '/app/backups')
