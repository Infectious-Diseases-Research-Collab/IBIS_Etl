from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from modules.partition_migrator import ensure_month_partition, is_partitioned, migrate_to_partitioned


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
    assert any('baseline_y2026_m01' in s for s in executed_sql)
    assert any('baseline_y2026_m02' in s for s in executed_sql)
    assert any('INSERT INTO store_ibis."_new_baseline"' in s for s in executed_sql)
    assert any('RENAME TO "_old_baseline"' in s for s in executed_sql)
    assert any('"_new_baseline" RENAME TO "baseline"' in s for s in executed_sql)
    assert any('DROP TABLE store_ibis."_old_baseline"' in s for s in executed_sql)


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
