from __future__ import annotations

import logging
import re
from datetime import date

from sqlalchemy import text

logger = logging.getLogger(__name__)


def _validate_table_name(name: str) -> str:
    """Reject names that could break SQL identifier quoting. Matches the
    _validate_table_name pattern already used in stages/promote_ibis.py and
    stages/store_ibis.py — duplicated here (not imported) so this module
    stays independent of the stages/ layer, matching the existing
    modules/incremental_writer.py convention."""
    if not re.match(r'^[a-z_][a-z0-9_]*$', name):
        raise ValueError(f"Invalid table name: '{name}'")
    return name


def is_partitioned(conn, schema: str, table: str) -> bool:
    """Return True if schema.table is a native Postgres partitioned table
    (relkind = 'p'), False if it's a plain table or doesn't exist."""
    _validate_table_name(schema)
    _validate_table_name(table)
    result = conn.execute(text("""
        SELECT c.relkind = 'p'
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema AND c.relname = :table
    """), {'schema': schema, 'table': table}).scalar()
    return bool(result)


def _month_bounds(for_date: date) -> tuple[date, date]:
    """Return (first day of for_date's month, first day of the next month)."""
    start = date(for_date.year, for_date.month, 1)
    if for_date.month == 12:
        end = date(for_date.year + 1, 1, 1)
    else:
        end = date(for_date.year, for_date.month + 1, 1)
    return start, end


def ensure_month_partition(
    conn, schema: str, table: str, for_date: date, *, name_as: str | None = None
) -> None:
    """
    Create the monthly partition of schema.table covering for_date's month,
    if it doesn't already exist. Idempotent — safe to call on every run.

    name_as: if given, the partition is NAMED using this table instead of
    *table* (which is still the PARTITION OF attach target). Needed by
    migrate_to_partitioned: during migration, partitions must be attached
    to the temporary "_new_<table>" table, but Postgres does NOT rename
    child partitions when the parent is later renamed in the blue-green
    swap — so a partition's name must already match the post-swap live
    table's naming convention *before* the swap happens, or the next
    ordinary call to ensure_month_partition(conn, schema, table, ...) for
    an already-migrated month will try to create a same-range partition
    under the correct name and fail with a Postgres "partition would
    overlap" error against the wrongly-named one left behind by migration.

    Partition boundaries are embedded as date literals (not bound
    parameters): they're computed purely from *for_date* (a date object,
    never external input), and CREATE TABLE ... PARTITION OF's FOR VALUES
    clause requires constant expressions — embedding a program-controlled
    ISO date string is simpler than relying on driver-specific behavior for
    parameterized DDL literals.
    """
    _validate_table_name(schema)
    _validate_table_name(table)
    naming_table = table
    if name_as is not None:
        _validate_table_name(name_as)
        naming_table = name_as
    start, end = _month_bounds(for_date)
    partition_name = f"{naming_table}_y{start.year}_m{start.month:02d}"
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {schema}."{partition_name}"
        PARTITION OF {schema}."{table}"
        FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')
    """))
    logger.debug(f"Ensured partition {schema}.{partition_name} exists.")


def migrate_to_partitioned(conn, schema: str, table: str, partition_col: str) -> None:
    """
    One-time migration of an existing plain table schema.table into a table
    partitioned monthly on partition_col (retyped to DATE). Blue-green swap,
    mirroring stages/promote_ibis.py's pattern: build the new table fully,
    verify its row count matches the original, only then swap names. Aborts
    without touching the original table if the row counts don't match.

    partition_col's existing column is assumed to hold a value castable to
    DATE (::date) — this project uses it for a TEXT column storing ISO date
    strings. Other columns keep their existing bare data_type from
    information_schema.columns; this is a reasonable simplification here
    specifically because store_ibis tables are pandas/CTAS-derived and use
    simple types (text/bigint/timestamp/etc.) without meaningful length
    modifiers — not a general-purpose schema-cloning utility.
    """
    _validate_table_name(schema)
    _validate_table_name(table)
    _validate_table_name(partition_col)

    cols = conn.execute(text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """), {'schema': schema, 'table': table}).fetchall()

    col_names = [c[0] for c in cols]
    col_defs = ', '.join(
        f'"{name}" DATE NOT NULL' if name == partition_col else f'"{name}" {dtype}'
        for name, dtype in cols
    )
    select_list = ', '.join(
        f'"{name}"::date' if name == partition_col else f'"{name}"'
        for name in col_names
    )
    insert_col_list = ', '.join(f'"{name}"' for name in col_names)

    new_table = f'_new_{table}'
    old_table = f'_old_{table}'

    conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{new_table}" CASCADE'))
    conn.execute(text(
        f'CREATE TABLE {schema}."{new_table}" ({col_defs}) '
        f'PARTITION BY RANGE ("{partition_col}")'
    ))

    months = conn.execute(text(
        f'SELECT DISTINCT date_trunc(\'month\', "{partition_col}"::date)::date '
        f'FROM {schema}."{table}"'
    )).fetchall()
    for (month_start,) in months:
        ensure_month_partition(conn, schema, new_table, month_start, name_as=table)

    conn.execute(text(
        f'INSERT INTO {schema}."{new_table}" ({insert_col_list}) '
        f'SELECT {select_list} FROM {schema}."{table}"'
    ))

    old_count = conn.execute(text(f'SELECT COUNT(*) FROM {schema}."{table}"')).scalar()
    new_count = conn.execute(text(f'SELECT COUNT(*) FROM {schema}."{new_table}"')).scalar()
    if old_count != new_count:
        raise RuntimeError(
            f"Partition migration row-count mismatch for {schema}.{table}: "
            f"old={old_count} new={new_count} — aborting without swapping."
        )

    conn.execute(text(f'ALTER TABLE {schema}."{table}" RENAME TO "{old_table}"'))
    conn.execute(text(f'ALTER TABLE {schema}."{new_table}" RENAME TO "{table}"'))
    conn.execute(text(f'DROP TABLE {schema}."{old_table}" CASCADE'))
    logger.info(
        f"Migrated {schema}.{table} to a partitioned table "
        f"({len(months)} partition(s), {new_count} row(s))."
    )
