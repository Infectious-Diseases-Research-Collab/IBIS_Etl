from __future__ import annotations

import logging
import re
from datetime import date

from sqlalchemy import text

from modules.partition_migrator import (
    ensure_month_partition,
    is_partitioned,
    migrate_to_partitioned,
    retire_old_partitions,
)
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

RETENTION_MONTHS = 12
ARCHIVE_DIR = '/app/backups/store_ibis_archive'


def _cutoff_month(today: date, months: int) -> date:
    """First day of the month `months` calendar months before today's month.

    Built via today.replace(...) rather than the bare date(...) constructor
    so this keeps working when callers patch the module-level `date` symbol
    (as stages/store_ibis's own tests do to freeze date.today()) — replace()
    is an instance method resolved on the real `today` object, not a lookup
    of the (possibly patched) class."""
    month_index = (today.year * 12 + (today.month - 1)) - months
    year, month = divmod(month_index, 12)
    return today.replace(year=year, month=month + 1, day=1)


def _validate_table_name(name: str) -> str:
    """Reject names that could break SQL identifier quoting."""
    if not re.match(r'^[a-z_][a-z0-9_]*$', name):
        raise ValueError(f"Invalid table name: '{name}'")
    return name


def _reconcile_columns(conn, table: str) -> None:
    """
    store_ibis.<table> is append-only and never rebuilt — if ibis.<table>
    gains a column after store_ibis.<table> was first created, the append
    would otherwise fail with a column-count mismatch. Add any column
    present in ibis.<table> but missing from store_ibis.<table> before
    every snapshot attempt. Adding a column to a partitioned parent table
    in Postgres automatically applies to all its existing child partitions
    too, so this is safe to run against an already-partitioned table.
    """
    ibis_cols = conn.execute(text(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'ibis' AND table_name = :t"
    ), {'t': table}).fetchall()
    store_cols = {
        row[0] for row in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'store_ibis' AND table_name = :t"
        ), {'t': table}).fetchall()
    }
    for col_name, data_type in ibis_cols:
        if col_name not in store_cols:
            conn.execute(text(
                f'ALTER TABLE store_ibis."{table}" ADD COLUMN IF NOT EXISTS "{col_name}" {data_type}'
            ))
            logger.info(
                f"  Added missing column '{col_name}' to store_ibis.{table} "
                f"(schema drift reconciliation)."
            )


class StoreIbis(BaseStage):
    name = 'store_ibis'
    dependencies: list[str] = ['promote_ibis']

    def run(self) -> StageResult:
        today = date.today()
        errors: list[str] = []
        tables: list[str] = []

        # See transform_ibis.py for why this is try/except-wrapped rather
        # than left to propagate: the inner raise aborts the transaction on
        # the first failing table, and the outer except gets back to the
        # return below with *errors* and *tables* populated instead of
        # losing that detail to a bare caught-and-rewrapped exception upstream.
        try:
            with self.engine.begin() as conn:
                rows = conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'ibis' AND table_type = 'BASE TABLE'"
                    )
                ).fetchall()

                tables = [r[0] for r in rows]
                logger.info(
                    f"Snapshotting {len(tables)} table(s) from ibis → store_ibis "
                    f"(snapshot_date={today.isoformat()})."
                )

                for table in tables:
                    try:
                        self._snapshot_table(conn, table, today)
                    except Exception as exc:
                        msg = f"Failed to snapshot '{table}': {exc}"
                        logger.error(msg)
                        errors.append(msg)
                        raise
        except Exception as exc:
            if not errors:
                errors.append(str(exc))

        return StageResult(
            success=len(errors) == 0,
            rows_written=len(tables),
            errors=errors,
        )

    def _snapshot_table(self, conn, table: str, today: date) -> None:
        """Append today's snapshot of ibis.<table> to store_ibis.<table>
        (a table partitioned monthly by snapshot_date), skipping if already
        complete and repairing an incomplete prior attempt for the same date.
        A brand-new table is created directly as partitioned; an existing
        unpartitioned table is migrated once, automatically."""
        _validate_table_name(table)

        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'store_ibis' AND table_name = :t"
        ), {'t': table}).scalar()

        if not exists:
            self._create_partitioned_table(conn, table)
        elif not is_partitioned(conn, 'store_ibis', table):
            logger.warning(f"  Migrating store_ibis.{table} to a partitioned table...")
            migrate_to_partitioned(conn, 'store_ibis', table, 'snapshot_date')
            logger.info(f"  Migration complete: store_ibis.{table} is now partitioned.")

        _reconcile_columns(conn, table)

        ensure_month_partition(conn, 'store_ibis', table, today)

        cutoff = _cutoff_month(today, RETENTION_MONTHS)
        retired = retire_old_partitions(conn, 'store_ibis', table, cutoff, ARCHIVE_DIR)
        if retired:
            logger.info(f"  Retired {len(retired)} partition(s) for store_ibis.{table}: {retired}")

        snapshot_count = conn.execute(
            text(f'SELECT COUNT(*) FROM store_ibis."{table}" WHERE snapshot_date = :d'),
            {'d': today},
        ).scalar()
        source_count = conn.execute(
            text(f'SELECT COUNT(*) FROM ibis."{table}"')
        ).scalar()

        if snapshot_count == source_count and snapshot_count > 0:
            logger.info(
                f"  Skipping store_ibis.{table} — already snapshotted today "
                f"({today.isoformat()}, {snapshot_count} rows)."
            )
            return

        if snapshot_count > 0:
            logger.warning(
                f"  Removing incomplete snapshot for store_ibis.{table} "
                f"({snapshot_count}/{source_count} rows) — will retry."
            )
            conn.execute(text(
                f'DELETE FROM store_ibis."{table}" WHERE snapshot_date = :d'
            ), {'d': today})

        conn.execute(text(
            f'INSERT INTO store_ibis."{table}" SELECT *, :d AS snapshot_date FROM ibis."{table}"'
        ), {'d': today})
        logger.info(f"  Snapshotted: ibis.{table} → store_ibis.{table}")

    def _create_partitioned_table(self, conn, table: str) -> None:
        """Create store_ibis.<table> for the first time, directly as a table
        partitioned monthly by snapshot_date — mirrors ibis.<table>'s current
        columns, same as the old CREATE TABLE ... AS SELECT ... WHERE FALSE
        trick, but with an explicit PARTITION BY clause and a proper DATE
        snapshot_date column (the old version cast it to text)."""
        cols = conn.execute(text(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'ibis' AND table_name = :t ORDER BY ordinal_position"
        ), {'t': table}).fetchall()
        col_defs = ', '.join(f'"{c}" {t}' for c, t in cols)
        conn.execute(text(
            f'CREATE TABLE store_ibis."{table}" ({col_defs}, snapshot_date DATE NOT NULL) '
            f'PARTITION BY RANGE (snapshot_date)'
        ))
        logger.info(f"  Created store_ibis.{table} as a new partitioned table.")
