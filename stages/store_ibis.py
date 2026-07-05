from __future__ import annotations

import logging
import re
from datetime import date

from sqlalchemy import text

from modules.partition_migrator import ensure_month_partition, is_partitioned, migrate_to_partitioned
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


def _validate_table_name(name: str) -> str:
    """Reject names that could break SQL identifier quoting."""
    if not re.match(r'^[a-z_][a-z0-9_]*$', name):
        raise ValueError(f"Invalid table name: '{name}'")
    return name


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

        ensure_month_partition(conn, 'store_ibis', table, today)

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
