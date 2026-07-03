from __future__ import annotations

import logging
import re
from datetime import date

from sqlalchemy import text

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
        snapshot_date = date.today().isoformat()
        errors: list[str] = []
        tables: list[str] = []

        # The inner raise (in _snapshot_table) aborts the transaction on the
        # first failing table and rolls back via engine.begin(). The outer
        # except exists only to get back to the return below with *errors*
        # and *tables* populated — letting the exception propagate to the
        # caller would report just the bare exception, discarding which
        # table failed and how many were even discovered.
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
                    f"(snapshot_date={snapshot_date})."
                )

                for table in tables:
                    try:
                        self._snapshot_table(conn, table, snapshot_date)
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

    def _snapshot_table(self, conn, table: str, snapshot_date: str) -> None:
        """Append today's snapshot of ibis.<table> to store_ibis.<table>,
        skipping if already complete and repairing an incomplete prior
        attempt for the same date."""
        _validate_table_name(table)

        conn.execute(text(
            f'CREATE TABLE IF NOT EXISTS store_ibis."{table}" AS '
            f'SELECT *, CURRENT_DATE::text AS snapshot_date '
            f'FROM ibis."{table}" WHERE FALSE'
        ))
        snapshot_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM store_ibis."{table}" '
                f'WHERE snapshot_date = :d'
            ),
            {'d': snapshot_date},
        ).scalar()
        source_count = conn.execute(
            text(f'SELECT COUNT(*) FROM ibis."{table}"')
        ).scalar()

        if snapshot_count == source_count and snapshot_count > 0:
            logger.info(
                f"  Skipping store_ibis.{table} — already snapshotted today "
                f"({snapshot_date}, {snapshot_count} rows)."
            )
            return

        if snapshot_count > 0:
            logger.warning(
                f"  Removing incomplete snapshot for store_ibis.{table} "
                f"({snapshot_count}/{source_count} rows) — will retry."
            )
            conn.execute(text(
                f'DELETE FROM store_ibis."{table}" WHERE snapshot_date = :d'
            ), {'d': snapshot_date})

        conn.execute(text(
            f"INSERT INTO store_ibis.\"{table}\" "
            f"SELECT *, '{snapshot_date}' AS snapshot_date "
            f'FROM ibis."{table}"'
        ))
        logger.info(f"  Snapshotted: ibis.{table} → store_ibis.{table}")
