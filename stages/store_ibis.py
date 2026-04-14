from __future__ import annotations

import logging
from datetime import date

from sqlalchemy import text

from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class StoreIbis(BaseStage):
    name = 'store_ibis'
    dependencies: list[str] = ['promote_ibis']

    def run(self) -> StageResult:
        snapshot_date = date.today().isoformat()
        errors: list[str] = []

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
                    # Create store table with snapshot_date column if it doesn't exist.
                    # WHERE FALSE ensures the table is created with no rows.
                    conn.execute(text(
                        f"CREATE TABLE IF NOT EXISTS store_ibis.{table} AS "
                        f"SELECT *, CURRENT_DATE::text AS snapshot_date "
                        f"FROM ibis.{table} WHERE FALSE"
                    ))
                    # Idempotency guard: skip if this table already has a snapshot for
                    # today. Prevents duplicate rows when the full pipeline (-a) and the
                    # dedicated store_cron both run on the same day.
                    already = conn.execute(
                        text(
                            f"SELECT 1 FROM store_ibis.{table} "
                            f"WHERE snapshot_date = :d LIMIT 1"
                        ),
                        {'d': snapshot_date},
                    ).fetchone()
                    if already:
                        logger.info(
                            f"  Skipping store_ibis.{table} — already snapshotted today "
                            f"({snapshot_date})."
                        )
                        continue
                    # Append current production rows with today's snapshot date
                    conn.execute(text(
                        f"INSERT INTO store_ibis.{table} "
                        f"SELECT *, '{snapshot_date}' AS snapshot_date "
                        f"FROM ibis.{table}"
                    ))
                    logger.info(f"  Snapshotted: ibis.{table} → store_ibis.{table}")
                except Exception as exc:
                    msg = f"Failed to snapshot '{table}': {exc}"
                    logger.error(msg)
                    errors.append(msg)
                    raise

        return StageResult(
            success=len(errors) == 0,
            rows_written=len(tables),
            errors=errors,
        )
