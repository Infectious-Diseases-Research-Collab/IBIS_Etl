from __future__ import annotations

import logging

from sqlalchemy import text

from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class PromoteIbis(BaseStage):
    name = 'promote_ibis'
    dependencies: list[str] = ['measures_ibis']

    def run(self) -> StageResult:
        errors: list[str] = []

        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'gold_ibis' AND table_type = 'BASE TABLE'"
                )
            ).fetchall()

            tables = [r[0] for r in rows]
            logger.info(f"Promoting {len(tables)} table(s) from gold_ibis → ibis.")

            for table in tables:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS ibis.{table}'))
                    conn.execute(
                        text(f'CREATE TABLE ibis.{table} AS SELECT * FROM gold_ibis.{table}')
                    )
                    logger.info(f"  Promoted: gold_ibis.{table} → ibis.{table}")
                except Exception as exc:
                    msg = f"Failed to promote '{table}': {exc}"
                    logger.error(msg)
                    errors.append(msg)
                    raise  # triggers transaction rollback

        return StageResult(
            success=len(errors) == 0,
            rows_written=len(tables),
            errors=errors,
        )
