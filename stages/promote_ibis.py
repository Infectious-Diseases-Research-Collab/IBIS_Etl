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
                new_table = f'_new_{table}'
                old_table = f'_old_{table}'
                try:
                    # Step 1: build the new table under a temp name in ibis schema
                    conn.execute(text(f'DROP TABLE IF EXISTS ibis.{new_table}'))
                    conn.execute(text(
                        f'CREATE TABLE ibis.{new_table} AS '
                        f'SELECT * FROM gold_ibis.{table}'
                    ))

                    # Step 2: atomic rename swap — old live table → _old, new → live
                    conn.execute(text(
                        f'ALTER TABLE IF EXISTS ibis.{table} '
                        f'RENAME TO {old_table}'
                    ))
                    conn.execute(text(
                        f'ALTER TABLE ibis.{new_table} RENAME TO {table}'
                    ))

                    # Step 3: drop the old table now that the new one is live
                    conn.execute(text(f'DROP TABLE IF EXISTS ibis.{old_table}'))

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
