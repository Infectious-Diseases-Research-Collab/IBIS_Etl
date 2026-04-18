from __future__ import annotations

import logging
import re

from sqlalchemy import text

from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


def _validate_table_name(name: str) -> str:
    """Reject names that could break SQL identifier quoting."""
    if not re.match(r'^[a-z_][a-z0-9_]*$', name):
        raise ValueError(f"Invalid table name: '{name}'")
    return name


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
                _validate_table_name(table)
                new_table = f'_new_{table}'
                old_table = f'_old_{table}'
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS ibis."{new_table}"'))
                    conn.execute(text(
                        f'CREATE TABLE ibis."{new_table}" AS '
                        f'SELECT * FROM gold_ibis."{table}"'
                    ))
                    conn.execute(text(
                        f'ALTER TABLE IF EXISTS ibis."{table}" '
                        f'RENAME TO "{old_table}"'
                    ))
                    conn.execute(text(
                        f'ALTER TABLE ibis."{new_table}" RENAME TO "{table}"'
                    ))
                    conn.execute(text(f'DROP TABLE IF EXISTS ibis."{old_table}"'))

                    logger.info(f"  Promoted: gold_ibis.{table} → ibis.{table}")
                except Exception as exc:
                    msg = f"Failed to promote '{table}': {exc}"
                    logger.error(msg)
                    errors.append(msg)
                    raise

        return StageResult(
            success=len(errors) == 0,
            rows_written=len(tables),
            errors=errors,
        )
