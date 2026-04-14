from __future__ import annotations

import logging
import os
from pathlib import Path

from sqlalchemy import text

from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

SQL_TRANSFORM_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql', 'transform')


def _load_sql_files(directory: str) -> list[Path]:
    """Return all .sql files in *directory*, sorted by filename."""
    return sorted(Path(directory).glob('*.sql'))


class TransformIbis(BaseStage):
    name = 'transform_ibis'
    dependencies: list[str] = ['bronze_to_silver']

    def run(self) -> StageResult:
        sql_files = _load_sql_files(SQL_TRANSFORM_DIR)
        if not sql_files:
            msg = f"No SQL files found in '{SQL_TRANSFORM_DIR}'."
            logger.error(msg)
            return StageResult(success=False, rows_written=0, errors=[msg])

        errors: list[str] = []

        with self.engine.begin() as conn:
            for sql_path in sql_files:
                sql = sql_path.read_text()
                try:
                    conn.execute(text(sql))
                    logger.info(f"Executed: {sql_path.name}")
                except Exception as exc:
                    msg = f"SQL error in '{sql_path.name}': {exc}"
                    logger.error(msg)
                    errors.append(msg)
                    raise  # roll back the transaction

        return StageResult(
            success=len(errors) == 0,
            rows_written=0,
            errors=errors,
        )
