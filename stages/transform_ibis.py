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

        # The inner raise aborts the transaction on the first failing file
        # (Postgres won't run further statements in an aborted transaction
        # anyway) and rolls back via engine.begin(). The outer except exists
        # only to get back to the return below with *errors* populated —
        # letting the exception propagate to the caller would report just
        # the bare exception, discarding the "which file" context.
        try:
            with self.engine.begin() as conn:
                for sql_path in sql_files:
                    try:
                        sql = sql_path.read_text()
                        conn.execute(text(sql))
                        logger.info(f"Executed: {sql_path.name}")
                    except Exception as exc:
                        msg = f"SQL error in '{sql_path.name}': {exc}"
                        logger.error(msg)
                        errors.append(msg)
                        raise
        except Exception as exc:
            if not errors:
                errors.append(str(exc))

        return StageResult(
            success=len(errors) == 0,
            rows_written=0,
            errors=errors,
        )
