from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from modules.data_validator import DataValidator
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)

SQL_MEASURES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql', 'measures')


def _load_sql_files(directory: str) -> list[Path]:
    return sorted(Path(directory).glob('*.sql'))


class MeasuresIbis(BaseStage):
    name = 'measures_ibis'
    dependencies: list[str] = ['transform_ibis']

    def run(self) -> StageResult:
        trial = self.config.get('trial')
        country_code_map: dict[str, int] = trial.get('country_code_map', {})

        silver_df = pd.read_sql('SELECT * FROM silver_ibis.baseline', self.engine)

        if silver_df.empty:
            logger.warning("silver_ibis.baseline is empty — skipping measures.")
            return StageResult(success=True, rows_written=0)

        errors: list[str] = []
        all_reports: list[pd.DataFrame] = []

        for country, group in silver_df.groupby('country'):
            try:
                country_code = country_code_map.get(str(country))
                if country_code is None:
                    logger.warning(
                        f"[{country}] No country_code in config — countrycode mismatch check skipped."
                    )
                validator = DataValidator()
                report = validator.validate(
                    group.copy(),
                    country_code=country_code,
                    country_name=str(country),
                )
                all_reports.append(report)
            except Exception as exc:
                msg = f"[{country}] Validation failed: {exc}"
                logger.error(msg)
                errors.append(msg)

        if not all_reports:
            logger.warning("All country validations failed — skipping report write.")
            return StageResult(success=False, rows_written=0, errors=errors)

        full_report = pd.concat(all_reports, ignore_index=True)
        full_report.to_sql(
            'ds_validation_report', self.engine, schema='gold_ibis',
            if_exists='replace', index=False,
        )
        logger.info(
            f"Wrote {len(full_report)} validation issue(s) → gold_ibis.ds_validation_report."
        )

        # Run measures SQL files
        sql_files = _load_sql_files(SQL_MEASURES_DIR)
        if not sql_files:
            msg = f"No SQL files found in '{SQL_MEASURES_DIR}'."
            logger.error(msg)
            errors.append(msg)
            return StageResult(success=False, rows_written=len(full_report), errors=errors)
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
                    # Re-raise to roll back the SQL transaction. Note: ds_validation_report
                    # was already written above and will not be rolled back. A re-run
                    # will overwrite it (if_exists='replace'), so this is recoverable.
                    raise

        return StageResult(success=len(errors) == 0, rows_written=len(full_report), errors=errors)
