from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import pandas as pd

from modules.data_cleaner import DataCleaner
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class BronzeToSilver(BaseStage):
    name = 'bronze_to_silver'
    dependencies: list[str] = ['mdb_to_bronze']

    def run(self) -> StageResult:
        trial = self.config.get('trial')
        dedup_key = trial['dedup_key']
        country_code_map: dict[str, int] = trial.get('country_code_map', {})

        bronze_df = pd.read_sql('SELECT * FROM bronze_ibis.baseline', self.engine)

        if bronze_df.empty:
            logger.warning("bronze_ibis.baseline is empty — nothing to process.")
            return StageResult(success=True, rows_written=0)

        logger.info(f"Read {len(bronze_df)} rows from bronze_ibis.baseline.")

        errors: list[str] = []
        all_cleaned: list[pd.DataFrame] = []

        for country, group in bronze_df.groupby('country'):
            try:
                country_code = country_code_map.get(str(country))
                cleaner = DataCleaner(group.copy())

                if country_code is not None:
                    df = cleaner.filter_by_countrycode(country_code)
                    cleaner = DataCleaner(df)
                else:
                    logger.warning(f"No country code for '{country}'; skipping country filter.")
                    df = group.copy()

                df = cleaner.drop_exact_duplicates()
                cleaner = DataCleaner(df)

                if dedup_key in df.columns:
                    if dedup_key != 'uniqueid':
                        df = df.rename(columns={dedup_key: 'uniqueid'})
                        df = DataCleaner(df).deduplicate_by_uniqueid()
                        df = df.rename(columns={'uniqueid': dedup_key})
                    else:
                        df = DataCleaner(df).deduplicate_by_uniqueid()
                else:
                    logger.warning(
                        f"Dedup key '{dedup_key}' not found in data for '{country}'."
                    )

                all_cleaned.append(df)
                logger.info(f"[{country}] {len(df)} rows after deduplication.")
            except Exception as exc:
                msg = f"[{country}] Failed during silver processing: {exc}"
                logger.error(msg)
                errors.append(msg)

        if not all_cleaned:
            return StageResult(success=len(errors) == 0, rows_written=0, errors=errors)

        silver_df = pd.concat(all_cleaned, ignore_index=True)

        # Drop internal tracking columns before writing to silver
        silver_df = silver_df.drop(columns=['_source_db'], errors='ignore')

        run_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        meta = pd.DataFrame([{
            'run_uuid': run_id,
            'file_name': '(silver consolidation)',
            'file_path': '',
            'country': '(all)',
            'community': '(all)',
            'extracted_at': now,
            'last_modified': now,
            'loaded': True,
        }])

        with self.engine.begin() as conn:
            silver_df.to_sql('baseline', conn, schema='silver_ibis', if_exists='replace', index=False)
            meta.to_sql('meta', conn, schema='silver_ibis', if_exists='append', index=False)

        logger.info(f"Wrote {len(silver_df)} rows → silver_ibis.baseline.")
        return StageResult(success=len(errors) == 0, rows_written=len(silver_df), errors=errors)
