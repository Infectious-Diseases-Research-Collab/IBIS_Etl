from __future__ import annotations

import logging

import pandas as pd

from modules.data_cleaner import DataCleaner

logger = logging.getLogger(__name__)


def clean_full_history(
    bronze_df: pd.DataFrame,
    dedup_key: str,
    country_code_map: dict[str, int],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Clean and deduplicate a full (or partial) bronze DataFrame using the
    same per-country logic bronze_to_silver has always used: country-code
    filtering, exact-duplicate removal, then keep-latest-by-extracted_at
    deduplication on dedup_key. Pure function — no DB I/O — so both the
    normal incremental path (on a new batch) and --full-rebuild /
    reconciliation (on all of bronze) can call it identically.

    Returns (cleaned_df, errors); errors are per-country failures that
    don't stop other countries from being processed (matches existing
    bronze_to_silver behavior).
    """
    if bronze_df.empty:
        return bronze_df, []

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
                logger.warning(
                    f"[{country}] No country code configured; keeping rows unfiltered."
                )
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
                logger.warning(f"[{country}] Dedup key '{dedup_key}' not found.")

            all_cleaned.append(df)
        except Exception as exc:
            msg = f"[{country}] Failed during cleaning: {exc}"
            logger.error(msg)
            errors.append(msg)

    if not all_cleaned:
        return bronze_df.iloc[0:0], errors

    cleaned = pd.concat(all_cleaned, ignore_index=True)
    return cleaned.drop(columns=['_source_db'], errors='ignore'), errors
