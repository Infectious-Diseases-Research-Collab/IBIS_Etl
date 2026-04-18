from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# System code used by the tablet software to denote a question was skipped
# via built-in skip logic.  Not defined in the XML spec but present in data.
SYSTEM_SKIP_CODE = -9


class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def drop_exact_duplicates(self) -> pd.DataFrame:
        """
        Return a new DataFrame with exact duplicate rows removed.
        Does NOT mutate self.df — the original is preserved.
        """
        # Exclude the source-tracking column from the comparison so that the
        # same record read from two different snapshot files is still treated
        # as a duplicate.
        compare_cols = [c for c in self.df.columns if c != '_source_db']
        cleaned = self.df.drop_duplicates(subset=compare_cols)
        dropped = len(self.df) - len(cleaned)
        logger.info(f"Dropped {dropped} exact duplicate rows ({len(cleaned)} remaining).")
        return cleaned

    def deduplicate_by_uniqueid(self) -> pd.DataFrame:
        """
        Deduplicate records by *uniqueid*, the system-assigned unique survey
        session identifier.  When the same uniqueid appears multiple times
        (e.g. because a field edit caused the file to be re-ingested), the
        most recently extracted copy is kept.
        """
        df = self.df.copy()

        has_uid = df['uniqueid'].notna() & (df['uniqueid'].astype(str).str.strip() != '')
        with_uid = df[has_uid].copy()
        without_uid = df[~has_uid]

        if with_uid.empty:
            logger.info("No uniqueid values found; skipping uniqueid deduplication.")
            return df

        # Sort by extracted_at descending so the newest version of each record wins
        if 'extracted_at' in with_uid.columns:
            with_uid_sorted = with_uid.sort_values('extracted_at', ascending=False)
        else:
            with_uid_sorted = with_uid
        deduped = with_uid_sorted.drop_duplicates(subset=['uniqueid'], keep='first')

        before = len(with_uid)
        after = len(deduped)
        logger.info(
            f"Deduplication by uniqueid: removed {before - after} duplicate row(s) "
            f"({after} unique records retained; {len(without_uid)} rows had no uniqueid)."
        )

        return pd.concat([deduped, without_uid], ignore_index=True)

    def filter_by_countrycode(self, expected_code: int) -> pd.DataFrame:
        """
        Retain only rows whose *countrycode* matches *expected_code*.
        Rows with a missing or non-matching countrycode are logged and dropped.

        This prevents cross-country record contamination where one country's
        tablets accidentally contain records belonging to another country.

        Returns a new DataFrame; does NOT mutate self.df.
        """
        if 'countrycode' not in self.df.columns:
            logger.warning("'countrycode' column not found; skipping country filter.")
            return self.df.copy()

        country_col = pd.to_numeric(self.df['countrycode'], errors='coerce')
        match = country_col == expected_code
        filtered = self.df[match].copy()
        dropped = len(self.df) - len(filtered)
        if dropped > 0:
            logger.warning(
                f"Dropped {dropped} row(s) whose countrycode != {expected_code} "
                f"(cross-country contamination)."
            )
        return filtered
