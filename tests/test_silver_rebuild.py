from unittest.mock import patch

import pandas as pd

from modules.data_cleaner import DataCleaner
from modules.silver_rebuild import clean_full_history


def test_clean_full_history_deduplicates_by_uniqueid():
    df = pd.DataFrame({
        'uniqueid': ['a', 'a', 'b'],
        'countrycode': [2, 2, 2],
        'country': ['kenya', 'kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-02', '2026-01-01']),
    })
    cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})

    assert errors == []
    assert failed == set()
    assert len(cleaned) == 2
    # the newer (2026-01-02) copy of 'a' must win
    assert cleaned.loc[cleaned['uniqueid'] == 'a', 'extracted_at'].iloc[0] == pd.Timestamp('2026-01-02')


def test_clean_full_history_filters_by_country_code():
    df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'countrycode': [2, 1],  # 'b' belongs to a different country
        'country': ['kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })
    cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})

    assert errors == []
    assert failed == set()
    assert list(cleaned['uniqueid']) == ['a']


def test_clean_full_history_renames_custom_dedup_key():
    df = pd.DataFrame({
        'custom_id': ['x', 'x'],
        'countrycode': [2, 2],
        'country': ['kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-02']),
    })
    cleaned, errors, failed = clean_full_history(df, dedup_key='custom_id', country_code_map={'kenya': 2})

    assert errors == []
    assert failed == set()
    assert len(cleaned) == 1
    assert 'custom_id' in cleaned.columns
    assert 'uniqueid' not in cleaned.columns


def test_clean_full_history_returns_empty_and_no_errors_for_empty_input():
    df = pd.DataFrame(columns=['uniqueid', 'countrycode', 'country', 'extracted_at'])
    cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={})

    assert cleaned.empty
    assert errors == []
    assert failed == set()


def test_clean_full_history_collects_per_country_errors_without_raising():
    """A country with no entry in country_code_map must not be silently
    dropped — it passes through unfiltered (with a warning) alongside
    countries that do have a configured code, matching today's
    bronze_to_silver behavior."""
    df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'countrycode': ['not-a-number', 2],
        'country': ['broken', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })
    # 'broken' has no entry in country_code_map, so its rows pass through
    # unfiltered; 'kenya' is configured and matches countrycode 2.
    cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})
    assert sorted(cleaned['uniqueid']) == ['a', 'b']  # order depends on groupby iteration, not guaranteed
    assert errors == []
    assert failed == set()


def test_clean_full_history_keeps_unconfigured_country_rows_unfiltered():
    """A country with no country_code_map entry is not a failure — its rows
    pass through unfiltered with a warning, same as bronze_to_silver.py has
    always done. Must not appear in failed_countries."""
    df = pd.DataFrame({
        'uniqueid': ['x', 'y'],
        'countrycode': [9, 9],
        'country': ['unmapped', 'unmapped'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })
    cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})

    assert errors == []
    assert failed == set()
    assert sorted(cleaned['uniqueid']) == ['x', 'y']


def test_clean_full_history_returns_empty_with_errors_when_all_groups_raise():
    """If every per-country group raises during cleaning, all_cleaned stays
    empty and the function must fall through to the empty-result path:
    an empty DataFrame (same columns as input) plus one error message per
    country describing the failure, and that country recorded in
    failed_countries."""
    df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'countrycode': [2, 2],
        'country': ['kenya', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-02']),
    })

    with patch.object(DataCleaner, 'drop_exact_duplicates', side_effect=RuntimeError('boom')):
        cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})

    assert cleaned.empty
    assert list(cleaned.columns) == list(df.columns)
    assert len(errors) == 1
    assert errors[0] == '[kenya] Failed during cleaning: boom'
    assert failed == {'kenya'}


def test_clean_full_history_reports_failed_countries_without_dropping_others():
    """A country whose processing raises must: (a) contribute zero rows to
    cleaned_df, (b) appear in failed_countries, and (c) not prevent other
    countries from being cleaned normally. This distinction — a country
    that failed vs. a country that legitimately produced zero rows — is
    exactly what failed_countries exists to let callers tell apart."""
    df = pd.DataFrame({
        'uniqueid': ['a', 'b'],
        'countrycode': [2, 2],
        'country': ['broken', 'kenya'],
        'extracted_at': pd.to_datetime(['2026-01-01', '2026-01-01']),
    })
    original = DataCleaner.drop_exact_duplicates

    def flaky(self):
        # Fail only the 'broken' group's call — deterministic regardless of
        # groupby iteration order, since it inspects the group's own data
        # rather than counting calls.
        if 'broken' in self.df['country'].values:
            raise RuntimeError('boom')
        return original(self)

    with patch.object(DataCleaner, 'drop_exact_duplicates', flaky):
        cleaned, errors, failed = clean_full_history(df, dedup_key='uniqueid', country_code_map={'kenya': 2})

    assert failed == {'broken'}
    assert len(errors) == 1 and 'broken' in errors[0]
    assert list(cleaned['uniqueid']) == ['b']
