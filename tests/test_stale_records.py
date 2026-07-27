"""
Unit tests for modules.stale_records.find_stale_uniqueids.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from modules.stale_records import find_stale_uniqueids


def _mock_engine():
    engine = MagicMock()
    mock_conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _fake_read_sql(meta_df, present_df, silver_df):
    def _side_effect(sql, conn, params=None):
        sql_str = str(sql)
        if 'bronze_ibis.meta' in sql_str:
            return meta_df
        if 'bronze_ibis.baseline' in sql_str:
            return present_df
        if 'silver_ibis.baseline' in sql_str:
            return silver_df
        raise AssertionError(f"unexpected query: {sql_str}")
    return _side_effect


class TestFindStaleUniqueids(unittest.TestCase):

    def test_flags_uniqueid_absent_from_last_two_syncs(self):
        meta_df = pd.DataFrame({
            'file_path': [
                'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb',
                'Extracted/Uganda/Tablet53_2026_07_15-16_41_47/IBIS_pilot.mdb',
            ],
            'run_uuid': ['run_latest', 'run_prev'],
            'last_modified': pd.to_datetime(['2026-07-24', '2026-07-15']),
        })
        present_df = pd.DataFrame({'uniqueid': ['still_here']})
        silver_df = pd.DataFrame({
            'uniqueid': ['still_here', 'deleted_one'],
            'tabletnum': ['53', '53'],
        })

        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=_fake_read_sql(meta_df, present_df, silver_df),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, {'deleted_one'})

    def test_does_not_flag_when_present_in_either_of_last_two_syncs(self):
        meta_df = pd.DataFrame({
            'file_path': [
                'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb',
                'Extracted/Uganda/Tablet53_2026_07_15-16_41_47/IBIS_pilot.mdb',
            ],
            'run_uuid': ['run_latest', 'run_prev'],
            'last_modified': pd.to_datetime(['2026-07-24', '2026-07-15']),
        })
        present_df = pd.DataFrame({'uniqueid': ['seen_recently']})
        silver_df = pd.DataFrame({
            'uniqueid': ['seen_recently'],
            'tabletnum': ['53'],
        })

        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=_fake_read_sql(meta_df, present_df, silver_df),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, set())

    def test_skips_tablet_with_fewer_than_two_syncs(self):
        meta_df = pd.DataFrame({
            'file_path': [
                'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb',
            ],
            'run_uuid': ['run_only'],
            'last_modified': pd.to_datetime(['2026-07-24']),
        })
        present_df = pd.DataFrame({'uniqueid': []})
        silver_df = pd.DataFrame({
            'uniqueid': ['new_tablet_record'],
            'tabletnum': ['53'],
        })

        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=_fake_read_sql(meta_df, present_df, silver_df),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, set())

    def test_uses_most_recent_two_regardless_of_gap_between_them(self):
        # A quarantined sync never writes a bronze_ibis.meta row, so the
        # function only ever sees successful syncs, however unevenly spaced.
        # This confirms it still only compares against the 2 most recent
        # by last_modified, not all rows it happens to see.
        meta_df = pd.DataFrame({
            'file_path': [
                'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb',
                'Extracted/Uganda/Tablet53_2026_06_02-11_36_05/IBIS_pilot.mdb',
                'Extracted/Uganda/Tablet53_2026_05_13-10_01_31/IBIS_pilot.mdb',
            ],
            'run_uuid': ['run_latest', 'run_middle', 'run_oldest'],
            'last_modified': pd.to_datetime(['2026-07-24', '2026-06-02', '2026-05-13']),
        })
        # Not present in the 2 most recent syncs' bronze data.
        present_df = pd.DataFrame({'uniqueid': []})
        silver_df = pd.DataFrame({
            'uniqueid': ['old_record'],
            'tabletnum': ['53'],
        })

        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=_fake_read_sql(meta_df, present_df, silver_df),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, {'old_record'})

    def test_returns_empty_set_on_query_failure(self):
        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=Exception('connection refused'),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, set())

    def test_returns_empty_set_on_invalid_table_name(self):
        engine = _mock_engine()
        stale = find_stale_uniqueids(engine, 'baseline; DROP TABLE users')
        self.assertEqual(stale, set())

    def test_normalizes_float_suffix_tabletnum_before_matching(self):
        meta_df = pd.DataFrame({
            'file_path': [
                'Extracted/Uganda/Tablet53_2026_07_24-16_25_25/IBIS_pilot.mdb',
                'Extracted/Uganda/Tablet53_2026_07_15-16_41_47/IBIS_pilot.mdb',
            ],
            'run_uuid': ['run_latest', 'run_prev'],
            'last_modified': pd.to_datetime(['2026-07-24', '2026-07-15']),
        })
        present_df = pd.DataFrame({'uniqueid': ['still_here']})
        # tabletnum has the float-suffix artifact pandas adds when a column
        # round-trips through float64 — must still match tablet '53'.
        silver_df = pd.DataFrame({
            'uniqueid': ['still_here', 'deleted_one'],
            'tabletnum': ['53.0', '53.0'],
        })

        engine = _mock_engine()
        with patch(
            'modules.stale_records.pd.read_sql',
            side_effect=_fake_read_sql(meta_df, present_df, silver_df),
        ):
            stale = find_stale_uniqueids(engine, 'baseline')

        self.assertEqual(stale, {'deleted_one'})


if __name__ == '__main__':
    unittest.main()
