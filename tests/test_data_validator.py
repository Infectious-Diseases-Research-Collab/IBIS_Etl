"""
Unit tests for DataValidator duplicate / related identity checks.
"""
from __future__ import annotations

import unittest
import pandas as pd
from modules.data_validator import DataValidator


def _validator() -> DataValidator:
    return DataValidator()


class TestNormalizePhone(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_strips_kenya_country_code(self):
        self.assertEqual(self.v._normalize_phone('254712345678'), '712345678')

    def test_strips_uganda_country_code(self):
        self.assertEqual(self.v._normalize_phone('256712345678'), '712345678')

    def test_strips_leading_zero(self):
        self.assertEqual(self.v._normalize_phone('0712345678'), '712345678')

    def test_strips_plus_sign(self):
        self.assertEqual(self.v._normalize_phone('+254712345678'), '712345678')

    def test_strips_spaces_and_dashes(self):
        self.assertEqual(self.v._normalize_phone('0712-345 678'), '712345678')

    def test_unrecognised_pattern_returned_as_is(self):
        self.assertEqual(self.v._normalize_phone('123'), '123')


class TestDuplicateSubjid(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_duplicate_subjid(self):
        df = pd.DataFrame({'subjid': ['S001', 'S001', 'S002'], 'consent': [1, 1, 1]})
        issues = self.v._check_duplicate_subjid(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'duplicate_subjid')
        self.assertEqual(issues[0]['record_count'], 2)

    def test_no_issue_when_all_unique(self):
        df = pd.DataFrame({'subjid': ['S001', 'S002', 'S003'], 'consent': [1, 1, 1]})
        self.assertEqual(self.v._check_duplicate_subjid(df), [])

    def test_flags_duplicate_regardless_of_consent(self):
        # S001 appears twice — one record not yet consented, still a duplicate
        df = pd.DataFrame({'subjid': ['S001', 'S001', 'S002'], 'consent': [1, 0, 1]})
        issues = self.v._check_duplicate_subjid(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['record_count'], 2)

    def test_skips_when_column_absent(self):
        df = pd.DataFrame({'other': [1, 2, 3]})
        self.assertEqual(self.v._check_duplicate_subjid(df), [])


class TestDuplicatePhone(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_same_number_different_format(self):
        df = pd.DataFrame({'mobile_number': ['0712345678', '+254712345678']})
        issues = self.v._check_duplicate_phone(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'duplicate_phone')

    def test_no_issue_when_all_unique(self):
        df = pd.DataFrame({'mobile_number': ['0712345678', '0723456789']})
        self.assertEqual(self.v._check_duplicate_phone(df), [])

    def test_skips_when_no_phone_column(self):
        df = pd.DataFrame({'other': [1, 2]})
        self.assertEqual(self.v._check_duplicate_phone(df), [])


class TestOneDigitDiffPairsBlocking(unittest.TestCase):
    """
    _one_digit_diff_pairs replaced an O(n^2) full pairwise scan with a
    digit-masking blocking technique to make _check_similar_phones scale.
    These compare its output against a brute-force reference implementation
    on randomized data to confirm the optimization is lossless — the whole
    point of the blocking key is that every true match still shares a
    bucket, so it must find exactly the same pairs, just with less work.
    """

    @staticmethod
    def _brute_force(phones, indices):
        results = []
        for i in range(len(phones)):
            for j in range(i + 1, len(phones)):
                if sum(a != b for a, b in zip(phones[i], phones[j])) == 1:
                    results.append((indices[i], indices[j], phones[i], phones[j]))
        return results

    def _assert_matches_brute_force(self, phones):
        indices = list(range(len(phones)))
        v = _validator()
        blocked = {(min(a, b), max(a, b)) for a, b, _, _ in v._one_digit_diff_pairs(phones, indices)}
        brute = {(min(a, b), max(a, b)) for a, b, _, _ in self._brute_force(phones, indices)}
        self.assertEqual(blocked, brute)

    def test_matches_brute_force_on_crafted_cases(self):
        self._assert_matches_brute_force([
            '712345678', '712345679',  # 1-digit diff
            '712345678', '712345678',  # exact dup — must NOT be flagged here
            '712345600',               # 2-digit diff from the first
            '999999999',               # unrelated
        ])

    def test_matches_brute_force_on_random_data(self):
        import random
        rng = random.Random(42)
        for _ in range(20):
            n = rng.randint(2, 60)
            phones = [''.join(rng.choice('0123456789') for _ in range(9)) for _ in range(n)]
            self._assert_matches_brute_force(phones)

    def test_empty_and_singleton_return_nothing(self):
        v = _validator()
        self.assertEqual(v._one_digit_diff_pairs([], []), [])
        self.assertEqual(v._one_digit_diff_pairs(['712345678'], [0]), [])


class TestSimilarPhones(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_single_digit_difference(self):
        df = pd.DataFrame({'mobile_number': ['712345678', '712345679']})
        issues = self.v._check_similar_phones(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'similar_phone')
        self.assertEqual(issues[0]['record_count'], 1)

    def test_no_issue_when_numbers_differ_by_two_digits(self):
        df = pd.DataFrame({'mobile_number': ['712345678', '712345600']})
        self.assertEqual(self.v._check_similar_phones(df), [])

    def test_no_issue_for_identical_numbers(self):
        # Identical numbers are handled by duplicate_phone, not similar_phone
        df = pd.DataFrame({'mobile_number': ['712345678', '712345678']})
        self.assertEqual(self.v._check_similar_phones(df), [])

    def test_ignores_different_length_numbers(self):
        df = pd.DataFrame({'mobile_number': ['71234567', '712345678']})
        self.assertEqual(self.v._check_similar_phones(df), [])


class TestDuplicateName(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_case_insensitive_duplicate(self):
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru', 'alice wanjiru', 'Bob Otieno']})
        issues = self.v._check_duplicate_name(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'duplicate_name')
        self.assertEqual(issues[0]['record_count'], 2)

    def test_no_issue_when_all_unique(self):
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru', 'Bob Otieno']})
        self.assertEqual(self.v._check_duplicate_name(df), [])

    def test_skips_when_no_name_column(self):
        df = pd.DataFrame({'other': ['a', 'b']})
        self.assertEqual(self.v._check_duplicate_name(df), [])


class TestSimilarNamePairsDobBlocking(unittest.TestCase):
    """
    _similar_name_pairs_blocked_by_dob replaced one O(n^2) rapidfuzz.cdist
    call over an entire country with per-DOB-group comparisons, relying on
    the fact that a flagged pair always requires an exact DOB match. These
    confirm that's actually lossless by comparing against a brute-force
    reference (full scan, then filter by DOB) on randomized data.
    """

    @staticmethod
    def _brute_force_pairs_matching_dob(v, norm, dob_series):
        full = v._similar_name_pairs_full_scan(norm)
        return {
            (a, b) for _, _, a, b, _ in full
            if not pd.isna(dob_series.get(a)) and not pd.isna(dob_series.get(b))
            and dob_series.get(a) == dob_series.get(b)
        }

    def test_matches_brute_force_on_random_data(self):
        import random
        rng = random.Random(7)
        first = ['Alice', 'Alicia', 'Bob', 'Bobby', 'Carol', 'Caroline', 'David', 'Davide']
        last = ['Wanjiru', 'Wanjeru', 'Otieno', 'Otieno', 'Mukasa', 'Mukasa']
        dobs = pd.to_datetime(['2000-01-01', '2000-01-02', '1995-06-15', '1988-12-25'])

        for _ in range(15):
            n = rng.randint(2, 25)
            names = [f"{rng.choice(first)} {rng.choice(last)}" for _ in range(n)]
            norm = pd.Series([n.strip().lower() for n in names])
            dob_series = pd.Series(rng.choices(list(dobs) + [pd.NaT], k=n))

            v = _validator()
            blocked = {
                (a, b) for _, _, a, b, _ in
                v._similar_name_pairs_blocked_by_dob(norm, dob_series)
            }
            blocked_normalized = {(min(a, b), max(a, b)) for a, b in blocked}
            brute = self._brute_force_pairs_matching_dob(v, norm, dob_series)
            brute_normalized = {(min(a, b), max(a, b)) for a, b in brute}
            self.assertEqual(blocked_normalized, brute_normalized)


class TestSimilarNames(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_highly_similar_names(self):
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru', 'Alice Wanjiru']})
        # Identical names are excluded from similar check, no issue expected
        issues = self.v._check_similar_names(df)
        self.assertEqual(issues, [])

    def test_flags_near_identical_names(self):
        # One character omitted — different after strip, should exceed threshold
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru', 'Alice Wanjru']})
        issues = self.v._check_similar_names(df)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'similar_name')

    def test_no_issue_for_very_different_names(self):
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru', 'John Otieno']})
        self.assertEqual(self.v._check_similar_names(df), [])

    def test_skips_when_no_name_column(self):
        df = pd.DataFrame({'other': ['a', 'b']})
        self.assertEqual(self.v._check_similar_names(df), [])

    def test_skips_single_record(self):
        df = pd.DataFrame({'participants_name': ['Alice Wanjiru']})
        self.assertEqual(self.v._check_similar_names(df), [])


class TestStaleRecord(unittest.TestCase):
    def setUp(self):
        self.v = _validator()

    def test_flags_uniqueid_in_stale_set(self):
        df = pd.DataFrame({'uniqueid': ['a', 'b'], 'tabletnum': ['53', '53']})
        issues = self.v._check_stale_record(df, {'a'})
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['check'], 'stale_record_missing_from_tablet')
        self.assertEqual(issues[0]['severity'], 'WARNING')
        self.assertEqual(issues[0]['record_count'], 1)

    def test_no_issue_when_stale_set_empty(self):
        df = pd.DataFrame({'uniqueid': ['a', 'b']})
        self.assertEqual(self.v._check_stale_record(df, set()), [])

    def test_no_issue_when_no_match(self):
        df = pd.DataFrame({'uniqueid': ['a', 'b']})
        self.assertEqual(self.v._check_stale_record(df, {'z'}), [])

    def test_skips_when_column_absent(self):
        df = pd.DataFrame({'other': [1, 2]})
        self.assertEqual(self.v._check_stale_record(df, {'a'}), [])

    def test_validate_stale_records_returns_full_report_shape(self):
        df = pd.DataFrame({'uniqueid': ['a'], 'tabletnum': ['53']})
        report = self.v.validate_stale_records(
            df, {'a'}, country_name='uganda', site_name='Bushenyi'
        )
        self.assertEqual(len(report), 1)
        self.assertEqual(report.iloc[0]['check'], 'stale_record_missing_from_tablet')
        self.assertEqual(report.iloc[0]['country'], 'uganda')
        self.assertEqual(report.iloc[0]['site'], 'Bushenyi')

    def test_validate_stale_records_empty_set_returns_empty_report(self):
        df = pd.DataFrame({'uniqueid': ['a']})
        report = self.v.validate_stale_records(df, set())
        self.assertTrue(report.empty)


class TestValidateRegistry(unittest.TestCase):
    """
    Covers validate() itself — the registry-driven orchestration added when
    the 24 hand-written `issues += self._check_x(df)` call sites were
    replaced by DataValidator._CHECKS. Individual checks are covered above
    and in each check's own tests; these confirm the registry wires the
    skip_identity / country_code gating and per-issue defaults correctly.
    """

    def setUp(self):
        self.v = _validator()

    def _df(self, **overrides):
        base = {
            'subjid': ['S001', 'S002'],
            'mobile_number': ['0712345678', '0712345678'],  # duplicate -> identity check fires
            'participants_name': ['Alice Wanjiru', 'Bob Otieno'],
            'countrycode': [2, 2],
        }
        base.update(overrides)
        return pd.DataFrame(base)

    def test_runs_all_25_registered_checks(self):
        self.assertEqual(len(DataValidator._CHECKS), 25)

    def test_stale_uniqueids_none_skips_stale_record_check(self):
        df = self._df(uniqueid=['U1', 'U2'])
        report = self.v.validate(df, stale_uniqueids=None)
        self.assertTrue(report[report['check'] == 'stale_record_missing_from_tablet'].empty)

    def test_stale_uniqueids_given_runs_stale_record_check(self):
        df = self._df(uniqueid=['U1', 'U2'])
        report = self.v.validate(df, stale_uniqueids={'U1'})
        self.assertFalse(report[report['check'] == 'stale_record_missing_from_tablet'].empty)

    def test_skip_identity_true_excludes_identity_checks(self):
        report = self.v.validate(self._df(), skip_identity=True)
        self.assertTrue(report[report['check'] == 'duplicate_phone'].empty)

    def test_skip_identity_false_includes_identity_checks(self):
        report = self.v.validate(self._df(), skip_identity=False)
        self.assertFalse(report[report['check'] == 'duplicate_phone'].empty)

    def test_country_code_none_skips_countrycode_mismatch_check(self):
        # countrycode=1 but no country_code passed -> mismatch check must not run at all
        report = self.v.validate(self._df(countrycode=[1, 1]), country_code=None)
        self.assertTrue(report[report['check'] == 'countrycode_mismatch'].empty)

    def test_country_code_given_runs_countrycode_mismatch_check(self):
        report = self.v.validate(self._df(countrycode=[1, 1]), country_code=2, country_name='kenya')
        self.assertFalse(report[report['check'] == 'countrycode_mismatch'].empty)

    def test_issues_default_country_and_site(self):
        report = self.v.validate(self._df(), country_name='uganda', site_name='Mbarara')
        self.assertTrue((report['country'] == 'uganda').all())
        self.assertTrue((report['site'] == 'Mbarara').all())

    def test_identity_check_names_stays_in_sync_with_registry_output(self):
        """
        IDENTITY_CHECK_NAMES must stay in sync with what the identity=True
        checks in _CHECKS actually produce — this is the fact measures_ibis.py
        relies on to filter a combined report down to identity-only rows.
        skip_identity=True must never leak a name from that set into the
        report; a df crafted to trigger identity checks must produce names
        that are all members of it.
        """
        self.assertTrue(any(s.identity for s in DataValidator._CHECKS))

        skipped_report = self.v.validate(self._df(), skip_identity=True)
        leaked = set(skipped_report['check']) & DataValidator.IDENTITY_CHECK_NAMES
        self.assertEqual(leaked, set())

        # duplicate subjid + duplicate phone are both easy to trigger from
        # the base fixture (S001 appears twice, same mobile_number twice).
        triggering_df = self._df(subjid=['S001', 'S001'])
        full_report = self.v.validate(triggering_df, skip_identity=False)
        produced_identity_names = set(full_report['check']) & {
            'duplicate_subjid', 'duplicate_phone', 'similar_phone',
            'duplicate_name', 'similar_name',
        }
        self.assertTrue(produced_identity_names)
        self.assertTrue(produced_identity_names <= DataValidator.IDENTITY_CHECK_NAMES)


if __name__ == '__main__':
    unittest.main()
