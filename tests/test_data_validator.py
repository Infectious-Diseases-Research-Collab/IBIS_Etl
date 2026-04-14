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


if __name__ == '__main__':
    unittest.main()
