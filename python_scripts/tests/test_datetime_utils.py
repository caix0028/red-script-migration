import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unittest
from datetime import date

from shared.datetime_utils import get_querytimes


class TestDatetimeUtils(unittest.TestCase):
    def test_get_querytimes_same_month(self):
        start_date = date(2024, 1, 16)
        end_date = date(2024, 1, 16)
        result = get_querytimes(start_date, end_date)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 1705363200000)

    def test_get_querytimes_different_months(self):
        start_date = date(2023, 10, 8)
        end_date = date(2023, 11, 27)
        result = get_querytimes(start_date, end_date)
        self.assertEqual(len(result), 2)
        expected_result = [1696118400000, 1698796800000]
        for actual, expected in zip(result, expected_result):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
