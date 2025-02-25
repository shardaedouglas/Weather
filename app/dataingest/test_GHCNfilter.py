import unittest
import polars as pl
from datetime import datetime
from GHCNfilter import filter_data


class TestFilterData(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame similar to what parse_fixed_width_file would produce
        self.df = pl.DataFrame([
            {"country_code": "US", "network_code": "1", "station_code": "0001",
             "year": 2023, "month": 1, "observation_type": "PRCP", "day_1": "10", "flag_1": "A"},
            {"country_code": "CA", "network_code": "1", "station_code": "0002",
             "year": 2022, "month": 5, "observation_type": "TMAX", "day_1": "15", "flag_1": "B"},
            {"country_code": "US", "network_code": "2", "station_code": "0003",
             "year": 2021, "month": 12, "observation_type": "PRCP", "day_1": "5", "flag_1": "C"},
        ])

    def test_filter_by_year(self):
        result = filter_data(self.df, year=2023)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["year"].to_list(), [2023])

    def test_filter_by_month(self):
        result = filter_data(self.df, month=5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result["month"].to_list(), [5])

    def test_filter_by_observation_type(self):
        result = filter_data(self.df, observation_type="PRCP")
        self.assertEqual(len(result), 2)
        self.assertTrue(result["observation_type"].to_list() == ["PRCP", "PRCP"])

    def test_filter_by_date_range(self):
        start_date = datetime(2021, 1, 1)
        end_date = datetime(2023, 12, 31)
        result = filter_data(self.df, start_date=start_date, end_date=end_date)
        self.assertEqual(len(result), 3)

    def test_filter_by_day(self):
        result = filter_data(self.df, day=1)
        self.assertIn("day_1", result.columns)
        self.assertIn("flag_1", result.columns)

    def test_filter_by_multiple_conditions(self):
        result = filter_data(self.df, year=2023, month=1, observation_type="PRCP")
        self.assertEqual(len(result), 1)
        self.assertEqual(result["year"].to_list(), [2023])
        self.assertEqual(result["month"].to_list(), [1])
        self.assertEqual(result["observation_type"].to_list(), ["PRCP"])


if __name__ == "__main__":
    unittest.main()
