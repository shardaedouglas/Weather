import unittest
import polars as pl
from datetime import datetime
from GHCNhfilter import filter_data

class TestFilterData(unittest.TestCase):
    def setUp(self):
        """Create a sample Polars DataFrame for testing with the correct columns."""
        self.df = pl.DataFrame({
            "Station_ID": ["USW00013993", "USW00013993", "USW00013993"],
            "Station_name": ["ST JOSEPH ROSECRANS MEM AP"] * 3,
            "Latitude": [39.7683] * 3,
            "Longitude": [-94.9094] * 3,
            "Elevation": [246.0] * 3,
            "temperature": [0.0, 1.5, 3.0],
            "temperature_Quality_Code": [5, 5, 5],
            "wind_speed": [8.2, 7.5, 6.0],
            "wind_speed_Quality_Code": [5, 5, 5],
            "relative_humidity": [96, 90, 85],
            "relative_humidity_Quality_Code": [5, 5, 5],
            "datetime": [
                datetime(2019, 1, 1, 0, 6),
                datetime(2019, 1, 1, 1, 6),
                datetime(2019, 1, 1, 2, 6),
            ],
        })

    def test_filter_by_datetime_range(self):
        """Test filtering by datetime range."""
        start_date = "2019-01-01T00:00"
        end_date = "2019-01-01T01:30"
        filtered_df = filter_data(self.df, start_datetime=start_date, end_datetime=end_date)

        self.assertEqual(filtered_df.shape[0], 2)  # Two rows should match
        self.assertTrue(all(filtered_df["datetime"] >= datetime(2019, 1, 1, 0, 0)))
        self.assertTrue(all(filtered_df["datetime"] <= datetime(2019, 1, 1, 1, 30)))

    def test_column_selection(self):
        """Test selecting specific columns."""
        columns_to_keep = ["datetime", "temperature", "Station_ID"]
        filtered_df = filter_data(self.df, columns_to_keep=columns_to_keep)

        self.assertEqual(set(filtered_df.columns), set(columns_to_keep))

    def test_show_first_x_columns(self):
        """Test limiting the number of displayed columns."""
        filtered_df = filter_data(self.df, show_first_x_columns=3)

        self.assertEqual(len(filtered_df.columns), 3)

    def test_show_first_y_rows(self):
        """Test limiting the number of displayed rows."""
        filtered_df = filter_data(self.df, show_first_y_rows=2)

        self.assertEqual(filtered_df.shape[0], 2)

if __name__ == "__main__":
    unittest.main()
