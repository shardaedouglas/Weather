import unittest
import polars as pl
from io import StringIO
from GHCNhreader import parse

class TestParseFunction(unittest.TestCase):
    def setUp(self):
        self.sample_data = """Station_ID|Station_name|Latitude|Longitude|Elevation|Year|Month|Day|Hour|Minute|temperature|wind_speed|dew_point_temperature|wind_direction|precipitation
        STN001|Station A|35.6895|139.6917|100|2023|1|1|0|0|5.0|3.0|1.0|90|0.1
        STN001|Station A|35.6895|139.6917|100|2023|1|1|1|0|6.0|4.0|2.0|100|0.2"""
        
        self.expected_columns = [
            "Station_ID", "Station_name", "Latitude", "Longitude", "Elevation",
            "temperature", "wind_speed", "dew_point_temperature",
            "wind_direction", "precipitation", "datetime"
        ]

    def test_parse_function(self):
        # Use StringIO to mock a file
        file_like_object = StringIO(self.sample_data)
        df = parse(file_like_object, delimiter='|')
        
        # Check if the output is a Polars DataFrame
        self.assertIsInstance(df, pl.DataFrame)
        
        # Check if expected columns are present
        self.assertListEqual(df.columns, self.expected_columns)
        
        # Check values in the datetime column
        expected_datetimes = ['2023-01-01 00:00:00.000000', '2023-01-01 01:00:00.000000']
        self.assertListEqual(df["datetime"].cast(str).to_list(), expected_datetimes)
        
        # Check a sample value
        self.assertEqual(df["temperature"].to_list()[0], '5.0')

if __name__ == '__main__':
    unittest.main()
