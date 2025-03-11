import unittest
import polars as pl
import tempfile
import os
from GHCNreader import parse_fixed_width_file

class TestParseFixedWidthFile(unittest.TestCase):
    def setUp(self):
        # Create a temporary file with sample fixed-width data
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        self.temp_file.write("USW00093991194904TMAX  106  0  111  0  111  0  156  0  183  0  250  0  244  0  211  0  178  0  222  0  206  0  206  0  267  0  244  0  150  0  189  0  283  0  189  0  178  0  167  0  178  0  272  0  294  0  261  0  283  0  228  0  256  0  256  0  228  0  200  0-9999   \n")
        self.temp_file.write("USW00093991194904TMIN   61  0   56  0   56  0   22  0   78  0   44  0  100  0   94  0   72  0  100  0   89  0   89  0  100  0   67  0   33  0   11  0   89  0   72  0   17  0   56  0  117  0  133  0  122  0  128  0   83  0  172  0  172  0  150  0  139  0  128  0-9999   \n")
        self.temp_file.close()
        
    def tearDown(self):
        # Remove the temporary file after test
        os.remove(self.temp_file.name)
    
    def test_parse_fixed_width_file(self):
        # Run the function
        df = parse_fixed_width_file(self.temp_file.name)
        
        # Verify the DataFrame structure
        self.assertIsInstance(df, pl.DataFrame)
        self.assertEqual(df.shape[0], 2)  # Expecting two rows
        self.assertEqual(df.shape[1], 68)  # 6 metadata fields + 31 day data + 31 flags
        
        # Check specific values
        self.assertEqual(df["country_code"].to_list()[0], "US")
        self.assertEqual(df["network_code"].to_list()[0], "W")
        self.assertEqual(df["year"].to_list()[0], 1949)
        self.assertEqual(df["month"].to_list()[0], 4)
        self.assertEqual(df["observation_type"].to_list()[0], "TMAX")
        self.assertEqual(df["observation_type"].to_list()[1], "TMIN")
        self.assertEqual(df["day_1"].to_list()[0], "106")
        self.assertEqual(df["flag_1"].to_list()[0], "0")
        
if __name__ == "__main__":
    unittest.main()
