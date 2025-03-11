import pytest
from app.dataingest.readandfilterGHCN import parse_and_filter
import polars as pl

@pytest.fixture
def test_params(): 
    return {
        "station_code": "USW00093991",
        "file_path": "/data/ops/ghcnd/data/ghcnd_all/USW00093991.dly",
        "year": 2023,
        "month": 6,
        "correction_type": 1,
    }

@pytest.fixture
def filtered_df(test_params):
    return parse_and_filter(
        station_code=test_params["station_code"],
        file_path=test_params["file_path"],
        year=test_params["year"],
        month=test_params["month"],
        correction_type=test_params["correction_type"]
    )

def test_dataframe_structure(filtered_df):
    # Test if filtered_df is a dictionary, not a Polars DataFrame
    assert isinstance(filtered_df, dict), "Expected filtered_df to be a dictionary"

def test_station_information(filtered_df, test_params):
    # Check if the concatenated code is correct: country_code + network_code + station_code
    concatenated_station_code = f"{filtered_df['country_code']}{filtered_df['network_code'][0]}{filtered_df['station_code']}"
    expected_station_code = test_params["station_code"]  # Expected format: USW0093991
    print("Concatenated Station Code:", concatenated_station_code)
    print("expected_station_code:", expected_station_code)
    
    # Assert that the concatenated station code matches the expected value
    assert concatenated_station_code == expected_station_code, f"Expected station code {expected_station_code}, but got {concatenated_station_code}"

def test_observation_types(filtered_df):
    # Check if observation types are valid (e.g., TMAX, TMIN, PRCP, etc.)
    valid_observation_types = {'TMAX', 'TMIN', 'PRCP', 'SNOW', 'AWND', 'WSF5', 'WT01', 'WT02', 'WT03', 'WT08', 'WDF2', 'WSF2', 'WDF5'}
    observation_types = set(filtered_df['observation_type'])
    assert observation_types.issubset(valid_observation_types), f"Unexpected observation types found: {observation_types}"
    
        
def test_missing_values(filtered_df):
    # Check if any of the day columns contain missing values or invalid values.
    for day_col in [f'day_{i}' for i in range(1, 32)]:
        invalid_values = filtered_df.get(day_col, [])
        
        # Ensure that no value is missing or invalid (i.e., all values should be present)
        assert all(val is not None and val != "" for val in invalid_values), f"Column {day_col} contains missing or empty values"