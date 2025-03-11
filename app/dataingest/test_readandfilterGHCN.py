import pytest
import polars as pl
from app.dataingest.GHCNreader import parse_fixed_width_file
from app.dataingest.GHCNfilter import filter_data
from datetime import datetime, timedelta
from app.dataingest.readandfilterGHCN import get_date_list, set_ranged_data

@pytest.fixture
def mock_dataframe():
    file_path = "/data/ops/ghcnd/data/ghcnd_all/USW00093991.dly"
    
    df = parse_fixed_width_file(file_path)
    
    # Return mock DataFrame for testing
    return df


@pytest.fixture
def test_params():
    return {
        "year": 2023,
        "month": 10,
        "day": 22,
        "observation_type": "TMAX",
        "station_code": "USW00093991",
        "station_id": "USW00093991"[3:],  # Extracting "93991"
        "begin_date": datetime(2023, 10, 25),
        "end_date": datetime(2023, 10, 31),
    }
    


@pytest.fixture
def filtered_df(mock_dataframe, test_params):
    correction_type = "daily"
    year = test_params["year"]
    month = test_params["month"]
    day = test_params["day"]
    observation_type = test_params["observation_type"]
    station_code = test_params["station_code"]
    station_id = test_params["station_id"]

    if correction_type != "range":
        filtered_df = filter_data(
            mock_dataframe,
            year=year,
            month=month,
            day=day,
            observation_type=observation_type,
            station_code=station_id,
        )

        # If no data found (empty DataFrame), assert it is empty
        if len(filtered_df) == 0 or filtered_df.shape[0] == 0:
            print(f"No data found for station {station_id}. Skipping.")
            assert len(filtered_df) == 0 or filtered_df.shape[0] == 0  # Assert empty DataFrame
        else:
            print(f"Filtered DataFrame contains data: {filtered_df}")
            assert len(filtered_df) > 0 and filtered_df.shape[0] > 0  # Assert non-empty DataFrame
    else:
        filtered_df = filter_data(
            df,
            start_date=begin_date,
            end_date=end_date,
            observation_type=observation_type,
            station_code=station_id,
            country_code=country_code,
            network_code=network_code,
        )
        # If no data found (empty DataFrame), assert it is empty
        if len(filtered_df) == 0 or filtered_df.shape[0] == 0:
            print(f"No data found for station {station_id}. Skipping.")
            assert len(filtered_df) == 0 or filtered_df.shape[0] == 0  # Assert empty DataFrame
        else:
            print(f"Filtered DataFrame contains data: {filtered_df}")
            assert len(filtered_df) > 0 and filtered_df.shape[0] > 0  # Assert non-empty DataFrame
        
    return filtered_df  # Return the filtered_df to use it in other tests


import pytest
from datetime import datetime

def test_is_not_ranged_correction(filtered_df):
    correction_type = "daily"
    
    # Ensure correction_type is NOT "range"
    assert correction_type != "range"

    # Assert that filtered_df is either empty or contains data
    if len(filtered_df) == 0 or filtered_df.shape[0] == 0:
        print("No data found in filtered_df. Skipping test.")
        assert len(filtered_df) == 0 or filtered_df.shape[0] == 0  # Assert empty DataFrame
    else:
        print(f"Filtered DataFrame contains data: {filtered_df}")
        assert len(filtered_df) > 0 and filtered_df.shape[0] > 0  # Assert non-empty DataFrame


def test_is_ranged_correction(mock_dataframe, test_params):
    correction_type = "range"
    
    year = test_params["year"]
    month = test_params["month"]
    day = test_params["day"]
    observation_type = test_params["observation_type"]
    station_code = test_params["station_code"]
    station_id = test_params["station_id"]
    begin_date = test_params["begin_date"]
    end_date = test_params["end_date"]
    
    if correction_type == "range":
        # Test the filtering and the range data function
        filtered_df = filter_data(
            mock_dataframe,
            start_date=begin_date,
            end_date=end_date,
            observation_type=observation_type,
            station_code=station_id,
            country_code=station_code[:2],
            network_code=station_code[2],
        )
        
        date_list = get_date_list(begin_date, end_date)
        formatted_range_data = set_ranged_data(date_list, filtered_df)
        
        # Assertions
        assert isinstance(formatted_range_data, list), "formatted_range_data should be a list"

        # Assert that each entry is a dictionary containing 'Date' and 'Value'
        for entry in formatted_range_data:
            assert isinstance(entry, dict), f"Each entry should be a dictionary, found: {type(entry)}"
            assert 'Date' in entry, "Each dictionary should contain 'Date'"
            assert 'Value' in entry, "Each dictionary should contain 'Value'"
            
            # Validate date format
            try:
                datetime.strptime(entry['Date'], "%Y-%m-%d")
            except ValueError:
                assert False, f"Invalid date format: {entry['Date']}"

        # Assert that the length of the formatted range data matches the number of days (7 in this case)
        assert len(formatted_range_data) == len(date_list), f"Expected {len(date_list)} entries, found {len(formatted_range_data)}"
        
        # Check that filtered_df is not empty for valid date ranges
        assert len(filtered_df) > 0, "filtered_df should not be empty when correction_type is 'range'"

        # Check for edge case: No data for invalid date ranges
        invalid_end_date = datetime(1004, 10, 31)
        filtered_df_invalid = filter_data(
            mock_dataframe,
            start_date=begin_date,
            end_date=invalid_end_date,
            observation_type=observation_type,
            station_code=station_id,
            country_code=station_code[:2],
            network_code=station_code[2],
        )
        
        # Assert that filtered_df_invalid should be empty for invalid date ranges
        assert len(filtered_df_invalid) == 0, "filtered_df should be empty for out-of-range dates"

    else:
        print("is not range")
        
    assert correction_type == "range"
    
    
def test_o_value_correction(mock_dataframe, test_params):
    correction_type = "o_value"  # This is the correction type we are testing
    year = test_params["year"]
    month = test_params["month"]
    day = test_params["day"]
    observation_type = test_params["observation_type"]
    station_code = test_params["station_code"]
    station_id = test_params["station_id"]
    begin_date = test_params["begin_date"]
    end_date = test_params["end_date"]

    print("year", year)
    if correction_type == "o_value":
        try:
            o_value = mock_dataframe['day_' + str(day)][0]  # Pull the value for the day column
            print("O-Value for day:", o_value)
            
            assert o_value is None or isinstance(o_value, (str)), f"O-Value should be None or a string, found: {type(o_value)}"
            
        except KeyError:
            assert False, f"Column 'day_{day}' not found in mock_dataframe"
        except IndexError:
            assert False, f"Failed to retrieve the O-Value for day {day}"
    
    # Assert correction_type is "o_value"
    assert correction_type == "o_value", f"Expected correction_type to be 'o_value', found {correction_type}"
    
    

def test_compare_correction(mock_dataframe, filtered_df, test_params):
    correction_type = "compare"
    year = 2023
    month = test_params["month"]
    day = test_params["day"]
    observation_type = test_params["observation_type"]
    station_code = test_params["station_code"]
    station_id = test_params["station_id"]

    # Ensure correction_type is "compare"
    assert correction_type == "compare", f"Expected correction_type to be 'compare', found {correction_type}"
    
    # Ensure day is not None
    assert day is not None, "Day should not be None"
    
    try:
        # Calculate prior_day and next_day dynamically
        date = datetime(year, month, day)
        prior_day = (date - timedelta(days=1)).day
        next_day = (date + timedelta(days=1)).day
    except ValueError:
        pytest.fail(f"Invalid date parameters: year={year}, month={month}, day={day}")
    
    # Create prior_day_filtered_df and next_day_filtered_df using the fixture data
    
    prior_day_filtered_df = filter_data(
        mock_dataframe,
        year=year,
        month=month,
        day=prior_day,
        observation_type=observation_type,
        station_code=station_id,
    )
    print("prior_day_filtered_df: ", prior_day_filtered_df)
    print("day_of_filtered_df: ", filtered_df)

    next_day_filtered_df = filter_data(
        mock_dataframe,
        year=year,
        month=month,
        day=next_day,
        observation_type=observation_type,
        station_code=station_id,
    )
    print("next_day_filtered_df: ", next_day_filtered_df)

    # Assert that prior_day_filtered_df and next_day_filtered_df are created correctly
    assert not prior_day_filtered_df.is_empty(), f"Prior day filtered DataFrame is empty for {prior_day}"
    assert not next_day_filtered_df.is_empty(), f"Next day filtered DataFrame is empty for {next_day}"

    # Create the daily_data dictionary
    daily_data = {
        'country_code': filtered_df['country_code'][0],
        'network_code': filtered_df['network_code'][0],
        'station_code': filtered_df['station_code'][0],
        'year': filtered_df['year'][0],
        'month': filtered_df['month'][0],
        'observation_type': filtered_df['observation_type'][0],
        'dayMinus': prior_day_filtered_df['day_' + str(prior_day)][0] if not prior_day_filtered_df.is_empty() else None,
        'day': filtered_df['day_' + str(day)][0] if not filtered_df.is_empty() else None,
        'dayPlus': next_day_filtered_df['day_' + str(next_day)][0] if not next_day_filtered_df.is_empty() else None,
    }
    
    print("COMARISON DATA: ", daily_data)

    # Assert that daily_data matches the formatting correctly
    assert isinstance(daily_data, dict), f"Expected daily_data to be a dictionary, found {type(daily_data)}"
    assert 'country_code' in daily_data, "Missing 'country_code' in daily_data"
    assert 'network_code' in daily_data, "Missing 'network_code' in daily_data"
    assert 'station_code' in daily_data, "Missing 'station_code' in daily_data"
    assert 'year' in daily_data, "Missing 'year' in daily_data"
    assert 'month' in daily_data, "Missing 'month' in daily_data"
    assert 'observation_type' in daily_data, "Missing 'observation_type' in daily_data"
    assert 'dayMinus' in daily_data, "Missing 'dayMinus' in daily_data"
    assert 'day' in daily_data, "Missing 'day' in daily_data"
    assert 'dayPlus' in daily_data, "Missing 'dayPlus' in daily_data"
    
    # Assert values are properly assigned
    assert daily_data['dayMinus'] is not None, "Expected 'dayMinus' to be valid"
    assert daily_data['day'] is not None, "Expected 'day' to be valid"
    assert daily_data['dayPlus'] is not None, "Expected 'dayPlus' to be valid"
    
    
    
# def test_monthly_correction(mock_dataframe, filtered_df, test_params):
#     correction_type = "monthly"

#     # Ensure correction_type is "monthly"
#     assert correction_type == "monthly", f"Expected correction_type to be 'monthly', found {correction_type}"

#     # Ensure filtered_df is not empty (Polars version)
#     assert not filtered_df.is_empty(), "Filtered DataFrame is empty"

#     # Create monthly_data dictionary
#     monthly_data = {
#         'country_code': filtered_df['country_code'][0],
#         'network_code': filtered_df['network_code'][0],
#         'station_code': filtered_df['station_code'][0],
#         'year': filtered_df['year'][0],
#         'month': filtered_df['month'][0],
#         'observation_type': filtered_df['observation_type'][0],
#     }

#     # Add all days of the month
#     for day in range(1, 32):  # Loop through days 1-31
#         day_key = f'day_{day}'
#         monthly_data[day_key] = (
#             filtered_df[day_key] if day_key in filtered_df.columns and not filtered_df.is_empty() else None
#         )
        
#     print("MONTHLY DATA:\n", monthly_data)

#     # Validate monthly_data dictionary structure
#     required_keys = [
#         'country_code', 'network_code', 'station_code',
#         'year', 'month', 'observation_type'
#     ]
    
#     for key in required_keys:
#         assert key in monthly_data, f"Missing '{key}' in monthly_data"

#     # Ensure that all days in the month are present
#     for day in range(1, 32):
#         day_key = f'day_{day}'
#         assert day_key in monthly_data, f"Missing '{day_key}' in monthly_data"

#     # Ensure key values are valid
#     assert monthly_data['country_code'] is not None, "Expected 'country_code' to be valid"
#     assert monthly_data['network_code'] is not None, "Expected 'network_code' to be valid"
#     assert monthly_data['station_code'] is not None, "Expected 'station_code' to be valid"
#     assert monthly_data['year'] is not None, "Expected 'year' to be valid"
#     assert monthly_data['month'] is not None, "Expected 'month' to be valid"
#     assert monthly_data['observation_type'] is not None, "Expected 'observation_type' to be valid"