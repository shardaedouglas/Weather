from app.dataingest.GHCNfilter import filter_data
from app.dataingest.GHCNreader import parse_fixed_width_file
import polars as pl
import json
import calendar

from datetime import datetime, timedelta

def parse_and_filter(
    file_path: str,
    correction_type=None,
    year=None,
    month=None,
    day=None,
    observation_type=None,
    country_code=None,
    network_code=None,
    station_code=None,
    begin_date=None,
    end_date=None
):
    """
    Reads and filters the data from a fixed-width file.

    Parameters:
    file_path (str): Path to the fixed-width data file.
    year (int, optional): Year to filter on.
    month (int, optional): Month to filter on.
    day (int, optional): Day to filter on.
    observation_type (str, optional): Observation type to filter on.
    country_code (str, optional): Country code to filter on.
    network_code (str, optional): Network code to filter on.
    station_code (str, optional): Station code to filter on.

    Returns:
    dict: The data for prior, current, and next day in the required format.
    """
    
    country_code = station_code[:2]
    network_code = station_code[2]
    station_id = station_code[3:]

    # print(f"Parser_Incoming parameters:")
    # print(f"Parser_file_path: {file_path}")
    # print(f"Parser_correction_type: {correction_type}")
    # print(f"Parser_year: {year}")
    # print(f"Parser_month: {month}")
    # print(f"Parser_day: {day}")
    # print(f"Parser_observation_type: {observation_type}")
    # print(f"Parser_country_code: {country_code}")
    # print(f"Parser_network_code: {network_code}")
    # print(f"Parser_station_code: {station_code}")
    # print(f"Parser_begin_date: {begin_date}")
    # print(f"Parser_end_date: {end_date}")

    # Step 1: Parse the fixed-width file into a DataFrame
    df = parse_fixed_width_file(file_path)
    
    # print("file Path: ", file_path)
    print("df: ", df)
    json_str = json.dumps(df.to_dicts(), indent=2)
    # Step 2: Apply filtering using filter_data

    filtered_df = None  # Initialize variable outside

    if correction_type != "range":
        filtered_df = filter_data(
            df,
            year=year,
            month=month,
            day=day,
            observation_type=observation_type,
            station_code=station_id,
        )
        # print(filtered_df)    
        # If no data is found (check for empty DataFrame using length or shape), return a special message indicating to skip
        if len(filtered_df) == 0 or filtered_df.shape[0] == 0:
            # print(f"No data found for station {station_code} with the given filters. Skipping station.")
            return {'status': 'skip', 'station_code': station_code}
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
        
        
        # print("filtered_df_RANGE: ", filtered_df)

    # If correction_type is "compare", include prior and next day values
    if correction_type == "compare" and day is not None:
        prior_day = (datetime(year, month, day) - timedelta(days=1)).day
        next_day = (datetime(year, month, day) + timedelta(days=1)).day

        prior_day_filtered_df = filter_data(
            df,
            year=year,
            month=month,
            day=prior_day,
            observation_type=observation_type,
            station_code=station_id,
        )
        
        next_day_filtered_df = filter_data(
            df,
            year=year,
            month=month,
            day=next_day,
            observation_type=observation_type,
            station_code=station_id,
        )
        

        # Extract the relevant values from the DataFrame and prepare the result in the right format
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

        # Return the result
        return daily_data
    
    elif correction_type == "daily":
        
        print ("DoDailyThingsHere")     
        
    elif correction_type == "range":
        
        date_list = get_date_list(begin_date, end_date)
        formatted_range_data = set_ranged_data(date_list, filtered_df)  # Pass the date_list variable
        return formatted_range_data
        
    elif correction_type == "o_value":
        # Filter the data for the specific day
        o_value = filtered_df['day_' + str(day)][0] # Pull the value for the day column
        print("O-Value for day:", o_value)

        return o_value
    
    
    else:   #correction_type = monthly
            monthly_data = {
                'country_code': filtered_df['country_code'][0],
                'network_code': filtered_df['network_code'][0],
                'station_code': filtered_df['station_code'][0],
                'year': filtered_df['year'],
                'month': filtered_df['month'],
                'observation_type': filtered_df['observation_type'],
            }

            # Add data for all days in the month
            for day in range(1, 32):  # Loop through days 1-31
                monthly_data[f'day_{day}'] = filtered_df['day_' + str(day)] if not filtered_df.is_empty() else None

            return monthly_data

    
def get_date_list(begin_date, end_date):
    # Ensure that begin_date and end_date are datetime objects (if they are already, this step is unnecessary)
    if isinstance(begin_date, datetime):
        begin_date = begin_date.date()  # Convert to date if it's a datetime object
    if isinstance(end_date, datetime):
        end_date = end_date.date()  # Convert to date if it's a datetime object

    # Initialize the list to store dates in the JSON format
    date_list = []

    # Generate the list of dates within the range
    current_date = begin_date
    while current_date <= end_date:
        date_list.append({"Date": current_date.strftime("%Y-%m-%d")})
        current_date += timedelta(days=1)

    # Print the final list as a formatted JSON
    
    return date_list
      
      
def set_ranged_data(date_list, filtered_df):
    for date_entry in date_list:
        # Split the date string into year, month, and day
        date_str = date_entry.get("Date", "")
        if date_str:
            year, month, day = date_str.split("-")

            # Apply filter
            filtered_row = filtered_df.filter(
                (pl.col("year") == int(year)) & (pl.col("month") == int(month))
            )

            # Check if filtered_row is empty
            if filtered_row.is_empty():
                print(f"No data found for {year}-{month}-{day}")
            else:
                day = str(int(day))
                day_column_name = f"day_{day}"
                # Check if the column exists in the dataframe
                if day_column_name in filtered_row.columns:
                    # Extract the value for that specific day
                    value = filtered_row.select(day_column_name)

                    if not value.is_empty():
                        # Get the first value from the filtered data (assuming it's unique per day)
                        value = value[0, 0]  # Assuming the value is the first item in the first row
                        
                        # Append the date and value to the date_list
                        date_entry['Value'] = value
                        # print(f"Appended Value: {value} for {date_str}")
                    else:
                        print(f"No data for {year}-{month}-{day} in column {day_column_name}")
                else:
                    print(f"Column {day_column_name} does not exist in the dataframe")
        else:
            print("Invalid date format or missing 'Date' field")
    return date_list


# if __name__ == "__main__":

#     file_path =  "../../USW00093991.dly"

    # Example 1: Filter by year and country code
    # filtered_df = parse_and_filter(
    #     file_path=file_path,
    #     year=2023,
    #     country_code="US",
    #     day = 10
    # )
    # print(filtered_df)

#     print(filtered_df)

#     # Example 2: Filter by month, observation type, and network code
#     filtered_df = parse_and_filter(
#         file_path=file_path,
#         month=1,
#         observation_type="PRCP",
#         network_code="1"
#     )

#     print(filtered_df)

#     # Example 3: Filter by station code and specific day
#     filtered_df = parse_and_filter(
#         file_path=file_path,
#         station_code="MOMA0004",
#         day=15
#     )

#     print(filtered_df)

