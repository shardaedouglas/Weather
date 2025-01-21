from app.dataingest.GHCNfilter import filter_data
from app.dataingest.GHCNreader import parse_fixed_width_file

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

    # Step 1: Parse the fixed-width file into a DataFrame
    df = parse_fixed_width_file(file_path)
    
    # Step 2: Apply filtering using filter_data
    filtered_df = filter_data(
        df,
        year=year,
        month=month,
        day=day,
        observation_type=observation_type,
        station_code=station_id,
    )
    
    # If no data is found (check for empty DataFrame using length or shape), return a special message indicating to skip
    if len(filtered_df) == 0 or filtered_df.shape[0] == 0:
        # print(f"No data found for station {station_code} with the given filters. Skipping station.")
        return {'status': 'skip', 'station_code': station_code}


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
        
    else:
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


    # # If no correction_type or day is not provided, just return the current day's data
    # return {
    #     'country_code': filtered_df['country_code'][0],
    #     'network_code': filtered_df['network_code'][0],
    #     'station_code': filtered_df['station_code'][0],
    #     'year': filtered_df['year'],
    #     'month': filtered_df['month'],
    #     'observation_type': filtered_df['observation_type'],
    #     # 'day': filtered_df['day_' + str(day)] if not filtered_df.is_empty() else None,
    # }



# if __name__ == "__main__":

#     file_path =  "../../USW00093991.dly"

#     # Example 1: Filter by year and country code
#     filtered_df = parse_and_filter(
#         file_path=file_path,
#         year=2023,
#         country_code="US"
#     )

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

