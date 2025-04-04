# def get_state_for_GHCN_table_df():
#     try:
#         # Extract form data from the POST request
#         state = request.form.get('state')  # Expecting 'FL', 'TX', etc.
#         station_type = request.form.get('station_type')  # Observation type (e.g., TMAX, TMIN, etc.)
#         correction_date = request.form.get('date')

#         # Parse the date into components
#         if correction_date:
#             correction_year, correction_month, correction_day = map(int, correction_date.split('-'))
#         else:
#             correction_year, correction_month, correction_day = None, None, None
        
#         file_path = '/data/ops/ghcnd/data/ghcnd-stations.txt'
#         matching_stations = []

#         # Read and filter stations by state
#         with open(file_path, 'r') as f:
#             for line in f:
#                 parts = line.strip().split()  # Splitting by whitespace
#                 if len(parts) < 5:
#                     continue  # Skip malformed lines
                
#                 ghcn_id = parts[0]  # First column is the station ID
#                 state_code = parts[4]  # Fifth column is the state code
                
#                 if state_code == state:
#                     matching_stations.append(line)

#         print(f"Found {len(matching_stations)} stations for state {state}")
        
#         all_filtered_dfs = []  # List to accumulate filtered DataFrames
#         noDataCount = 0

# # Loop through the matching stations and parse their data
#         for station in matching_stations:  # You can adjust how many you process here
#             parts = station.strip().split()
#             ghcn_id = parts[0]  # Station ID
            
#             # Build the file path for each station's data
#             station_file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
#             print(f"Processing file for {ghcn_id}: {station_file_path}")
            
#             #Run parser and get data for table, (month, year, all types)
#             filtered_data = parse_and_filter(
#                 station_code=ghcn_id,
#                 file_path=station_file_path,
#                 correction_type="graph",
#                 month=correction_month,
#                 year=correction_year
#             )
            
#             # Ensure the filtered_data is converted to a Polars DataFrame (if it's not already one)
#             if isinstance(filtered_data, dict):
#                 # If it's a dictionary, we should convert it to a Polars DataFrame
#                 filtered_df = pl.DataFrame(filtered_data)
#             else:
#                 # Otherwise, assume it's already a Polars DataFrame
#                 filtered_df = filtered_data

#             # Check if the filtered DataFrame is empty
#             if filtered_df.is_empty():
#                 print(f"Skipping station {ghcn_id} due to no data.")
#                 noDataCount += 1
#                 continue  # Skip this station and move to the next
            
#             # Align columns by adding missing columns to the DataFrame
#             if all_filtered_dfs:
#                 # Get the columns of the first DataFrame
#                 existing_columns = all_filtered_dfs[0].columns
#                 current_columns = filtered_df.columns

#                 # Add missing columns to current DataFrame, with None values
#                 missing_columns = set(existing_columns) - set(current_columns)
#                 for col in missing_columns:
#                     filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

#                 # Ensure columns are in the same order
#                 filtered_df = filtered_df.select(existing_columns)
            
#             # Append to list of DataFrames
#             all_filtered_dfs.append(filtered_df)
        
        
#         save_path_limited = f"/data/ops/ghcnd/TestData_pub/limited_data/{state}/{correction_month}/{state}_combined_data.json"
#         save_path_limited_parquet = f"/data/ops/ghcnd/TestData_pub/limited_data/{state}/{correction_month}/{state}_combined_data.parquet"

#         # Ensure directories exist
#         os.makedirs(os.path.dirname(save_path_limited), exist_ok=True)

#         if all_filtered_dfs:
#             combined_df = pl.concat(all_filtered_dfs, how="vertical")
#             print("Combined DataFrame: ", combined_df)

#             # Save JSON
#             combined_df.write_json(save_path_limited)

#             # Save Parquet
#             combined_df.write_parquet(save_path_limited_parquet)

#             return jsonify(combined_df.to_dicts())  # Convert to a list of dictionaries for JSON serialization
#         else:
#             return jsonify({"error": "No data available for the requested stations."}), 404

#     except Exception as e:
#         print(f"Error in get_state_for_GHCN_table_df: {e}")
#         return jsonify({"error": "Internal server error"}), 500
    
    
# @ghcndata_bp.route('/test_monthlyPub')
# def test_monthlyPub():
#     try:
#         generateMonthlyPub()
#         return jsonify({"message": "generateMonthlyPub() executed successfully!"})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 500  
    
import polars as pl
import json

def makeGraph(df):
    """
    Placeholder function to process the DataFrame for graphing.
    """
    print("DF!!!!!!!!!!!!: ", df)
    print("Generating graph...")

def processDataForTable(date_param=None):
    """
    Placeholder function for chart processing.
    """
    print("Processing chart data!")
    
def highestRecordedTemp(df: pl.DataFrame) -> dict:
    # Filter only TMAX observation type
    print(df.select("observation_type").unique())
    
    tmax_df = df.filter(df["observation_type"] == "TMAX")
    
    # If no TMAX data is found, return an empty dict
    if tmax_df.is_empty():
        print("EMPTY DATA")
        return {}

    # Convert day columns to numeric and ignore missing values (-9999)
    day_columns = [col for col in df.columns if col.startswith("day_")]
    tmax_df = tmax_df.with_columns([ 
        pl.col(day_columns).cast(pl.Int64, strict=False).fill_null(-9999)
    ])

    # Replace missing values (-9999) with nulls so they don't interfere with max calculations
    tmax_df = tmax_df.with_columns([ 
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col) 
        for col in day_columns
    ])

    # Find the highest TMAX for each station and the corresponding date
    result = {}
    for row in tmax_df.iter_rows(named=True):
        station = row["station_code"]
        country_code = row["country_code"]
        network_code = row["network_code"]
        
        # Combine the codes into one identifier (e.g., US1CAAL0001)
        combined_station_code = f"{country_code}{network_code}{station}"

        max_temp = -float('inf')  # Initialize to a very low value
        max_day = None
        for day_num, col in enumerate(day_columns, start=1):
            if row[col] is not None and row[col] > max_temp:
                max_temp = max(row[col] for col in day_columns if row[col] is not None) / 10
                max_day = day_num

        if max_day is not None:  # Only store if a valid day was found
            year = row["year"]
            month = row["month"]
            date = f"{year}-{month:02d}-{max_day:02d}"  # Format as YYYY-MM-DD

            # Store the result using the combined station code
            if combined_station_code not in result:
                result[combined_station_code] = {"TMAX": max_temp, "date": date}

    return result


def lowestRecordedTemp(df: pl.DataFrame) -> dict:
    # Filter only TMIN observation type
    print(df.select("observation_type").unique())
    
    tmin_df = df.filter(df["observation_type"] == "TMIN")
    
    # If no TMIN data is found, return an empty dict
    if tmin_df.is_empty():
        print("EMPTY DATA")
        return {}

    # Convert day columns to numeric and ignore missing values (-9999)
    day_columns = [col for col in df.columns if col.startswith("day_")]
    tmin_df = tmin_df.with_columns([ 
        pl.col(day_columns).cast(pl.Int64, strict=False).fill_null(-9999)
    ])

    # Replace missing values (-9999) with nulls so they don't interfere with min calculations
    tmin_df = tmin_df.with_columns([ 
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col) 
        for col in day_columns
    ])

    # Find the lowest TMIN for each station and the corresponding date
    result = {}
    for row in tmin_df.iter_rows(named=True):
        station = row["station_code"]
        country_code = row["country_code"]
        network_code = row["network_code"]
        
        # Combine the codes into one identifier (e.g., US1CAAL0001)
        combined_station_code = f"{country_code}{network_code}{station}"

        min_temp = float('inf')  # Initialize to a very high value
        min_day = None
        for day_num, col in enumerate(day_columns, start=1):
            if row[col] is not None and row[col] < min_temp:
                min_temp = min(row[col] for col in day_columns if row[col] is not None) / 10
                min_day = day_num

        if min_day is not None:  # Only store if a valid day was found
            year = row["year"]
            month = row["month"]
            date = f"{year}-{month:02d}-{min_day:02d}"  # Format as YYYY-MM-DD

            # Store the result using the combined station code
            if combined_station_code not in result:
                result[combined_station_code] = {"TMIN": min_temp, "date": date}

    return result



def generateMonthlyPub(date_param=None):
    """
    Reads a Parquet file using Polars, processes it for graphing, 
    and then prepares data for charting.
    
    Args:
        date_param (optional): A date parameter (currently unused).
    """
    parquet_path = "/data/ops/ghcnd/TestData_pub/full_data/CA/2/CA_table_data.parquet"
    final_data = {}
    
    try:
        # Load the data lazily using scan_parquet for efficiency
        df = pl.scan_parquet(parquet_path).collect()
        
        #Highest Recorded Temperature + Date
        highestRecordedTempValue = highestRecordedTemp(df)
        print("highestRecordedTempValue: ", highestRecordedTempValue)
        # Add the result to final_data
        for station, temp_data in highestRecordedTempValue.items():
            if station not in final_data:
                final_data[station] = {}
            final_data[station]["HighestTemp"] = temp_data
        
        
        #Lowest Recorded Temperature + Date
        lowestRecordedTempValue = lowestRecordedTemp(df)
        print("lowestRecordedTempValue: ", lowestRecordedTempValue)
        # Add the result to final_data
        for station, temp_data in lowestRecordedTempValue.items():
            if station not in final_data:
                final_data[station] = {}
            final_data[station]["LowestTemp"] = temp_data
        
        
        # Pass data to graphing function
        makeGraph(df)

        # Pass data to chart processing function
        processDataForTable()
        
        with open("JSONresults.json", "w") as json_file:
            json.dump(final_data, json_file, indent=4)

    except Exception as e:
        print(f"Error reading or processing the Parquet file: {e}")

