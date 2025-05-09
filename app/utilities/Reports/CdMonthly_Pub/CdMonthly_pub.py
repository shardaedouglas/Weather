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
import json, os
from collections import defaultdict
from calendar import monthrange
from app.utilities.Reports.HomrDB import ConnectDB, QuerySoM
from app.dataingest.readandfilterGHCN import parse_and_filter

def makeGraph(df):
    """
    Placeholder function to process the DataFrame for graphing.
    """
    # print("DF!!!!!!!!!!!!: ", df)
    print("Generating graph...")

def processDataForTable(date_param=None):
    """
    Placeholder function for chart processing.
    """
    print("Processing chart data!")
    
# def highestRecordedTemp(df: pl.DataFrame) -> dict:
#     # Filter only TMAX observation type
#     print(df.select("observation_type").unique())
    
#     tmax_df = df.filter(df["observation_type"] == "TMAX")
    
#     # If no TMAX data is found, return an empty dict
#     if tmax_df.is_empty():
#         print("EMPTY DATA")
#         return {}

#     # Convert day columns to numeric and ignore missing values (-9999)
#     day_columns = [col for col in df.columns if col.startswith("day_")]
#     tmax_df = tmax_df.with_columns([ 
#         pl.col(day_columns).cast(pl.Int64, strict=False).fill_null(-9999)
#     ])

#     # Replace missing values (-9999) with nulls so they don't interfere with max calculations
#     tmax_df = tmax_df.with_columns([ 
#         pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col) 
#         for col in day_columns
#     ])

#     # Find the highest TMAX for each station and the corresponding date
#     result = {}
#     for row in tmax_df.iter_rows(named=True):
#         station = row["station_code"]
#         country_code = row["country_code"]
#         network_code = row["network_code"]
        
#         # Combine the codes into one identifier (e.g., US1CAAL0001)
#         combined_station_code = f"{country_code}{network_code}{station}"

#         max_temp = -float('inf')  # Initialize to a very low value
#         max_day = None
#         for day_num, col in enumerate(day_columns, start=1):
#             if row[col] is not None and row[col] > max_temp:
#                 max_temp = max(row[col] for col in day_columns if row[col] is not None) / 10
#                 max_day = day_num

#         if max_day is not None:  # Only store if a valid day was found
#             year = row["year"]
#             month = row["month"]
#             date = f"{year}-{month:02d}-{max_day:02d}"  # Format as YYYY-MM-DD

#             # Store the result using the combined station code
#             if combined_station_code not in result:
#                 result[combined_station_code] = {"TMAX": max_temp, "date": date}

#     return result




# def lowestRecordedTemp(df: pl.DataFrame) -> dict:
#     # Filter only TMIN observation type
#     print(df.select("observation_type").unique())
    
#     tmin_df = df.filter(df["observation_type"] == "TMIN")
    
#     # If no TMIN data is found, return an empty dict
#     if tmin_df.is_empty():
#         print("EMPTY DATA")
#         return {}

#     # Convert day columns to numeric and ignore missing values (-9999)
#     day_columns = [col for col in df.columns if col.startswith("day_")]
#     tmin_df = tmin_df.with_columns([ 
#         pl.col(day_columns).cast(pl.Int64, strict=False).fill_null(-9999)
#     ])

#     # Replace missing values (-9999) with nulls so they don't interfere with min calculations
#     tmin_df = tmin_df.with_columns([ 
#         pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col) 
#         for col in day_columns
#     ])

#     # Find the lowest TMIN for each station and the corresponding date
#     result = {}
#     for row in tmin_df.iter_rows(named=True):
#         station = row["station_code"]
#         country_code = row["country_code"]
#         network_code = row["network_code"]
        
#         # Combine the codes into one identifier (e.g., US1CAAL0001)
#         combined_station_code = f"{country_code}{network_code}{station}"

#         min_temp = float('inf')  # Initialize to a very high value
#         min_day = None
#         for day_num, col in enumerate(day_columns, start=1):
#             if row[col] is not None and row[col] < min_temp:
#                 min_temp = min(row[col] for col in day_columns if row[col] is not None) / 10
#                 min_day = day_num

#         if min_day is not None:  # Only store if a valid day was found
#             year = row["year"]
#             month = row["month"]
#             date = f"{year}-{month:02d}-{min_day:02d}"  # Format as YYYY-MM-DD

#             # Store the result using the combined station code
#             if combined_station_code not in result:
#                 result[combined_station_code] = {"TMIN": min_temp, "date": date}

#     return result


def getHighestTemperatureExtreme(df: pl.DataFrame) -> dict:
    # Filter only TMAX records
    tmax_df = df.filter(pl.col("observation_type") == "TMAX")
    if tmax_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to Int64, replace -9999 with nulls
    tmax_df = tmax_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    tmax_df = tmax_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    max_temp = -float("inf")
    station_day_map = defaultdict(list)

    for row in tmax_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        for day_num, col in enumerate(day_columns, start=1):
            val = row[col]
            if val is not None:
                temp_c = val / 10.0
                if temp_c > max_temp:
                    max_temp = temp_c
                    station_day_map = defaultdict(list)
                    station_day_map[ghcn_id].append(day_num)
                elif temp_c == max_temp:
                    station_day_map[ghcn_id].append(day_num)

    # Flatten tied days and stations
    tied_days = sorted({day for days in station_day_map.values() for day in days})
    tied_stations = list(station_day_map.keys())

    day_str = f"{tied_days[0]:02d}" + ("+" if len(tied_days) > 1 else "")
    station_str = f"{len(tied_stations)} STATIONS" if len(tied_stations) > 1 else tied_stations[0]

    return {
        "value": max_temp,
        "day": day_str,
        "station": station_str
    }


def getLowestTemperatureExtreme(df: pl.DataFrame) -> dict:
    # Filter only TMIN records
    tmin_df = df.filter(pl.col("observation_type") == "TMIN")
    if tmin_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to Int64, replace -9999 with nulls
    tmin_df = tmin_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    tmin_df = tmin_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    min_temp = float("inf")
    station_day_map = defaultdict(list)

    for row in tmin_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        for day_num, col in enumerate(day_columns, start=1):
            val = row[col]
            if val is not None:
                temp_c = val / 10.0
                if temp_c < min_temp:
                    min_temp = temp_c
                    station_day_map = defaultdict(list)
                    station_day_map[ghcn_id].append(day_num)
                elif temp_c == min_temp:
                    station_day_map[ghcn_id].append(day_num)

    tied_days = sorted({day for days in station_day_map.values() for day in days})
    tied_stations = list(station_day_map.keys())

    day_str = f"{tied_days[0]:02d}" + ("+" if len(tied_days) > 1 else "")
    station_str = f"{len(tied_stations)} STATIONS" if len(tied_stations) > 1 else tied_stations[0]

    return {
        "value": min_temp,
        "day": day_str,
        "station": station_str
    }


def getGreatestTotalPrecipitationExtreme(df: pl.DataFrame) -> dict:
    # Filter only PRCP records
    prcp_df = df.filter(pl.col("observation_type") == "PRCP")
    if prcp_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to Int64, replace -9999 with nulls
    prcp_df = prcp_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    prcp_df = prcp_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    station_totals = {}
    for row in prcp_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        total = sum(val for col, val in row.items() if col.startswith("day_") and val is not None)
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

    # Find the maximum total precipitation
    max_total = max(station_totals.values())
    tied_stations = [station for station, total in station_totals.items() if total == max_total]

    return {
        "value": round(max_total / 10.0, 1),  # Convert to mm
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }
    




def getLeastTotalPrecipitationExtreme(df: pl.DataFrame) -> dict:
    # Filter only PRCP records
    prcp_df = df.filter(pl.col("observation_type") == "PRCP")
    if prcp_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to Int64 and replace -9999 with nulls
    prcp_df = prcp_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    prcp_df = prcp_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    station_totals = {}
    for row in prcp_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        year, month = row['year'], row['month']
        _, days_in_month = monthrange(year, month)

        daily_vals = [row[f"day_{i}"] for i in range(1, days_in_month + 1)]

        # Skip if any daily value is missing
        if any(val is None for val in daily_vals):
            continue

        total = sum(daily_vals)  # still in tenths of mm
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

    converted_totals = {}
    for station, total in sorted(station_totals.items(), key=lambda x: -x[1]):
        mm = total / 10
        inches = mm / 25.4
        roundedValue = round(inches, 2)
        converted_totals[station] = roundedValue

    # Find minimum value after conversion and rounding
    min_value = min(converted_totals.values())
    tied_stations = [station for station, val in converted_totals.items() if val == min_value]
    station_str = f"{len(tied_stations)} STATIONS" if len(tied_stations) > 1 else tied_stations[0]

    return {
        "value": min_value,
        "station": station_str
    }
    
    
    
def getGreatest1DayPrecipitationExtreme(df: pl.DataFrame) -> dict:
    prcp_df = df.filter(pl.col("observation_type") == "PRCP")
    if prcp_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to integers first
    prcp_df = prcp_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    # Then replace -9999 with nulls
    prcp_df = prcp_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    records = []

    for row in prcp_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        for day_col in day_columns:
            val = row[day_col]
            if val is not None:
                records.append((val, day_col, ghcn_id))

    if not records:
        return {}

    max_val = max(records, key=lambda x: x[0])[0]
    tied_records = [r for r in records if r[0] == max_val]

    return {
        "value": round(max_val / 10.0, 1),
        "day": "+".join(sorted(r[1].split("_")[1].zfill(2) for r in tied_records)),
        "station": tied_records[0][2] if len(set(r[2] for r in tied_records)) == 1 else "MULTIPLE STATIONS"
    }



def getGreatestTotalSnowfallExtreme(df: pl.DataFrame) -> dict:
    snow_df = df.filter(pl.col("observation_type") == "SNOW")
    if snow_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to integers first
    snow_df = snow_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    # Then replace -9999 with nulls
    snow_df = snow_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    station_totals = {}
    for row in snow_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        valid_values = [val for col, val in row.items() if col.startswith("day_") and val is not None]
        if not valid_values:
            continue
        total = sum(valid_values)
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

    max_total = max(station_totals.values())
    tied_stations = [s for s, v in station_totals.items() if v == max_total]

    return {
        "value": round(max_total / 10.0, 1),
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }

    
    
def getGreatestSnowDepthExtreme(df: pl.DataFrame) -> dict:
    # Filter only SNWD records
    snwd_df = df.filter(pl.col("observation_type") == "SNWD")
    if snwd_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast to Int64, replace -9999 with nulls
    snwd_df = snwd_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    snwd_df = snwd_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    station_max_depth = {}
    for row in snwd_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        valid_values = [val for col, val in row.items() if col.startswith("day_") and val is not None]
        if not valid_values:
            continue  # Skip stations with no valid data
        max_depth = max(valid_values)
        station_max_depth[ghcn_id] = max(station_max_depth.get(ghcn_id, 0), max_depth)

    if not station_max_depth:
        return {}

    max_value = max(station_max_depth.values())
    tied_stations = [station for station, val in station_max_depth.items() if val == max_value]

    return {
        "value": round(max_value / 10.0, 1),  # Convert to mm
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }





def generateMonthlyPub_hardcoded(date_param=None):
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
        
        test_json_data = [
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A001",
                "observation_type": "TMAX",
                "day_1": 210,
                "day_2": -9999,
                "day_3": 750,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A002",
                "observation_type": "TMAX",
                "day_1": 300,
                "day_2": 350,
                "day_3": -9999,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A003",
                "observation_type": "TMIN", 
                "day_1": -100,   
                "day_2": -9999,
                "day_3": -100,  
                "day_4": -50
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A004",
                "observation_type": "TMIN",
                "day_1": -9999,
                "day_2": -100,
                "day_3": -9999,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A005",
                "observation_type": "PRCP",
                "day_1": 50,
                "day_2": 100,
                "day_3": 80,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A006",
                "observation_type": "PRCP",
                "day_1": 225, 
                "day_2": 50,
                "day_3": -9999,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A007",
                "observation_type": "PRCP",
                "day_1": 0,
                "day_2": 0,
                "day_3": 5,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A008",
                "observation_type": "PRCP",
                "day_1": 2,
                "day_2": 0,
                "day_3": 0,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A009",
                "observation_type": "SNOW",
                "day_1": 300,
                "day_2": 200,
                "day_3": 55,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A010",
                "observation_type": "SNOW",
                "day_1": 10,
                "day_2": 15,
                "day_3": -9999,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A011",
                "observation_type": "SNWD",
                "day_1": 300,
                "day_2": 200,
                "day_3": 500,
                "day_4": -9999
            },
            {
                "country_code": "US",
                "network_code": "1",
                "station_code": "A012",
                "observation_type": "SNWD",
                "day_1": 400,
                "day_2": 550,
                "day_3": -9999,
                "day_4": -9999
            }

        ]



        # Convert to Polars DataFrame
        testDF = pl.from_dicts(test_json_data)

        # Call your function
        result = getHighestTemperatureExtreme(testDF)
        print("Highest Temp Test Results: ", json.dumps(result, indent=2))
        
        result = getLowestTemperatureExtreme(testDF)
        print("Lowest Temp Test Results: ", json.dumps(result, indent=2))
        
        result = getGreatestTotalPrecipitationExtreme(testDF)
        print("Greatest Total Precipitation Test: ", json.dumps(result, indent=2))
        
        result = getLeastTotalPrecipitationExtreme(testDF)
        print("Least Total Precipitation Test: ", json.dumps(result, indent=2))
        
        result = getGreatest1DayPrecipitationExtreme(testDF)
        print("Greatest 1 Day Precipitation Test: ", json.dumps(result, indent=2))
        
        result = getGreatestTotalSnowfallExtreme(testDF)
        print("Greatest Total Snowfall Test ", json.dumps(result, indent=2))
        
        result = getGreatestSnowDepthExtreme(testDF)
        print("Greatest Snow Depth Test ", json.dumps(result, indent=2))


        # #Highest Temperature for Exremes data
        # highestTempExtremeValue = getHighestTemperatureExtreme(df)
        # print("getHighestTemperatureExtreme: ", highestTempExtremeValue)

        
        # Pass data to graphing function
        makeGraph(df)

        # Pass data to chart processing function
        processDataForTable()
        
        with open("JSONresults.json", "w") as json_file:
            json.dump(final_data, json_file, indent=4)

    except Exception as e:
        print(f"Error reading or processing the Parquet file: {e}")
        
        
        
        



def generateMonthlyPub():
    month = 2
    year = 2023

    try:
        stations = QuerySoM("som")
        print("Station list retrieved.")

        all_filtered_dfs = []
        noDataCount = 0

        for row in stations:
            ghcn_id = row[4]
            file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"

            if not os.path.exists(file_path):
                print(f"Missing file: {file_path}")
                continue

            try:
                filtered_data = parse_and_filter(
                    station_code=ghcn_id,
                    file_path=file_path,
                    correction_type="table",
                    month=month,
                    year=year
                )

                filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

                if filtered_df.is_empty():
                    print(f"Skipping station {ghcn_id} due to no data.")
                    noDataCount += 1
                    continue

                if all_filtered_dfs:
                    existing_columns = all_filtered_dfs[0].columns
                    current_columns = filtered_df.columns

                    missing_columns = set(existing_columns) - set(current_columns)
                    for col in missing_columns:
                        filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

                    filtered_df = filtered_df.select(existing_columns)

                all_filtered_dfs.append(filtered_df)
                print(f"Parsed {len(filtered_df)} records from {ghcn_id}")

            except Exception as e:
                print(f"Error parsing {ghcn_id}: {e}")
                continue

        if not all_filtered_dfs:
            print("No valid station files found.")
            return

        combined_df = pl.concat(all_filtered_dfs, how="vertical")
        output_file = f"combined_data_{month}_{year}.json"
        combined_df.write_json(output_file)
        print(f"Data saved to {output_file}")
        
        print("Highest Temperature:", getHighestTemperatureExtreme(combined_df))
        print("Lowest Temperature:", getLowestTemperatureExtreme(combined_df))
        print("Greatst Total Precip:", getGreatestTotalPrecipitationExtreme(combined_df))
        print("Least Total Precip:", getLeastTotalPrecipitationExtreme(combined_df))
        print("Greatest 1-Day Precip:", getGreatest1DayPrecipitationExtreme(combined_df))
        print("Greatest Snowfall:", getGreatestTotalSnowfallExtreme(combined_df))
        print("Greatest Snow Depth:", getGreatestSnowDepthExtreme(combined_df))

    except Exception as e:
        print(f"Error in generateMonthlyPub: {e}")




# def parse_and_filter_dly_file(station_code: str, file_path: str, correction_type: str, month: int = None, year: int = None) -> pl.DataFrame:
#     try:
#         # Run the parse_and_filter to get filtered data
#         filtered_data = parse_and_filter(
#             station_code=station_code,
#             file_path=file_path,
#             correction_type=correction_type,
#             month=month,
#             year=year
#         )
#         print("filtered_data", filtered_data)
#         # Ensure the filtered_data is converted to a Polars DataFrame (if it's not already one)
#         if isinstance(filtered_data, dict):
#             # If it's a dictionary, we should convert it to a Polars DataFrame
#             return pl.DataFrame(filtered_data)
#         else:
#             # Otherwise, assume it's already a Polars DataFrame
#             return filtered_data

#     except Exception as e:
#         print(f"Error while parsing the file: {e}")
#         return pl.DataFrame()  # Return an empty DataFrame if there's an error


# def generateMonthlyPub():
#     month = 2
#     year = 2023
    
#     try:
#         # Step 1: Query DB for stations
#         stations = QuerySoM("som")

#         print("STATION LIST: ", stations)

#         all_filtered_dfs = []  # List to accumulate filtered DataFrames
#         noDataCount = 0

#         for row in stations:
#             ghcn_id = row[4]  # assuming ghcn_id is at index 4
#             file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
#             if os.path.exists(file_path):
#                 # Use the new function to get the filtered data
#                 filtered_data = parse_and_filter_dly_file(
#                     station_code=ghcn_id,
#                     file_path=file_path,
#                     correction_type="graph",
#                     month=month,
#                     year=year    
#                 )

#                 # Ensure the filtered_data is converted to a Polars DataFrame (if it's not already one)
#                 if isinstance(filtered_data, dict):
#                     # If it's a dictionary, we should convert it to a Polars DataFrame
#                     filtered_df = pl.DataFrame(filtered_data)
#                 else:
#                     # Otherwise, assume it's already a Polars DataFrame
#                     filtered_df = filtered_data

#                 # Check if the filtered DataFrame is empty
#                 if filtered_df.is_empty():
#                     print(f"Skipping station {ghcn_id} due to no data.")
#                     noDataCount += 1
#                     continue  # Skip this station and move to the next
                
#                 # Align columns by adding missing columns to the DataFrame
#                 if all_filtered_dfs:
#                     # Get the columns of the first DataFrame
#                     existing_columns = all_filtered_dfs[0].columns
#                     current_columns = filtered_df.columns

#                     # Add missing columns to current DataFrame, with None values
#                     missing_columns = set(existing_columns) - set(current_columns)
#                     for col in missing_columns:
#                         filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

#                     # Ensure columns are in the same order
#                     filtered_df = filtered_df.select(existing_columns)
                
#                 # Append to list of DataFrames
#                 all_filtered_dfs.append(filtered_df)
#                 print(f"Parsed {len(filtered_df)} records from {ghcn_id}")
#             else:
#                 print(f"Missing file: {file_path}")
                
#         if not all_filtered_dfs:
#             print("No valid station files found.")
#             return

#         # Combine all DataFrames
#         combined_df = pl.concat(all_filtered_dfs)
        
#         output_file = f"combined_data_{month}_{year}.json"
#         combined_df.write_json(output_file)
#         print(f"Data saved to {output_file}")

#         # Print extreme values
#         print("Highest Temperature:", getHighestTemperatureExtreme(combined_df))
#         print("Lowest Temperature:", getLowestTemperatureExtreme(combined_df))
#         print("Greatest Total Precip:", getGreatestTotalPrecipitationExtreme(combined_df))
#         print("Least Total Precip:", getLeastTotalPrecipitationExtreme(combined_df))
#         print("Greatest 1-Day Precip:", getGreatest1DayPrecipitationExtreme(combined_df))
#         print("Greatest Snowfall:", getGreatestTotalSnowfallExtreme(combined_df))
#         print("Greatest Snow Depth:", getGreatestSnowDepthExtreme(combined_df))

#     except Exception as e:
#         print(f"Error in generateMonthlyPub: {e}")
