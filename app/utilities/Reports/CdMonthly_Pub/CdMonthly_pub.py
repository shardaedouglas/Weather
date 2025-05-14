import os
import traceback

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
import calendar
from calendar import monthrange
from datetime import datetime
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
    

def getHighestTemperatureExtreme(df: pl.DataFrame) -> dict:
    # Filter only TMAX records (maximum temperature)
    tmax_df = df.filter(pl.col("observation_type") == "TMAX")
    if tmax_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast values to integers and replace -9999 with nulls
    tmax_df = tmax_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    tmax_df = tmax_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    max_temp = -float("inf")
    station_day_map = defaultdict(list)

    # Iterate rows to find the highest temperature and track which stations and days it occurred on
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

    # Combine tied days and stations
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
    # Filter only TMIN records (minimum temperature)
    tmin_df = df.filter(pl.col("observation_type") == "TMIN")
    if tmin_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast values to integers and replace -9999 with nulls
    tmin_df = tmin_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    tmin_df = tmin_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    min_temp = float("inf")
    station_day_map = defaultdict(list)

    # Iterate rows to find the lowest temperature and track where it occurred
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

    # Combine tied days and stations
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
    # Filter only PRCP records (daily precipitation)
    prcp_df = df.filter(pl.col("observation_type") == "PRCP")
    if prcp_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast values to integers and replace -9999 with nulls
    prcp_df = prcp_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
    prcp_df = prcp_df.with_columns([
        pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
        for col in day_columns
    ])

    station_totals = {}
    # Sum precipitation across all days for each station
    for row in prcp_df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        total = sum(val for col, val in row.items() if col.startswith("day_") and val is not None)
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

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

    # Cast values to integers and replace -9999 with nulls
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

        # Skip stations with missing values for any day
        if any(val is None for val in daily_vals):
            continue

        total = sum(daily_vals)
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

    # Convert to inches and round
    converted_totals = {}
    for station, total in sorted(station_totals.items(), key=lambda x: -x[1]):
        mm = total / 10
        inches = mm / 25.4
        rounded_value = round(inches, 2)
        converted_totals[station] = rounded_value

    min_value = min(converted_totals.values())
    tied_stations = [station for station, val in converted_totals.items() if val == min_value]
    station_str = f"{len(tied_stations)} STATIONS" if len(tied_stations) > 1 else tied_stations[0]

    return {
        "value": min_value,
        "station": station_str
    }

    
    
def getGreatest1DayPrecipitationExtreme(df: pl.DataFrame) -> dict:
    # Filter for daily precipitation observations only
    prcp_df = df.filter(pl.col("observation_type") == "PRCP")
    if prcp_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast day columns to integers and replace missing values (-9999) with nulls
    prcp_df = prcp_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
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
                records.append((val, day_col, ghcn_id))  # Store value, day, and station

    if not records:
        return {}

    max_val = max(records, key=lambda x: x[0])[0]
    tied_records = [r for r in records if r[0] == max_val]

    return {
        "value": round(max_val / 10.0, 1),  # Convert tenths of mm to mm
        "day": "+".join(sorted(r[1].split("_")[1].zfill(2) for r in tied_records)),
        "station": tied_records[0][2] if len(set(r[2] for r in tied_records)) == 1 else "MULTIPLE STATIONS"
    }


def getGreatestTotalSnowfallExtreme(df: pl.DataFrame) -> dict:
    # Filter for daily snowfall observations only
    snow_df = df.filter(pl.col("observation_type") == "SNOW")
    if snow_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast day columns to integers and handle missing data
    snow_df = snow_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])
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
        total = sum(valid_values)  # Sum total snowfall over the month
        station_totals[ghcn_id] = station_totals.get(ghcn_id, 0) + total

    if not station_totals:
        return {}

    max_total = max(station_totals.values())
    tied_stations = [s for s, v in station_totals.items() if v == max_total]

    return {
        "value": round(max_total / 10.0, 1),  # Convert tenths of mm to mm
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }


def getGreatestSnowDepthExtreme(df: pl.DataFrame) -> dict:
    # Filter for snow depth observations only
    snwd_df = df.filter(pl.col("observation_type") == "SNWD")
    if snwd_df.is_empty():
        return {}

    day_columns = [col for col in df.columns if col.startswith("day_")]

    # Cast and clean up missing snow depth values
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
            continue
        max_depth = max(valid_values)  # Highest snow depth recorded for this station
        station_max_depth[ghcn_id] = max(station_max_depth.get(ghcn_id, 0), max_depth)

    if not station_max_depth:
        return {}

    max_value = max(station_max_depth.values())
    tied_stations = [station for station, val in station_max_depth.items() if val == max_value]

    return {
        "value": round(max_value / 10.0, 1),  # Convert tenths of mm to mm
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }
    

def getMonthlyHDD(df: pl.DataFrame) -> dict:
    tmax_data = {}
    tmin_data = {}

    for row in df.iter_rows(named=True):
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        # Collect daily values, keeping -9999 for missing data
        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, 32)
            if f"day_{i}" in row
        ]

        # Remove trailing -9999 values
        while daily_values and daily_values[-1] == -9999:
            daily_values.pop()

        if obs_type == "TMAX":
            tmax_data.setdefault(station_id, []).extend(daily_values)
        elif obs_type == "TMIN":
            tmin_data.setdefault(station_id, []).extend(daily_values)

    # Filter out stations with 10 or more -9999 values in either TMAX or TMIN
    valid_stations = {}
    for station in sorted(set(tmax_data) | set(tmin_data)):
        tmax_count = tmax_data.get(station, []).count(-9999)
        tmin_count = tmin_data.get(station, []).count(-9999)

        if tmax_count < 10 and tmin_count < 10:
            valid_stations[station] = {
                "TMAX": tmax_data.get(station, []),
                "TMIN": tmin_data.get(station, []),
            }

    # Convert to Fahrenheit (from tenths of Celsius), but keep -9999 values
    for station in valid_stations:
        valid_stations[station]["TMAX"] = [
            (value / 10 * 9 / 5) + 32 if value != -9999 else -9999
            for value in valid_stations[station]["TMAX"]
        ]
        valid_stations[station]["TMIN"] = [
            (value / 10 * 9 / 5) + 32 if value != -9999 else -9999
            for value in valid_stations[station]["TMIN"]
        ]

    # Final dictionary to hold the results
    hdd_results = {}

    for station, data in valid_stations.items():
        total_hdd = 0
        valid_days = 0
        missing_days = 0

        # Iterate through the daily data and calculate HDD
        for tmax, tmin in zip(data["TMAX"], data["TMIN"]):
            if tmax == -9999 or tmin == -9999:
                missing_days += 1
                continue

            # Round both, then average, then subtract from 65
            rtmax = round(tmax)
            rtmin = round(tmin)
            avg = (rtmax + rtmin) / 2

            hdd = int(65 - avg) if avg < 65 else 0
            total_hdd += hdd
            valid_days += 1

        # Normalize if there are missing values
        if missing_days > 0:
            avg_hdd = total_hdd / valid_days if valid_days > 0 else 0
            total_hdd = avg_hdd * (valid_days + missing_days)
            total_hdd = round(total_hdd)
            # Append "E" to indicate normalization
            total_hdd = f"{total_hdd}E"

        # Add HDD results for this station
        hdd_results[station] = {
            "total_HDD": total_hdd
        }

    return hdd_results













# def getMonthlyHDD(df: pl.DataFrame) -> dict:
#     tmax_df = df.filter(pl.col("observation_type") == "TMAX")
#     tmin_df = df.filter(pl.col("observation_type") == "TMIN")
#     if tmax_df.is_empty() or tmin_df.is_empty():
#         return {}

#     day_columns = [col for col in df.columns if col.startswith("day_")]

#     # Cast all day values to integers, allow coercion, then replace -9999 with None
#     for data in [tmax_df, tmin_df]:
#         data = data.with_columns([pl.col(day_columns).cast(pl.Int64, strict=False)])
#         data = data.with_columns([
#             pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
#             for col in day_columns
#         ])

#     # Re-assign cleaned data
#     tmax_df = tmax_df.with_columns([
#         pl.col(day_columns).cast(pl.Int64, strict=False)
#     ]).with_columns([
#         pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
#         for col in day_columns
#     ])

#     tmin_df = tmin_df.with_columns([
#         pl.col(day_columns).cast(pl.Int64, strict=False)
#     ]).with_columns([
#         pl.when(pl.col(col) != -9999).then(pl.col(col)).otherwise(None).alias(col)
#         for col in day_columns
#     ])

#     tmax_data = tmax_df.to_dicts()
#     tmin_data = tmin_df.to_dicts()

#     combined_data = {}

#     for tmax_entry in tmax_data:
#         station_code = tmax_entry['station_code']
#         year = tmax_entry['year']
#         month = tmax_entry['month']
#         num_days = calendar.monthrange(year, month)[1]

#         tmin_entry = next(
#             (item for item in tmin_data if item['station_code'] == station_code and
#              item['year'] == year and item['month'] == month),
#             None
#         )
#         if not tmin_entry:
#             continue

#         missing_count = 0
#         daily_values = []
#         sum_of_hdd = 0

#         for day in range(1, num_days + 1):
#             tmax_val = tmax_entry.get(f'day_{day}')
#             tmin_val = tmin_entry.get(f'day_{day}')

#             if tmax_val is None or tmin_val is None:
#                 missing_count += 1

#         if missing_count >= 10:
#             continue  # Skip this station due to too much missing data

#         for day in range(1, 32):  # Always include up to day_31 for completeness
#             tmax_val = tmax_entry.get(f'day_{day}')
#             tmin_val = tmin_entry.get(f'day_{day}')

#             if tmax_val is not None and tmin_val is not None:
#                 tmax_f = round((tmax_val / 10) * 9/5 + 32)
#                 tmin_f = round((tmin_val / 10) * 9/5 + 32)
#                 avg_f = (tmax_f + tmin_f) / 2
#                 hdd = max(65 - avg_f, 0)
#                 daily_values.append(round(hdd, 2))
#                 sum_of_hdd += hdd
#             else:
#                 daily_values.append(None)

#         combined_data[station_code] = {
#             'country_code': tmax_entry['country_code'],
#             'network_code': tmax_entry['network_code'],
#             'station_code': station_code,
#             'year': year,
#             'month': month,
#             'daily_values': daily_values,
#             'sum_of_hdd': round(sum_of_hdd, 2)
#         }

#     return combined_data










# def generateMonthlyPub_hardcoded(date_param=None):
#     """
#     Reads a Parquet file using Polars, processes it for graphing, 
#     and then prepares data for charting.
    
#     Args:
#         date_param (optional): A date parameter (currently unused).
#     """
#     parquet_path = "/data/ops/ghcnd/TestData_pub/full_data/CA/2/CA_table_data.parquet"
#     final_data = {}
    
#     try:
#         # Load the data lazily using scan_parquet for efficiency
#         df = pl.scan_parquet(parquet_path).collect()
        
#         test_json_data = [
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A001",
#                 "observation_type": "TMAX",
#                 "day_1": 210,
#                 "day_2": -9999,
#                 "day_3": 750,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A002",
#                 "observation_type": "TMAX",
#                 "day_1": 300,
#                 "day_2": 350,
#                 "day_3": -9999,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A003",
#                 "observation_type": "TMIN", 
#                 "day_1": -100,   
#                 "day_2": -9999,
#                 "day_3": -100,  
#                 "day_4": -50
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A004",
#                 "observation_type": "TMIN",
#                 "day_1": -9999,
#                 "day_2": -100,
#                 "day_3": -9999,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A005",
#                 "observation_type": "PRCP",
#                 "day_1": 50,
#                 "day_2": 100,
#                 "day_3": 80,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A006",
#                 "observation_type": "PRCP",
#                 "day_1": 225, 
#                 "day_2": 50,
#                 "day_3": -9999,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A007",
#                 "observation_type": "PRCP",
#                 "day_1": 0,
#                 "day_2": 0,
#                 "day_3": 5,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A008",
#                 "observation_type": "PRCP",
#                 "day_1": 2,
#                 "day_2": 0,
#                 "day_3": 0,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A009",
#                 "observation_type": "SNOW",
#                 "day_1": 300,
#                 "day_2": 200,
#                 "day_3": 55,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A010",
#                 "observation_type": "SNOW",
#                 "day_1": 10,
#                 "day_2": 15,
#                 "day_3": -9999,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A011",
#                 "observation_type": "SNWD",
#                 "day_1": 300,
#                 "day_2": 200,
#                 "day_3": 500,
#                 "day_4": -9999
#             },
#             {
#                 "country_code": "US",
#                 "network_code": "1",
#                 "station_code": "A012",
#                 "observation_type": "SNWD",
#                 "day_1": 400,
#                 "day_2": 550,
#                 "day_3": -9999,
#                 "day_4": -9999
#             }

#         ]



#         # Convert to Polars DataFrame
#         testDF = pl.from_dicts(test_json_data)

#         # Call your function
#         result = getHighestTemperatureExtreme(testDF)
#         print("Highest Temp Test Results: ", json.dumps(result, indent=2))
        
#         result = getLowestTemperatureExtreme(testDF)
#         print("Lowest Temp Test Results: ", json.dumps(result, indent=2))
        
#         result = getGreatestTotalPrecipitationExtreme(testDF)
#         print("Greatest Total Precipitation Test: ", json.dumps(result, indent=2))
        
#         result = getLeastTotalPrecipitationExtreme(testDF)
#         print("Least Total Precipitation Test: ", json.dumps(result, indent=2))
        
#         result = getGreatest1DayPrecipitationExtreme(testDF)
#         print("Greatest 1 Day Precipitation Test: ", json.dumps(result, indent=2))
        
#         result = getGreatestTotalSnowfallExtreme(testDF)
#         print("Greatest Total Snowfall Test ", json.dumps(result, indent=2))
        
#         result = getGreatestSnowDepthExtreme(testDF)
#         print("Greatest Snow Depth Test ", json.dumps(result, indent=2))


#         # #Highest Temperature for Exremes data
#         # highestTempExtremeValue = getHighestTemperatureExtreme(df)
#         # print("getHighestTemperatureExtreme: ", highestTempExtremeValue)

        
#         # Pass data to graphing function
#         makeGraph(df)

#         # Pass data to chart processing function
#         processDataForTable()
        
#         with open("JSONresults.json", "w") as json_file:
#             json.dump(final_data, json_file, indent=4)

#     except Exception as e:
#         print(f"Error reading or processing the Parquet file: {e}")
        
        
        


def generateMonthlyPub():
    month = 2
    year = 2023

    try:
        stations = QuerySoM("som")
        print("Station list retrieved.")

        all_filtered_dfs = []
        noDataCount = 0

        for row in stations[:10]:
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

        json_data = json.dumps(combined_df.to_dicts(), indent=2)

        # Optional: write JSON string to file
        output_file = f"combined_data_{month}_{year}.json"
        with open(output_file, "w") as f:
            f.write(json_data)
        
        
        
        
        print(f"Data saved to {output_file}")
        
        # print("Highest Temperature:", getHighestTemperatureExtreme(combined_df))
        # print("Lowest Temperature:", getLowestTemperatureExtreme(combined_df))
        # print("Greatst Total Precip:", getGreatestTotalPrecipitationExtreme(combined_df))
        # print("Least Total Precip:", getLeastTotalPrecipitationExtreme(combined_df))
        # print("Greatest 1-Day Precip:", getGreatest1DayPrecipitationExtreme(combined_df))
        # print("Greatest Snowfall:", getGreatestTotalSnowfallExtreme(combined_df))
        # print("Greatest Snow Depth:", getGreatestSnowDepthExtreme(combined_df))
        print("MonthlyHDD:", getMonthlyHDD(combined_df))

        getMonthlyHDD

    except Exception as e:
        print(f"Error in generateMonthlyPub: {e}")



################################################################################
# Temporary File for Norms functions.

# NOTE: replace these with external functions or variables. 
CoopToGhcn_defaultPath = "/" + os.path.join("data", "ops", "ghcndqi")
imo = 2
hddid=[]
hddval=[]



def round_it(d: float, dec_place: int) -> str:
    val = ""

    if dec_place == 0:
        if d >= 0:
            val = str(d + 0.50000001)
            ix = val.find(".")
            val = val[:ix]
        else:
            val = str(d - 0.50000001)
            ix = val.find(".")
            if ix != -1:
                val = val[:ix]
                if val == "-0":
                    val = "0"

    elif dec_place == 1:
        if d >= 0:
            val = str(d + 0.050000001)
            ix = val.find(".")
            val = val[:ix+2]
        else:
            val = str(d - 0.050000001)
            ix = val.find(".")
            val = val[:ix+2]
            if val == "-0.0":
                val = "0.0"

    elif dec_place == 2:
        if d >= 0:
            val = str(d + 0.0050000001)
            ix = val.find(".")
            val = val[:ix+3]
        else:
            val = str(d - 0.0050000001)
            ix = val.find(".")
            val = val[:ix+3]
            if val == "-0.00":
                val = "0.00"

    return val




###########################################################################################


def get8110shdd(gid: str):
    """
    get8110shdd - get ??? for 1981 - 2010
        @param gid - GHCN Daily ID
        @return str
    """
    dat = ""

    try:
        # Actual Path to use
        fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        # script_dir = os.path.dirname(os.path.abspath(__file__))
        # fn = os.path.join(script_dir, gid + ".normals.txt")

        with open(fn, "r") as file:
            for line in file: 
                if (line is not None):
                    if (len(line) > 29):
                        elem = line[0:23]
                        if ("        ann-htdd-normal" in elem):
                            dat = line[24:29]

                    


    except Exception as err:
        print("{} {}".format(err, gid))

    return dat





def computeDivDFN( id: str,  atmp: str, pcn: str,  mo: str):
    """
    computeDivDFN - Compute Divisioanl DFN.
  		@param id - 4 character string
  		@param atmp
  		@param pcn
  		@param mo
  		@return dfn - list[str]
    """
    dfn = [None,None]

    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641F_1971-2000-NORM_CLIM85.txt")
    line2 = None
    line3 = None

    try:
        # print(fn)
        with open(fn, "r") as file:
            for line in file:
                # print(line)
                if (line is not None):

		# 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
		# 1010119712000  395   438   517   595   677   752   789   778   717   605   507   426    600 2   

                    tid = line[1:5]
                    tt1 = line[0:1]
                    tt2 = line[92:93]

                    if (tid == id):
                        if (tt1 == "1"):
                            if (tt2 == "2"): # Av Temp
                                line2 = line

                        elif (tt1 == "2"): # Precip
                            if (tt2 == "2"):
                                line3 = line
                                break
                else: 
                    break


        if (line2 is not None):
            dfm = []

            dfm.append(line2[13:18])
            dfm.append(line2[19:24])
            dfm.append(line2[25:30])
            dfm.append(line2[30:36])
            dfm.append(line2[37:42])
            dfm.append(line2[43:48])
            dfm.append(line2[49:54])
            dfm.append(line2[55:60])
            dfm.append(line2[61:66])
            dfm.append(line2[67:72])
            dfm.append(line2[73:78])
            dfm.append(line2[79:84])

        

            dfn[0] = dfm[imo-1]



            try:
                ix = atmp.find("M")
                if (ix > -1):
                    atmp = atmp[0:ix]
                

                d1 = float(atmp)
                d2 = float(dfn[0])
                d2 = d2 * 0.1


                d3 = d1 - d2
                dfn[0] = round_it(d3, 1)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(err))
                dfn[0] = " "
              


        else: 
            dfn[0] = " "


        
        if (line3 is not None):
            dfm = []

            dfm.append(line3[13:18])
            dfm.append(line3[19:24])
            dfm.append(line3[25:30])
            dfm.append(line3[30:36])
            dfm.append(line3[37:42])
            dfm.append(line3[43:48])
            dfm.append(line3[49:54])
            dfm.append(line3[55:60])
            dfm.append(line3[61:66])
            dfm.append(line3[67:72])
            dfm.append(line3[73:78])
            dfm.append(line3[79:84])

        


            dfn[1] = dfm[imo-1];

 

            try:
                # ix = atmp.index("M")
                # print(ix)
                # if (ix > -1):
                #     atmp = atmp[:ix]
                

                d1 = float(pcn)
                d2 = float(dfn[1])
                d2 = d2 * 0.01



                d3 = d1 - d2
                dfn[1] = round_it(d3, 2)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(traceback.format_exc()))
                dfn[1] = " "
              



        else: 
            dfn[1] = " "

    except Exception as err: 
        print("error: {}".format(traceback.format_exc()))
    
    return dfn


def loadHddNorm():
    """
    loadHddNorm -  Get Hdd Norms.
    Loads HDD Norms to variables hddval and hddid
    """
    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641C_1971-2000_NORM_CLIM81_MTH_STNNORM")

    try:
        with open(fn, "r") as file:
            for line in file:
                if line is not None:
                    tt1 = line[6:9]
                    # print(line)
                    # print(tt1)
                    if (tt1 == "604"): # HDD
                        # //      1         2         3         4         5         6         7         8         9								
                        # //0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
                        # //014064604   780    587    404    180     41      1      0      0     18    165    417    669    3262
                        tid = line[:6]
                        hddid.append(tid)

                        hdd = line[95:100]
                        hddval.append(hdd)


    
    except Exception as err: 
        print("error: {}".format(traceback.format_exc()))




# NOTE: This should be in ghcnDataBrowser.java
def getMlyNormals8110(gid: str):
    """
    getMlyNormals8110 - Get monthly 81-2010 normals.
        @param gid  - GHCND ID.
        @return list[str]  
    """
    
    tmax = ""
    tmin = ""
    tavg = ""

    cldd = ""
    hldd = ""
    prcp = ""
    snow = ""
    ok = False


    try:
        fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        with open(fn, "r") as file: 
            for line in file: 
                if line is not None:
                    try:
                        header = line[:23]
                        
                        if (header == "        mly-tmax-normal"):
                            tmax = line[23:]
                            ok = True
                        elif (header == "        mly-tavg-normal"):
                            tavg = line[23:]
                        
                        elif (header == "        mly-tmin-normal"):
                            tmin = line[23:]
                        elif (header == "        mly-cldd-normal"):
                            cldd = line[23:]
                        elif (header == "        mly-htdd-normal"):
                            hldd = line[23:]
                        elif (header == "        mly-prcp-normal"):
                            prcp = line[23:]
                        elif (header == "        mly-snow-normal"):
                            snow = line[23:]
                        
                    except Exception as err:
                        print("error: {}".format(traceback.format_exc()))
   

    except Exception as err:
        ok = False

    
# 		/*
#           1         2         3         4         5         6         7         8          
# 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
#    464R   491R   586R   694R   794R   888R   921R   899R   825R   714R   605R   497R
#    318R   341R   401R   508R   609R   707R   750R   731R   653R   541R   436R   348R
# 		 */


    dat = [''] * 7


    if ok:
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            try:
                val = tmax[i1:i1+6]
                d = float(val)
                d *= 0.1

                if (i == 0):
                    rec = round_it(d, 1)
                else:
                    rec = rec+","+round_it(d,1)

            except Exception as err:
                print("{} temp missing".format(gid))
                break
            
            i1 += 7
        
        dat[0] = rec

        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tmin[i1:i1+6]
            d = float(val)
            d *= 0.1


            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec += ","+round_it(d,1)


            i1 += 7
        

        dat[1] = rec


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tavg[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec += ","+round_it(d,1)
            
            i1 += 7
        

        dat[2] = rec
        
        


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = hldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7
        

        # dat[3]
        dat[3] = rec


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = cldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7

        # dat[4]
        dat[4] = rec



        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = prcp[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.01

                    if (i == 0):
                        rec = round_it(d, 2)
                    else:
                        rec += ","+round_it(d,2)
                
                else:
                    if (i == 0):
                        rec = "0.00"
                    else:
                        rec += ",0.00"

            except Exception as err:
                if (i == 0):
                    rec = "null"
                else:
                    rec += ",null"
            
            
            i1 += 7

        # dat[5]
        dat[5] = rec
        

        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = snow[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.1

                    if (i == 0):
                        rec = round_it(d, 1)
                    else:
                        rec += ","+round_it(d,1)
                
                else:
                    if (i == 0):
                        rec = "0.0"
                    else:
                        rec += ",0.0"

            except Exception as err:
                if (i == 0):
                    rec = "0.0"
                else:
                    rec += ",0.0"
            
            
            i1 += 7

        # dat[6]
        dat[6] = rec
    
    return dat
            


# NOTE: This should be in ghcnDataBrowser.java
def getMlyNormals9121(gid: str):
    """
    getMlyNormals9121 - Get monthly 91-2020 normals.
        @param gid  - GHCND ID.
		@return list[str] - 
    """
    tmax = ""
    tmin = ""
    tavg = ""

    cldd = ""
    hldd = ""
    prcp = ""
    snow = ""

    try:
        # fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        #  TESTING
        script_dir = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(script_dir, gid + ".normals.txt")

        with open(fn, "r") as file: 
            for line in file: 
                if line is not None:
                    try:
                        header = line[:23]
                        
                        if (header == "        mly-tmax-normal"):
                            tmax = line[23:]
                            ok = True
                        elif (header == "        mly-tavg-normal"):
                            tavg = line[23:]
                        
                        elif (header == "        mly-tmin-normal"):
                            tmin = line[23:]
                        elif (header == "        mly-cldd-normal"):
                            cldd = line[23:]
                        elif (header == "        mly-htdd-normal"):
                            hldd = line[23:]
                        elif (header == "        mly-prcp-normal"):
                            prcp = line[23:]
                        elif (header == "        mly-snow-normal"):
                            snow = line[23:]
                        
                    except Exception as err:
                        print("error: {}".format(traceback.format_exc()))


    except Exception as err:
        print("error: {}".format(traceback.format_exc()))

    # 			/*
	#           1         2         3         4         5         6         7         8          
	# 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
	#    464R   491R   586R   694R   794R   888R   921R   899R   825R   714R   605R   497R
	#    318R   341R   401R   508R   609R   707R   750R   731R   653R   541R   436R   348R
	# 		 */

    dat = [''] * 7

    if (not tmax == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            try:
                val = tmax[i1:i1+6]
                d = float(val)
                d *= 0.1

                if (i == 0):
                    rec = round_it(d, 1)
                else:
                    rec = rec+","+round_it(d,1)

                
            except Exception as err:
                print("error: {}".format(traceback.format_exc()))
                break

            i1 += 7
        
        dat[0] = rec


    if (not tmin == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tmin[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec = rec+","+round_it(d,1)

            i1 += 7
        # dat[1]
        dat[1] = rec

    if (not tavg == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tavg[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec = rec+","+round_it(d,1)

            i1 += 7
    
        # dat[2]
        dat[2] = rec


    if (not hldd == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = hldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7

        # dat[3]
        dat[3] = rec



    if (not cldd == ""):
        i1 = 0
        rec = ""

        try:
            for i in range(0, 12, 1):
                val = cldd[i1:i1+6]

                if (val == " -7777"):
                    if (i == 0):
                        rec = "0"
                    else:
                        rec += "," + str(0)
                else:
                    if (i == 0):
                        rec = val.strip()
                    else:
                        rec += "," + val.strip()

            
                i1 += 7
        except Exception as err:
            print("error: {}".format(traceback.format_exc()))

        dat[4] = rec


    if (not prcp == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = prcp[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.01

                    if (i == 0):
                        rec = round_it(d, 2)
                    else:
                        rec += ","+round_it(d,2)
                
                else:
                    if (i == 0):
                        rec = "0.00"
                    else:
                        rec += ",0.00"

            except Exception as err:
                if (i == 0):
                    rec = "null"
                else:
                    rec += ",null"
            
            
            i1 += 7

        # dat[5]
        dat[5] = rec

        if (not snow == ""):
            i1 = 0
            rec = ""
            for i in range(0, 12, 1):
                
                try:
                    val = snow[i1:i1+6]
                    if ( not val == " -7777"):
                        d = float(val)
                        d *= 0.1

                        if (i == 0):
                            rec = round_it(d, 1)
                        else:
                            rec += ","+round_it(d,1)
                    
                    else:
                        if (i == 0):
                            rec = "0.0"
                        else:
                            rec += ",0.0"

                except Exception as err:
                    if (i == 0):
                        rec = "0.0"
                    else:
                        rec += ",0.0"
                
                
                i1 += 7

            # dat[6]
            dat[6] = rec

    # print("{}".format(dat))
    return dat

    



def getTempNorm8110(id: str, atmp: str, pcn: str, imo: int):
    """
    getTempNorm8110 - Get temp 71 -2000 Norms. Departure from Normal for Temp and Precip.
     @param id - ghcnd id
     @param atmp -  avg Temp
     @param pcn - Precip total
     @param iMo - Month, integer. Note this is 0 indexed, not 1
     Return - List[str] - dfn
    """
    # I'm not sure what format pcn comes in as.
    dfn = [''] * 2

    # TODO: Update with the external class
    mdfn = getMlyNormals8110(id)
    # mdfn = ghcnDataBrowser.getMlyNormals8110(id)

    trec = mdfn[2]

    if (trec):
        mt = trec.split(",")
        ntmp = mt[imo]

        # multiprint(mt=mt,ntmp=ntmp)

        if (atmp.find("M") >= 0):
            atmp= atmp[0:atmp.find("M")]

        try:
            d1 = float(atmp)
            d2 = float(ntmp)

            d3 = d1- d2

            dfn[0] = round_it(d3, 1)
        except Exception as err: #TODO: Consider traceback.format_exc()
            # print("error: {}".format(traceback.format_exc()))
            dfn[0] = " "

        # multiprint(atmp=atmp, d1=d1, d2=d2, d3=d3, dfn=dfn[0])

    else:
        dfn[0] = " "

    # multiprint(dfn=dfn[0])

    prec = mdfn[5]

    if (prec):
        mt = prec.split(",")
        npcn = mt[imo]


        if( pcn.find("M") >= 0):
            pcn = pcn[:pcn.find("M")+1]
        if( pcn.find("F") >= 0):
            pcn = pcn[:pcn.find("F")+1]
        if( pcn.find("A") >= 0):
            pcn = pcn[:pcn.find("A")+1]



        try:
            d1 = float(pcn)
            d2 = float(npcn)

            d3 = d1- d2

            dfn[1] = round_it(d3, 2)
        except Exception as err: #TODO: Consider traceback.format_exc()
            print("error: {}".format(traceback.format_exc()))
            dfn[1] = " "

        # multiprint(pcn=pcn, d1=d1, d2=d2, d3=d3, dfn=dfn[1])


        # multiprint(mt=mt, npcn=npcn, pcn=pcn)

    else:
        dfn[1] = " "

    return dfn




def getTempNorm9120(id: str, atmp: str, pcn: str, imo: int):
    """
	   getTempNorm9121 - Get temp 91 -2021 Norms.
	     @param id - ghcnd id
	     @param atmp -  avg Temp
	     @param pcn - Precip total
	     @param iMo - Month, integer.

         @return list[str] - dfn
    """
    # # I'm not sure what format pcn comes in as.
    dfn = [''] * 2

    # # TODO: Update with the external class
    mdfn = getMlyNormals9121(id)
    # mdfn = ghcnDataBrowser.getMlyNormals8110(id)

    trec = mdfn[2]
    # trec = ""

    if (trec):
        mt = trec.split(",")
        ntmp = mt[imo]

        # multiprint(mt=mt,ntmp=ntmp)

        if (atmp.find("M") >= 0):
            atmp= atmp[:atmp.find("M")]

        try:
            d1 = float(atmp)
            d2 = float(ntmp)

            d3 = d1- d2

            dfn[0] = round_it(d3, 1)
        except Exception as err: #TODO: Consider traceback.format_exc()
            # print("error: {}".format(traceback.format_exc()))
            dfn[0] = " "

        # multiprint(atmp=atmp, d1=d1, d2=d2, d3=d3, dfn=dfn[0])

    else:
        dfn[0] = " "

    # multiprint(dfn=dfn[0])

    prec = mdfn[5]

    if (prec):
        mt = prec.split(",")
        npcn = mt[imo]


        if( pcn.find("M") >= 0):
            pcn = pcn[:pcn.find("M")+1]
        if( pcn.find("F") >= 0):
            pcn = pcn[:pcn.find("F")+1]
        if( pcn.find("A") >= 0):
            pcn = pcn[:pcn.find("A")+1]



        try:
            d1 = float(pcn)
            d2 = float(npcn)

            d3 = d1- d2

            dfn[1] = round_it(d3, 2)
        except Exception as err: #TODO: Consider traceback.format_exc()
    #         print("error: {}".format(traceback.format_exc()))
            dfn[1] = " "

        # multiprint(pcn=pcn, d1=d1, d2=d2, d3=d3, dfn=dfn[1])


        # multiprint(mt=mt, npcn=npcn, pcn=pcn)

    else:
        dfn[1] = " "

    return dfn






def getTempNorm7100(id: str, atmp: str, pcn:str, imo: int):
    """
    getTempNorm7100 -  Get temp 71 -2000 Norms.
	  @param id - coop id
	  @param atmp -  avg Temp
      @param pcn - Precip total
      @param iMo - Month, integer. 

      @return - list[str] - departure from normal
    
    """
    
    dfn = [''] * 2

    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641C_1971-2000_NORM_CLIM81_MTH_STNNORM")

    line2 = ""
    line3 = ""

    try:
        with open(fn, "r") as file:
            for line in file:
                if (line):
                    
                    tid = line[:6]
                    tt1 = line[6:7]
                    tt2 = line[7:9]

                    if (tid == id):
                        if (tt1 == "3"):
                            if (tt2 == "04"): # Av Temp
                                line2 = line

                        elif (tt1 == "4"): # Precip
                            if (tt2 == "04"):
                                line3 = line
                                break
                    
                else: 
                    break
    
            

    



        if (line2):
            dfm = [''] * 12

            dfm[0]=line2[9:15]
            dfm[1]=line2[15:22]
            dfm[2]=line2[22:29]
            dfm[3]=line2[29:36]
            dfm[4]=line2[36:43]
            dfm[5]=line2[43:50]
            dfm[6]=line2[50:57]
            dfm[7]=line2[57:64]
            dfm[8]=line2[64:71]
            dfm[9]=line2[71:78]
            dfm[10]=line2[78:85]
            dfm[11]=line2[85:92]

            dfn[0] = dfm[imo]

            try:
                ix = atmp.find("M")
                if (ix > -1):
                    atmp = atmp[0:ix]
                ix = atmp.find("F")
                if (ix > -1):
                    atmp = atmp[0:ix]
                

                d1 = float(atmp)
                d2 = float(dfn[0])
                d2 = d2 * 0.1


                d3 = d1 - d2
                dfn[0] = round_it(d3, 1)

                # multiprint(atmp=atmp, d1=d1 ,d2=d2 ,d3=d3 , dfn_0=dfn[0])

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                dfn[0] = " "
        else: 
                dfn[0] = " "    

        # multiprint(dfn_0=dfn[0], atmp=atmp, line3=line3)    

        # line3= None
        if (line3):
            dfm = [''] * 12

            dfm[0]=line2[9:15]
            dfm[1]=line2[15:22]
            dfm[2]=line2[22:29]
            dfm[3]=line2[29:36]
            dfm[4]=line2[36:43]
            dfm[5]=line2[43:50]
            dfm[6]=line2[50:57]
            dfm[7]=line2[57:64]
            dfm[8]=line2[64:71]
            dfm[9]=line2[71:78]
            dfm[10]=line2[78:85]
            dfm[11]=line2[85:92]



            dfn[1] = dfm[imo];

            # multiprint(dfm=dfm)

            try:
                ix = pcn.find("A")
                if (ix > -1):
                    pcn = pcn[ix+1:]


                d1 = float(pcn)
                d2 = float(dfn[1])
                d2 = d2 * 0.01



                d3 = d1 - d2
                dfn[1] = round_it(d3, 2)

                # multiprint(pcn=pcn, d1=d1 ,d2=d2 ,d3=d3 , dfm=dfm, dfn_1=dfn[1])

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                dfn[1] = " "
                
        else: 
            dfn[1] = " "

        # multiprint(dfn_1=dfn[1] ) 

    except Exception as err: #TODO: Consider traceback.format_exc()
            print("error: {}".format(traceback.format_exc()))

    return dfn

