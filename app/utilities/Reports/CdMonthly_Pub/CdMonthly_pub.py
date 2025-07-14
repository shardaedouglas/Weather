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
import json, os, math
from collections import defaultdict
import calendar
from calendar import monthrange
from datetime import datetime
# from app.utilities.Reports.HomrDB import ConnectDB, QuerySoM
from app.utilities.Reports.HomrDB import ConnectDB, QueryDB, QuerySoM, DailyPrecipQuery
from app.dataingest.readandfilterGHCN import parse_and_filter
from typing import List, Dict, Any

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
    
    
    
    
def add_station_names(data_dict, station_file_path='/data/ops/ghcnd/data/ghcnd-stations.txt'):

    # Load mapping from file
    station_names = {}
    with open(station_file_path, 'r') as f:
        for line in f:
            ghcn_id = line[:11].strip()
            name = line[41:71].strip()
            station_names[ghcn_id] = name

    # Add staiton name to output.
    updated_dict = {}
    for ghcn_id, data in data_dict.items():
        station_name = station_names.get(ghcn_id, "UNKNOWN")
        if isinstance(data, dict):
            updated = data.copy()
            updated["station_name"] = station_name
        else:
            updated = {
                "value": data,
                "station_name": station_name
            }
        updated_dict[ghcn_id] = updated
    
    return updated_dict




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
                temp_f = (val / 10.0) * 9 / 5 + 32
                if temp_f > max_temp:
                    max_temp = round(temp_f)
                    station_day_map = defaultdict(list)
                    station_day_map[ghcn_id].append(day_num)
                elif temp_f == max_temp:
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
                temp_f = (val / 10.0) * 9 / 5 + 32
                if temp_f < min_temp:
                    min_temp = round(temp_f)
                    station_day_map = defaultdict(list)
                    station_day_map[ghcn_id].append(day_num)
                elif temp_f == min_temp:
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
        "value": round(float(max_total) / 10 / 25.4, 2),
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
        "value": round(float(max_val) / 10 / 25.4, 2),
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
        "value": round((max_total / 10.0) / 2.54, 1),  # Convert tenths of mm to mm
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
        "value": round((max_value / 10.0) / 2.54),  # Convert tenths of mm to mm
        "station": "MULTIPLE STATIONS" if len(tied_stations) > 1 else tied_stations[0]
    }
    
  ####################
  ####END EXTREMES####
  #################### 
    
  
  
def dataframe_to_json(df: pl.DataFrame) -> str:
    # Convert to list of dicts
    records = df.to_dicts()

    # Convert list to dict keyed by 'station_code'
    json_dict = {
        row["station_code"]: {
            key: value for key, value in row.items() if key != "station_code"
        }
        for row in records
    }

    # Dump as JSON string
    return json.dumps(json_dict, indent=4)
  
  
  
def calculate_station_avg(df: pl.DataFrame) -> pl.DataFrame:
    
    df = df.with_columns([
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("station_code")
    ])

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    tmax_df = df.filter(pl.col("observation_type") == "TMAX")
    tmin_df = df.filter(pl.col("observation_type") == "TMIN")

    day_columns = [f"day_{i}" for i in range(1, 32)]

    def count_valid(obs_df, colname):
        return (
            obs_df.select(["station_code"] + day_columns)
            .melt(id_vars=["station_code"], variable_name="day", value_name=colname)
            .with_columns((pl.col(colname) != "-9999").cast(pl.Int8))
            .group_by("station_code").agg(pl.col(colname).sum().alias(f"valid_{colname}"))
        )

    valid_tmax = count_valid(tmax_df, "tmax")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!valid_tmax", valid_tmax)
    valid_tmin = count_valid(tmin_df, "tmin")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!valid_tmin", valid_tmin)

    tmax_long = tmax_df.unpivot(index=["station_code"], on=day_columns, variable_name="day", value_name="tmax").filter(pl.col("tmax") != "-9999")
    tmin_long = tmin_df.unpivot(index=["station_code"], on=day_columns, variable_name="day", value_name="tmin").filter(pl.col("tmin") != "-9999")

    tmax_long = tmax_long.with_columns([((pl.col("tmax").cast(pl.Int64) / 10) * 9 / 5 + 32).alias("tmax")])
    tmin_long = tmin_long.with_columns([((pl.col("tmin").cast(pl.Int64) / 10) * 9 / 5 + 32).alias("tmin")])

    tmax_avg = tmax_long.group_by("station_code").agg(pl.col("tmax").mean().alias("tmax_avg"))
    tmin_avg = tmin_long.group_by("station_code").agg(pl.col("tmin").mean().alias("tmin_avg"))

    tmax_90 = tmax_long.filter(pl.col("tmax") >= 90).group_by("station_code").len().rename({"len": "TMAX>=90"})
    tmax_32 = tmax_long.filter(pl.col("tmax") <= 32).group_by("station_code").len().rename({"len": "TMAX<=32"})
    tmin_32 = tmin_long.filter(pl.col("tmin") <= 32).group_by("station_code").len().rename({"len": "TMIN<=32"})
    tmin_0 = tmin_long.filter(pl.col("tmin") <= 0).group_by("station_code").len().rename({"len": "TMIN<=0"})

    result = tmax_avg.join(tmin_avg, on="station_code", how="full")
    result = result.join(tmax_90, on="station_code", how="left")
    result = result.join(tmax_32, on="station_code", how="left")
    result = result.join(tmin_32, on="station_code", how="left")
    result = result.join(tmin_0, on="station_code", how="left")
    result = result.join(valid_tmax, on="station_code", how="left")
    result = result.join(valid_tmin, on="station_code", how="left")

    result = result.fill_null(0)
    result = result.with_columns(((pl.col("tmax_avg") + pl.col("tmin_avg")) / 2).alias("overall_avg"))

    def label_avg(value, valid_days):
        missing = num_days - valid_days
        if missing >= 10:
            return "M"
        elif missing > 0:
            return f"{round(value, 1)}M"
        else:
            return round(value, 1)
    
    return {
    row["station_code"]: {
        "Average Maximum": label_avg(row["tmax_avg"], row["valid_tmax"]),
        "Average Minimum": label_avg(row["tmin_avg"], row["valid_tmin"]),
        ">=90_MAX": int(round(row["TMAX>=90"], 1)),
        "<=32_MAX": int(round(row["TMAX<=32"], 1)),
        "<=32_MIN": int(round(row["TMIN<=32"], 1)),
        "<=0_MIN": int(round(row["TMIN<=0"], 1)),
        "Average": label_avg(row["overall_avg"], min(row["valid_tmax"], row["valid_tmin"])),
    }
    for row in result.iter_rows(named=True)
}

    
    
    





    
    
def highestRecordedTemp(df: pl.DataFrame) -> dict:
    # Filter only TMAX observation type
    # print(df.select("observation_type").unique())
    
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

        combined_station_code = f"{country_code}{network_code}{station}"

        values = [(i + 1, row[col]) for i, col in enumerate(day_columns) if row[col] is not None]
        if not values:
            continue

        max_val = max(v for _, v in values)
        max_days = [day for day, val in values if val == max_val]
        max_temp = round((max_val / 10) * 9 / 5 + 32)

        year = row["year"]
        month = row["month"]
        last_day = max(max_days)
        date = f"{year}-{month:02d}-{last_day:02d}" + ("+" if len(max_days) > 1 else "")

        if combined_station_code not in result:
            result[combined_station_code] = {"value": max_temp, "date": date}


    return result




def lowestRecordedTemp(df: pl.DataFrame) -> dict:
    # Filter only TMIN observation type
    # print(df.select("observation_type").unique())
    
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

        values = [(i + 1, row[col]) for i, col in enumerate(day_columns) if row[col] is not None]
        if not values:
            continue

        min_val = min(v for _, v in values)
        min_days = [day for day, val in values if val == min_val]
        min_temp = round((min_val / 10) * 9 / 5 + 32)
        
        year = row["year"]
        month = row["month"]
        last_day = max(min_days)
        date = f"{year}-{month:02d}-{last_day:02d}" + ("+" if len(min_days) > 1 else "")

        if combined_station_code not in result:
            result[combined_station_code] = {"value": min_temp, "date": date}

    return result

    

    
    
    
    
    
    

def getMonthlyHDD(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    # Determine the number of days in the month
    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    tmax_data = {}
    tmin_data = {}

    # Step 1: Collect TMAX and TMIN values
    for row in df.iter_rows(named=True):
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)
        ]

        if obs_type == "TMAX":
            tmax_data.setdefault(station_id, []).extend(daily_values)
        elif obs_type == "TMIN":
            tmin_data.setdefault(station_id, []).extend(daily_values)

    # print("Stations with TMAX:", list(tmax_data.keys()))
    # print("Stations with TMIN:", list(tmin_data.keys()))

    # Step 2: Use all stations regardless of missing data
    valid_stations = {}
    all_stations = sorted(set(tmax_data) | set(tmin_data))
    # print("All candidate stations:", all_stations)

    for station in all_stations:
        valid_stations[station] = {
            "TMAX": tmax_data.get(station, []),
            "TMIN": tmin_data.get(station, []),
        }

    # print("Valid stations after missing value check:", list(valid_stations.keys()))

    # Step 3: Convert to Fahrenheit
    for station in valid_stations:
        valid_stations[station]["TMAX"] = [
            (value / 10 * 9 / 5) + 32 if value != -9999 else -9999
            for value in valid_stations[station]["TMAX"]
        ]
        valid_stations[station]["TMIN"] = [
            (value / 10 * 9 / 5) + 32 if value != -9999 else -9999
            for value in valid_stations[station]["TMIN"]
        ]

    # Step 4: Calculate HDD
    hdd_results = {}

    for station, data in valid_stations.items():
        total_hdd = 0
        valid_days = 0
        missing_days = 0

        for tmax, tmin in zip(data["TMAX"], data["TMIN"]):
            if tmax == -9999 or tmin == -9999:
                missing_days += 1
                continue

            rtmax = round(tmax)
            rtmin = round(tmin)
            avg = (rtmax + rtmin) / 2

            hdd = int(65 - avg) if avg < 65 else 0
            total_hdd += hdd
            valid_days += 1

        if missing_days > 0:
            avg_hdd = total_hdd / valid_days if valid_days > 0 else 0
            total_hdd = avg_hdd * (valid_days + missing_days)
            total_hdd = round(total_hdd)
            total_hdd = f"{total_hdd}E"

        hdd_results[station] = {
            "total_HDD": total_hdd
        }

    # print("Final stations in HDD results:", list(hdd_results.keys()))

    return hdd_results




def getMonthlyTemperatureThresholdCounts(df: pl.DataFrame) -> dict:
    """
    Returns a dictionary mapping station ID (ghcn_id) to a dictionary with counts of temperature threshold exceedances:
    - tmax_ge_90: TMAX >= 90°F
    - tmax_le_32: TMAX <= 32°F
    - tmin_le_32: TMIN <= 32°F
    - tmin_le_0:  TMIN <= 0°F
    """
    threshold_counts = defaultdict(lambda: {
        "tmax_ge_90": 0,
        "tmax_le_32": 0,
        "tmin_le_32": 0,
        "tmin_le_0": 0,
    })

    # Filter to only TMAX and TMIN
    df = df.filter(pl.col("observation_type").is_in(["TMAX", "TMIN"]))

    for row in df.iter_rows(named=True):
        ghcn_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]
        year = row["year"]
        month = row["month"]

        _, days_in_month = monthrange(year, month)

        valid_data_found = False

        for day in range(1, days_in_month + 1):
            val_str = row.get(f"day_{day}")
            try:
                val = int(val_str)
            except (ValueError, TypeError):
                continue
            if val == -9999:
                continue

            valid_data_found = True

            # Convert tenths °C to °F
            temp_f = val * 9 / 5 / 10 + 32

            if obs_type == "TMAX":
                if temp_f >= 90:
                    threshold_counts[ghcn_id]["tmax_ge_90"] += 1
                if temp_f <= 32:
                    threshold_counts[ghcn_id]["tmax_le_32"] += 1
            elif obs_type == "TMIN":
                if temp_f <= 32:
                    threshold_counts[ghcn_id]["tmin_le_32"] += 1
                if temp_f <= 0:
                    threshold_counts[ghcn_id]["tmin_le_0"] += 1

        if not valid_data_found:
            threshold_counts[ghcn_id] = {
                "tmax_ge_90": "no_data",
                "tmax_le_32": "no_data",
                "tmin_le_32": "no_data",
                "tmin_le_0": "no_data",
            }

    return threshold_counts


def getTotalSnowAndIcePellets(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    day_columns = [f"day_{i}" for i in range(1, num_days + 1)]

    # Filter for SNOW and WT04 observations only
    snow_df = df.filter(pl.col("observation_type").is_in(["SNOW", "WT04"]))

    # Cast daily values to Int64 (to be safe)
    snow_df = snow_df.with_columns([
        pl.col(day_columns).cast(pl.Int64, strict=False)
    ])

    # Create station ID
    snow_df = snow_df.with_columns([
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("ghcn_id")
    ])

    station_sums = {}
    for row in snow_df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        daily_values = [row[col] for col in day_columns]
        missing_days = sum(1 for val in daily_values if val == -9999)

        # Convert and round each daily value before summing
        valid_sum_in = sum(
            round(val * 0.03937, 1) for val in daily_values if val != -9999
        )

        if ghcn_id not in station_sums:
            station_sums[ghcn_id] = {"total_in": 0, "missing": 0}
        station_sums[ghcn_id]["total_in"] += valid_sum_in
        station_sums[ghcn_id]["missing"] += missing_days

    # Build initial results from stations with data
    result = {}
    for station, data in station_sums.items():
        if data["missing"] >= 10:
            result[station] = "M"
        elif 1 <= data["missing"] <= 9:
            result[station] = f"M {round(data['total_in'], 1)}"
        else:
            result[station] = round(data["total_in"], 1)

    # Now, include stations in original df but NOT in snow_df, mark them as "M"
    # Fix: safely handle None values
    original_station_ids = set(
        (
            (df[row, "country_code"] or "") +
            (df[row, "network_code"] or "") +
            (df[row, "station_code"] or "")
        )
        for row in range(df.height)
    )
    processed_station_ids = set(station_sums.keys())

    for station_id in original_station_ids - processed_station_ids:
        result[station_id] = "M"

    return result




def getMaxDepthOnGround(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]
    valid_day_cols = [f"day_{i}" for i in range(1, num_days + 1)]
    valid_flag_cols = [f"day_{i}_flag" for i in range(1, num_days + 1)]

    snwd_df = df.filter(pl.col("observation_type") == "SNWD").with_columns([
        pl.col(valid_day_cols).cast(pl.Int64, strict=False),
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("ghcn_id")
    ])

    station_max = {}

    for row in snwd_df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        daily_values = [row.get(f"day_{i}", None) for i in range(1, num_days + 1)]

        # Count how many days are missing/null (-9999 or None)
        missing_days = sum(1 for val in daily_values if val is None or val == -9999)

        # Exclude station ONLY if all days missing (all 31 days missing)
        if missing_days == num_days:
            continue  # exclude station completely

        # Filter to valid days: any day that is not missing (-9999 or None)
        valid_days = [(i, val) for i, val in enumerate(daily_values, start=1) if val is not None and val != -9999]

        if not valid_days:
            continue  # no valid days at all, exclude

        # Find max value and last day of that max
        max_val = max(val for i, val in valid_days)
        max_days = [i for i, val in valid_days if val == max_val]
        max_day = max_days[-1]  # last tie wins

        if max_val == 0:
            station_max[ghcn_id] = (0, "")
        else:
            inches = round(max_val * 0.03937)
            station_max[ghcn_id] = (inches, f"{max_day:02d}")

    return station_max



def getSnowAndSnwdTable(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    # Filter for SNOW, SNWD, WESD
    snow_df = df.filter(pl.col("observation_type").is_in(["SNOW", "SNWD", "WESD"]))

    # Create full station ID
    snow_df = snow_df.with_columns([
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("ghcn_id")
    ])

    result = {}

    for row in snow_df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        obs_type = row["observation_type"]

        daily_data = []
        for i in range(1, num_days + 1):
            raw_val = row.get(f"day_{i}", -9999)
            try:
                num_val = float(raw_val)
            except (TypeError, ValueError):
                num_val = -9999

            if num_val in (0, -9999):
                converted_val = num_val
            else:
                if obs_type == "WESD":
                    converted_val = round((num_val / 100.0) / 2.54, 1)
                else:  
                    converted_val = round(num_val / 25.4, 1)

            flag_val = row.get(f"flag_{i}", None)
            daily_data.append((converted_val, flag_val))

        if ghcn_id not in result:
            result[ghcn_id] = {}

        result[ghcn_id][obs_type] = daily_data

    converted_result = {}
    processed_stations = set()

    for row in df.iter_rows(named=True):
        if not all([row.get("country_code"), row.get("network_code"), row.get("station_code")]):
            continue

        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        if ghcn_id in processed_stations:
            continue
        processed_stations.add(ghcn_id)

        converted_result[ghcn_id] = {}
        for obs_type in ["SNOW", "SNWD", "WESD"]:
            if ghcn_id in result and obs_type in result[ghcn_id]:
                daily_list = result[ghcn_id][obs_type]
                converted_values = []
                for val, flag in daily_list:
                    flag = (flag or "").strip()
                    if '7' in flag and 'T' in flag:
                        converted_values.append('T')
                    elif val == 0.0:
                        converted_values.append('')
                    elif '7' in flag:
                        converted_values.append(val)
                    else:
                        converted_values.append('-')
                while len(converted_values) < 31:
                    converted_values.append('')
                converted_result[ghcn_id][obs_type] = converted_values
            else:
                converted_result[ghcn_id][obs_type] = ["MISSING DATA"]

    print("SnowAndSnwdTable:", converted_result)
    return converted_result




def getTemperatureTable(df: pl.DataFrame) -> dict:
    if df.is_empty():
        return {}

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    # Add ghcn_id column
    df = df.with_columns([
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("ghcn_id")
    ])

    result = {}
    ob_time_map = {}

    # Extract OB.TIME from TMAX rows (or TMIN if TMAX missing)
    # Build dict of {ghcn_id: time_of_obs}
    for row in df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        obs_type = row["observation_type"]
        time_of_obs = row.get("time_of_obs", "") or ""

        if obs_type == "TMAX":
            ob_time_map[ghcn_id] = time_of_obs

    # For stations without TMAX time_of_obs, fallback to TMIN time_of_obs if available
    for row in df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        obs_type = row["observation_type"]
        time_of_obs = row.get("time_of_obs", "") or ""

        if ghcn_id not in ob_time_map and obs_type == "TMIN":
            ob_time_map[ghcn_id] = time_of_obs

    # Now build TMAX and TMIN daily arrays
    for row in df.iter_rows(named=True):
        ghcn_id = row["ghcn_id"]
        obs_type = row["observation_type"]
        if obs_type not in ("TMAX", "TMIN"):
            continue

        daily_data = []
        for i in range(1, num_days + 1):
            raw_val = row.get(f"day_{i}", -9999)
            try:
                num_val = float(raw_val)
            except (TypeError, ValueError):
                num_val = -9999

            if num_val in (-9999, 9999):
                converted_val = ''
            else:
                # Convert tenths °C to °F rounded integer
                converted_val = round((num_val / 10.0) * 9 / 5 + 32)

            daily_data.append(converted_val)

        if ghcn_id not in result:
            result[ghcn_id] = {}
        # Assumes only one TMAX and one TMIN row per station
        result[ghcn_id][obs_type] = daily_data

    # Build final dict output
    final_result = {}
    unique_ghcn_ids = []
    seen = set()
    for ghcn_id in df["ghcn_id"]:
        if ghcn_id not in seen:
            unique_ghcn_ids.append(ghcn_id)
            seen.add(ghcn_id)
            
    for ghcn_id in unique_ghcn_ids:
        final_result[ghcn_id] = {
            "OB.TIME": (
                f"{int(ob_time_map[ghcn_id]) // 100:02d}"
                if ghcn_id in ob_time_map and ob_time_map[ghcn_id].isdigit()
                else ""
            ),            
            "TMAX": result.get(ghcn_id, {}).get("TMAX", [""] * num_days),
            "TMIN": result.get(ghcn_id, {}).get("TMIN", [""] * num_days),
        }

    return final_result



def getSoilsData(month: int, year: int) -> pl.DataFrame:
    try:
        soils_data = QuerySoM("soil")
        print("Soil metadata retrieved.")
        print("soils_data", soils_data)

        all_soil_dfs = []
        for row in soils_data:
            coop_id = row[0]
            ghcn_id = row[4]
            file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"

            if not os.path.exists(file_path):
                print(f"Missing soil file: {file_path}")
                continue

            try:
                filtered_data = parse_and_filter(
                    station_code=ghcn_id,
                    file_path=file_path,
                    correction_type="soilTable",
                    month=month,
                    year=year
                )

                filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

                if filtered_df.is_empty():
                    print(f"Skipping soil station {ghcn_id} due to no data.")
                    continue

                all_soil_dfs.append(filtered_df)
                print(f"Parsed {len(filtered_df)} soil records from {ghcn_id}")

            except Exception as e:
                print(f"Error parsing soil station {ghcn_id}: {e}")
                continue

        if not all_soil_dfs:
            print("No valid soil station files found.")
            return pl.DataFrame()

        combined_soil_df = pl.concat(all_soil_dfs, how="vertical")
        return combined_soil_df

    except Exception as e:
        print(f"Error in getSoilsData: {e}")
        return pl.DataFrame()


GROUND_COVER_MAP = {
    "0": "UNKNOWN",
    "1": "GRASS",
    "2": "FALLOW",
    "3": "BARE GROUND",
    "4": "BROME GRASS",
    "5": "SOD",
    "6": "STRAW MULCH",
    "7": "GRASS MUCK",
    "8": "BARE MUCK"
}

DEPTH_CODE_MAP = {
    "1": 5,
    "2": 10,
    "3": 20,
    "4": 50,
    "5": 100,
    "6": 150,
    "7": 180
}

def getSoilTemperatureTable(soils_combined_df: pl.DataFrame) -> list[dict]:
    grouped = defaultdict(list)

    if soils_combined_df.is_empty():
        print("No soil data available.")
        return []

    # Determine year and month from first row
    year = soils_combined_df[0, "year"]
    month = soils_combined_df[0, "month"]
    num_days = monthrange(year, month)[1]

    for row in soils_combined_df.iter_rows(named=True):
        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        obs_type = row["observation_type"]

        # Determine if max or min soil temp
        calc_type = "MAX" if obs_type[:2] == "SX" else "MIN"

        # Ground cover and depth codes from obs_type like SN32
        if len(obs_type) < 4:
            continue
        ground_cover_code = obs_type[2]
        depth_code = obs_type[3]

        # Collect daily values using actual month length
        daily_values = []
        for day in range(1, num_days + 1):
            val = row.get(f"day_{day}")
            if val is None or str(val).strip() == "-9999":
                daily_values.append("-")
                continue
            try:
                celsius = int(val) / 10
                fahrenheit = round((celsius * 9 / 5) + 32)
                daily_values.append(fahrenheit)
            except Exception:
                daily_values.append("-")

        # Pad out to 31 days with ""
        while len(daily_values) < 31:
            daily_values.append("")

        # Skip if all values are missing
        if all(v == "-" for v in daily_values[:num_days]):
            continue

        # Map depth code to cm, then to inches
        cm_val = DEPTH_CODE_MAP.get(depth_code)
        if cm_val is None:
            continue
        in_val = math.ceil(cm_val / 2.54)

        grouped[ghcn_id].append({
            "calc_type": calc_type,
            "ground_cover": GROUND_COVER_MAP.get(ground_cover_code, "unknown"),
            "depth_in": in_val,
            "values": daily_values
        })

    return [{"ghcn_id": ghcn_id, "soil_temperatures": entries} for ghcn_id, entries in grouped.items()]




def get_soil_refernce_notes(rows: List[tuple]) -> List[Dict[str, Any]]:

    report_list = []

    for row in rows:
        station_name = row[2]  # 'DAVIS 2 WSW EXP FARM'
        soil_type = row[5]     # 'LOAM'
        soil_cover = row[6]    # 'BARE GROUND'
        slope = row[8]         # 00
        units = row[9]         # 'F'

        # Convert slope to string, pad with zeros if needed (e.g. '20' -> '20')
        slope_str = str(slope).zfill(2) if slope is not None else ""

        report_list.append({
            "station_name": station_name,
            "soil_type": soil_type,
            "soil_cover": soil_cover,
            "slope": slope_str,
            "units": units
        })

    return report_list




def getWindMovement(wind_df: pl.DataFrame) -> list[dict]:
    
    grouped = defaultdict(list)

    if wind_df.is_empty():
        print("No wind data available.")
        return []

    # Determine year and month from first row
    year = wind_df[0, "year"]
    month = wind_df[0, "month"]
    num_days = monthrange(year, month)[1]
    total_wind = 0
    for row in wind_df.iter_rows(named=True):
        obs_type = row["observation_type"]
        if obs_type != "WDMV":
            continue

        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        daily_values = []

        for day in range(1, num_days + 1):
            val = row.get(f"day_{day}")
            if val is None or str(val).strip() == "-9999":
                daily_values.append("-")
                continue
            try:
                km = float(val)
                miles = round(km * 0.621371)
                daily_values.append(miles)
                total_wind = total_wind + miles
            except Exception:
                daily_values.append("-")

        # Pad to 31 days
        while len(daily_values) < 31:
            daily_values.append("")

        if all(v == "-" for v in daily_values[:num_days]):
            continue

        grouped[ghcn_id].append({
            "WIND": daily_values,
            "total_wind": total_wind
        })

    return [{"ghcn_id": ghcn_id, "wind_data": entries} for ghcn_id, entries in grouped.items()]


def getEvaporation(df: pl.DataFrame) -> list[dict]:
    grouped = defaultdict(list)

    if df.is_empty():
        return []

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    for row in df.iter_rows(named=True):
        if row["observation_type"] != "EVAP":
            continue

        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        daily_values = []
        total_evap = 0.0

        for day in range(1, num_days + 1):
            val = row.get(f"day_{day}")
            if val is None or str(val).strip() == "-9999":
                daily_values.append("-")
                continue
            try:
                mm = int(val) / 10
                hundredths_in = round(mm * 0.0393701, 2)
                daily_values.append(hundredths_in)
                total_evap += hundredths_in
            except Exception:
                daily_values.append("-")

        while len(daily_values) < 31:
            daily_values.append("")

        if all(v == "-" for v in daily_values[:num_days]):
            continue

        grouped[ghcn_id].append({
            "EVAP": daily_values,
            "total_evap": round(total_evap, 2)
        })

    return [{"ghcn_id": ghcn_id, "evap_data": entries} for ghcn_id, entries in grouped.items()]

def getPanMaxTemp(df: pl.DataFrame) -> list[dict]:
    grouped = defaultdict(list)

    if df.is_empty():
        return []

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    for row in df.iter_rows(named=True):
        if row["observation_type"] != "MXPN":
            continue
        print("MXPN found for:", row["country_code"], row["network_code"], row["station_code"])

        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        daily_values = []
        total_max = 0
        count = 0

        for day in range(1, num_days + 1):
            val = row.get(f"day_{day}")
            flag = row.get(f"flag_{day}", "").strip()
            if val is None or str(val).strip() == "-9999":
                daily_values.append("-")
                continue
            try:
                celsius = int(val) * 0.1
                fahrenheit = round((celsius * 9 / 5) + 32)
                suffix = "".join(c for c in flag if c.isalpha())
                value = f"{fahrenheit}{suffix}" if suffix else fahrenheit
                daily_values.append(value)
                total_max += fahrenheit
                count += 1
            except Exception:
                daily_values.append("-")

        while len(daily_values) < 31:
            daily_values.append("")

        if all(v == "-" for v in daily_values[:num_days]):
            continue

        avg_max = round(total_max / count, 1) if count > 0 else "-"
        grouped[ghcn_id].append({
            "MAX": daily_values,
            "avg_max": avg_max
        })

    return [{"ghcn_id": ghcn_id, "pan_max_data": entries} for ghcn_id, entries in grouped.items()]



def getPanMinTemp(df: pl.DataFrame) -> list[dict]:
    grouped = defaultdict(list)

    if df.is_empty():
        return []

    year = df[0, "year"]
    month = df[0, "month"]
    num_days = monthrange(year, month)[1]

    for row in df.iter_rows(named=True):
        if row["observation_type"] != "MNPN":
            continue
        print("MNPN found for:", row["country_code"], row["network_code"], row["station_code"])

        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        daily_values = []
        total_min = 0
        count = 0

        for day in range(1, num_days + 1):
            val = row.get(f"day_{day}")
            flag = row.get(f"flag_{day}", "").strip()
            if val is None or str(val).strip() == "-9999":
                daily_values.append("-")
                continue
            try:
                celsius = int(val) * 0.1
                fahrenheit = round((celsius * 9 / 5) + 32)
                suffix = "".join(c for c in flag if c.isalpha())
                value = f"{fahrenheit}{suffix}" if suffix else fahrenheit
                daily_values.append(value)
                total_min += fahrenheit
                count += 1
            except Exception:
                daily_values.append("-")

        while len(daily_values) < 31:
            daily_values.append("")

        if all(v == "-" for v in daily_values[:num_days]):
            continue

        avg_min = round(total_min / count, 1) if count > 0 else "-"
        grouped[ghcn_id].append({
            "MIN": daily_values,
            "avg_min": avg_min
        })

    return [{"ghcn_id": ghcn_id, "pan_min_data": entries} for ghcn_id, entries in grouped.items()]




# def getPanEvapTable(df: pl.DataFrame) -> list[dict]:
        
#     wind = getWindMovement(df)
#     evap = getEvaporation(df)
#     pan_max = getPanMaxTemp(df)
#     pan_min = getPanMinTemp(df)
    
#     # dump json to file
#     output_file = f"TESTwindDATA.json"
#     with open(output_file, "w") as f:
#         f.write(json.dumps(wind, indent=2))
        
#         # dump json to file
#     output_file = f"TESTevapDATA.json"
#     with open(output_file, "w") as f:
#         f.write(json.dumps(evap, indent=2))
        
#         # dump json to file
#     output_file = f"TESTpan_maxDATA.json"
#     with open(output_file, "w") as f:
#         f.write(json.dumps(pan_max, indent=2))
        
#         # dump json to file
#     output_file = f"TESTpan_minDATA.json"
#     with open(output_file, "w") as f:
#         f.write(json.dumps(pan_min, indent=2))


#     by_id = defaultdict(list)

       
#     for group in wind:
#         by_id[group["ghcn_id"]].extend(group.get("wind_data", []))
#     for group in evap:
#         by_id[group["ghcn_id"]].extend(group.get("evap_data", []))
#     for group in pan_max:
#         by_id[group["ghcn_id"]].extend(group.get("pan_max_data", []))
#     for group in pan_min:
#         by_id[group["ghcn_id"]].extend(group.get("pan_min_data", []))
        

#     return [{"ghcn_id": ghcn_id, "pan_evap_data": entries} for ghcn_id, entries in by_id.items()]


def getPanEvapTable(df: pl.DataFrame) -> list[dict]:
    wind = getWindMovement(df)
    evap = getEvaporation(df)
    pan_max = getPanMaxTemp(df)
    pan_min = getPanMinTemp(df)

    # Build lookup dicts keyed by ghcn_id for fast access
    wind_lookup = {g["ghcn_id"]: g.get("wind_data", []) for g in wind}
    evap_lookup = {g["ghcn_id"]: g.get("evap_data", []) for g in evap}
    pan_max_lookup = {g["ghcn_id"]: g.get("pan_max_data", []) for g in pan_max}
    pan_min_lookup = {g["ghcn_id"]: g.get("pan_min_data", []) for g in pan_min}

    # Extract station order exactly as in original df
    ordered_ids = []
    seen = set()
    for row in df.iter_rows(named=True):
        ghcn_id = row["country_code"] + row["network_code"] + row["station_code"]
        if ghcn_id not in seen:
            ordered_ids.append(ghcn_id)
            seen.add(ghcn_id)

    combined_list = []
    for ghcn_id in ordered_ids:
        combined_data = []
        combined_data.extend(wind_lookup.get(ghcn_id, []))
        combined_data.extend(evap_lookup.get(ghcn_id, []))
        combined_data.extend(pan_max_lookup.get(ghcn_id, []))
        combined_data.extend(pan_min_lookup.get(ghcn_id, []))

        combined_list.append({
            "ghcn_id": ghcn_id,
            "pan_evap_data": combined_data
        })

    return combined_list


def merge_SOM_data(
    high_temp: dict,
    low_temp: dict,
    avg_data: dict,
    snow_data: dict,
    depth_data: dict
) -> dict:
    merged = {}
    all_ids = list(dict.fromkeys(
        list(high_temp) + list(low_temp) + list(avg_data) + list(snow_data) + list(depth_data)
    ))
    for ghcn_id in all_ids:
        entry = {}

        if ghcn_id in high_temp:
            entry["Highest_Temp"] = high_temp[ghcn_id]
        if ghcn_id in low_temp:
            entry["Lowest_Temp"] = low_temp[ghcn_id]
        if ghcn_id in avg_data:
            entry.update(avg_data[ghcn_id])  # <- unpack directly here
        if ghcn_id in snow_data:
            entry["Total_SnowIce"] = snow_data[ghcn_id]
        if ghcn_id in depth_data:
            val = depth_data[ghcn_id]
            if isinstance(val, tuple):
                entry["Max_Depth_On_Ground"] = {"value": val[0], "day": val[1]}
            else:
                entry["Max_Depth_On_Ground"] = {"value": val, "day": ""}

        merged[ghcn_id] = entry

    return merged



def build_combined_df(station_rows, tobs_lookup, month, year):
    """
    Build a combined Polars DataFrame from station rows,
    parsing and filtering each station's .dly file.
    Returns the combined DataFrame.
    """

    all_filtered_dfs = []
    noDataCount = 0

    for row in station_rows[:10]:  # Keep limiting to first 10 for debug/safety
        print("Station row: ", row)
        coop_id = row[0]
        ghcn_id = row[4]
        file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"

        # # TEMP: Skip all but the target station
        # if ghcn_id != target_ghcn_id:
        #     continue

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

            # Add TOBS metadata if available
            if coop_id in tobs_lookup:
                meta_fields = [
                    "data_program", "element", "time_of_obs", "equipment", "shield_flag",
                    "roof_flag", "j_flag", "r_flag", "c_flag", "g_flag"
                ]
                metadata = dict(zip(meta_fields, tobs_lookup[coop_id]))
                for key, val in metadata.items():
                    filtered_df = filtered_df.with_columns(pl.lit(val).cast(pl.String).alias(key))
            else:
                print(f"No TOBS metadata for {coop_id}")

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
        return pl.DataFrame([])  # Return empty DataFrame to avoid crashes

    combined_df = pl.concat(all_filtered_dfs, how="vertical")
    return combined_df

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)

    # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}_flask.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)
    
    
    
    # print(f"Data saved to {output_file}")
    # print(json_data)


    ########################################
    #  Read the JSON file for Testing

    # json_data = None

    # with open(output_file) as f:
    #     json_data = json.load(f)
    #     # print(d)

    #########################################
    
    json_data = json.loads(json_data)
    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"]
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
            

    # for key, data in prcp_data.items():
    #     print(f"{key}:\t{data}")
    # print(f"-"*30)
    # for key, data in mdpr_data.items():
    #     print(f"{key}:\t{data}")
    # print(f"-"*30)
    # for key, data in dapr_data.items():
    #     print(f"{key}:\t{data}")


    ############################################


    # Update the PRCP dictionary with stations that have no PRCP data (but have MDPR and DAPR)
    


    for key in set(mdpr_data) | set(dapr_data) | set(full_station_id_list):  
        if key not in prcp_data:
            prcp_data[key] = None

    
    # Sort the prcp data in the same order as the station list from the DB.
    prcp_data = {key: prcp_data[key] for key in full_station_id_list if key in prcp_data}


    
# Daily Calculations

    daily_precip_table_rec = {}
    for station, data in prcp_data.items():
        print(station)

        pcnrec = [""] * 33 # Station Name, Precip Total, pcn record, pcn record, etc. 
    
        idy = 1     # Integer for current day
        inullct = 0 # Integer for null number of days??
        idyct = 0   # ???
        ptrace = False
        pcnFlagged = False
        total_pcn = 0
        pcn_count = 0 # each valid and non Q flagged PRCP data day
        ieommd = 0 # ???
        pcn_acc = False # Accumulated Precip flag
        pcn_missing = False
        
        pcnrec[0] = station

        for i in range(31):             
            if data is not None:
                try:    
                    pcn = data[i][0]
                    flg = data[i][1][:1]
                    qflg = data[i][1][1:2]
                    
                    # print(f"pcn:{pcn}  flg:{flg}  qflg:{qflg}")
                except IndexError as err:
                    # Handling months without 31 days
                    pcnrec[idy+1] = "  "
                    idy+=1
                    continue

                if pcn != -9999:
                    if qflg == " ":
                        d = float(pcn) * 0.1
                        d = round_it(get_mm_to_in(d),2)
                        pcnrec[idy+1] = d
                        
                        if pcnrec[idy+1] == '0.00':
                            pcnrec[idy+1] = " "

                        total_pcn += float(d) 
                        
                        total_pcn = float(round_it(total_pcn, 2)) #Is this rounding required? 

                        pcn_count += 1
                        ieommd = 0

                        if flg == "T":
                            ptrace = True


                    else:
                        d = float(pcn) * 0.1
                        d = get_mm_to_in(d)
                        pcnrec[idy+1] = round_it(d,2)
                        pcnFlagged = True

                    idyct += 1

                    if flg == "T":
                        pcnrec[idy+1] = "T  "

                    
                    # print(f"Line: pcnrec[idy+1]:{pcnrec[idy+1]} \ttotal_pcn:{total_pcn} \tpcn_count:{pcn_count} \tieommd:{ieommd}\tptrace:{ptrace} pcnFlagged:{pcnFlagged}")
                
                
                else:  # pcn == -9999
                    try:
                        if mdpr_data[station] is not None: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            pcn = mdpr_data[station][i][0]
                            flg = mdpr_data[station][i][1][:1]
                            qflg = mdpr_data[station][i][1][1:2]
                            ndays = 0


                            # print(f"MDPR: pcn {pcn}  flg {flg}  qflg {qflg}")

                            if pcn != -9999:
                                if qflg == " ":
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                    #  DAPR
                                    try:
                                        if dapr_data[station]:
                                            days = dapr_data[station][i][0]

                                            if days != -9999:
                                                ix = i+1 # Index for this calculation
                                                for i2 in range(1, days):
                                                    try:
                                                        pcnrec[ix] = "* "
                                                        ix -= 1
                                                        idyct += 1
                                                    except (ValueError, IndexError):
                                                        print("Too many days")

                                    except KeyError as err:
                                        pass 
                                
                                else:
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                if flg == "T":
                                    pcnrec[idy+1] = "Ta"
                            else:
                                pcnrec[idy+1] = "-  "

                            try:
                                if dapr_data[station]is not None:
                                    try:
                                        ndays = dapr_data[station][i][0]
                                        # print(f"ndays: {ndays}")
                                        if ndays != -9999:
                                            if i >= ndays - 1:
                                                pcn_count += ndays
                                                pcn_acc = True
                                            else:
                                                pcn_count = i + 1
                                                pcn_acc = True

                                            if pcn != -9999:
                                                if qflg == " ": 
                                                    d = float(pcn) * 0.1
                                                    d = get_mm_to_in(d)
                                                    d = float(round_it(d, 2))

                                                    total_pcn += d
                                                    ieommd = 0
                                                else:
                                                    pcnFlagged = True
                                            else:
                                                ieommd += 1
                                                pcn_missing = True    

                                        else:
                                            # print(f"ndays == -9999")
                                            ndays = 0
                                            pcn_missing = True
                                            ieommd += 1

                                    except ValueError as err:
                                        print("error: {}".format(traceback.format_exc()))
                                        pass
                                else:
                                    raise KeyError(f"{station} has DAPR key but no data.")

                            except KeyError as err:
                                # print("error: {}".format(traceback.format_exc()))
                                print(f"NOTE: No DAPR data")
                                pcn_missing = True
                                ieommd += 1
                        else:
                            raise KeyError(f"{station} has MDPR key but no data.")

                            
                    except KeyError as err:
                        # print("error: {}".format(traceback.format_exc()))
                        # print(f"No MDPR Data")
                        pcnrec[idy+1] = "-  "
                        pcn_missing = True
                        ieommd += 1

                # print(f"Line2: total_pcn: {total_pcn} pcn_count: {pcn_count} pcn_acc: {pcn_acc} pcn_missing:{pcn_missing} ieommd:{ieommd}")
            else:
                if i < num_days:
                    inullct += 1
                    pcnrec[idy+1] = "-  "

            
                    try:
                        if mdpr_data[station]is not None: # MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            try:
                                pcn = mdpr_data[station][i][0]
                                qflg = mdpr_data[station][i][1][1:2]
                            except IndexError as err: # Handling months with less than 31 days.
                                print("error: {}".format(traceback.format_exc()))
                                pcn = None 
                                qflg = None                             
                            try:
                                ndays = dapr_data[station][i][0] if dapr_data[station] is not None else 0
                            except KeyError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = 0
                            except IndexError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = None

                            # Index is past the number of days for a month.
                            if (pcn or qflg or ndays) is None: 
                                print(f"Skipping because of index {i}")
                                continue
                            

                            if pcn != -9999:
                                if qflg == " ":

                                    pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                    total_pcn = float(round_it(total_pcn +  pcn, 2))

                                    ndays = int(ndays)
                                    if ndays < i:
                                        pcn_count += ndays if ndays != -9999 else 0
                                    else:
                                        pcn_count = i
                                        pcn_acc = True

                                    if pcn != -9999:
                                        if qflg == " ":
                                            pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                            total_pcn = float(round_it(total_pcn +  pcn, 2))
                                            ieommd = 0
                                    else: # This clause is unreachable.
                                        ieommd += 1
                                        pcn_missing = True
                                else:
                                    pcnFlagged = True
                                    # ieommd += 1 # I feel like this should be here, but it isn't. 
                            else:
                                pcn_missing = True

                            # print(f"NOTE: Else.if mdpr_data[station].inches: pcn {pcn} qflg {qflg} ndays {ndays} total_pcn {total_pcn} " 
                            #         + f"\npcn_count {pcn_count} pcn_acc {pcn_acc} pcnFlagged {pcnFlagged} pcn_missing {pcn_missing} ieommd {ieommd} iteration {i}")
                        
                        else:
                            pcn_missing = True
                                
                    except KeyError as err:
                        print("error: {}".format(traceback.format_exc()))
                        pcn_missing = True





            
            idy+=1   


        
        # Add Flags to the Total Precip Calculation
        day_diff = monthrange(year, month)[1] - pcn_count

        if day_diff == 0:
            pcn_missing = False
        
        setAstr = False # Set Asterisk
        still_missing = True

        if day_diff <= 9:
            flag_total_pcn = round_it(total_pcn, 2)

            if day_diff == ieommd and ieommd > 0:
                still_missing = check_next_month_for_acc_pcn(station, month, year, ieommd)
                if not still_missing:
                    pcn_missing = False
                    setAstr = True

            # print(f"NOTE: flag_total_pcn={flag_total_pcn} ptrace={ptrace} day_diff={day_diff} pcn_missing={pcn_missing} setAstr={setAstr} still_missing={still_missing}" )

            if ptrace and flag_total_pcn == "0.00":
                flag_total_pcn = "T"

            label = ""


            #######
            # FOR TESTING FLAG LOGIC
            # label = True
            # setAstr = True

            #####
            if pcn_acc:
                if pcn_missing:
                    label = "FMA" if pcnFlagged else "MA"
                else:
                    label = "FA" if pcnFlagged else "A"
            else:
                if pcn_missing:
                    label = "FM" if pcnFlagged else "M"
                else:
                    label = "F" if pcnFlagged else ""

            if label:
                if setAstr:
                    flag_total_pcn = f"{label}* {flag_total_pcn}"
                else:
                    flag_total_pcn = f"{label} {flag_total_pcn}"
            elif setAstr:
                flag_total_pcn = f"* {flag_total_pcn}"
        

def get_mm_to_in(mm: float) -> float:
    """Convert millimeters to inches."""
    return mm * 0.03937 # 1 inch = 25.4 mm


def generateDailyPrecip(month:int = 9, year:int = 2020) -> dict:
    """ Generates Daily Precipitation and Total Precipitation for a state.

    Parameters
    ----------
    month : int, optional
        by default 9
    year : int, optional
        by default 2020

    Returns
    ---------
    daily_precip_table_rec : dict[dict]
        Records with their GHCN-ID as the key. Contains total_pcn, daily_pcn

    Notes
    ---------

    This should eventually be updated to recieve the month year (and possibly state) codes so they aren't hardcoded. 
    Change to take json_data as input? 

    """

    
    # Get Station List for Precipitation Query
    db_stations = QueryDB(DailyPrecipQuery)
    # print(bool(db_stations))
    # for station in db_stations:
    #     print(station)

    all_filtered_dfs = []
    noDataCount = 0
    full_station_id_list = []

    for row in db_stations[:10]:
    # for row in db_stations:
        ghcn_id = row[4]
        file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
        full_station_id_list.append(ghcn_id)

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
        # return?
    
        
        station_name = None
        if station in load_station_data():
            station_name = load_station_data()[station][1]

        
        # print(f"station: {station} {station_name}: {total_pcn} flag_total_pcn={flag_total_pcn}", "tprcp_output.txt")

        pcnrec[1] = flag_total_pcn

    
        # print(
        #     f"station={str(station):<13}  {str(station_name):<40}"
        #     f"  flag_total_pcn={str(flag_total_pcn):<5}"
        #     f"  total_pcn={str(total_pcn):<5}"
        #     f"  idy={str(idy):<3}"
        #     f"  inullct={str(inullct):<2}"
        #     f"  idyct={str(idyct):<3}"
        #     f"  ptrace={str(ptrace):<5}"
        #     f"  pcnFlagged={str(pcnFlagged):<5}"
        #     f"  pcn_count={str(pcn_count):<2}"
        #     f"  ieommd={str(ieommd):<1}"
        #     f"  pcn_acc={str(pcn_acc):<5}"
        #     f"  pcn_missing={str(pcn_missing):<5}"
        #     f"\n{str(station_name)} {str(pcnrec)}"
        # )

        ##############################
        # Printing the results for each station to a file to QA them. 
        # result = {}
        # result['station_id'] = pcnrec[0]
        # result['tprcp'] = pcnrec[1].strip()

        # for i, value in enumerate(pcnrec[2:], start=1):
        #     label = f"{i:02d}"
        #     result[label] = value.strip()

        # print(
        #      f"{str(station_name)} {str(result)}\n"
        #      , "tprcp_output.txt"
        # )
        ############################
        
        # End result format
        result = {}
        result['total_pcn'] = pcnrec[1].strip()

        daily_pcn = {}
        for i, value in enumerate(pcnrec[2:], start=1):
            label = f"{i:02d}"
            daily_pcn[label] = value.strip()

        result["daily_pcn"] = daily_pcn

        daily_precip_table_rec.setdefault(station, {}).update(result)



    return daily_precip_table_rec




def check_next_month_for_acc_pcn(station_id: str, month: int,year: int, ieommd: int) -> bool:
    """ Checks the next month for accumulated precipitation.

    Parameters
    ----------
    station_id : str
        GHCN-ID
    month, year : int

    ieommd : int
        Honestly I don't know what this stands for. I tried. 

    Returns
    -------
    bool
    """
    all_filtered_dfs = []
    noDataCount = 0

    still_missing = True

    # Parse month and increment
    month = month
    year = year

    if month < 12:
        month += 1
    else:
        month = 1
        year += 1

    ######################
    # Get next month's data

    file_path = f"/data/ops/ghcnd/data/ghcnd_all/{station_id}.dly"

    if not os.path.exists(file_path):
        print(f"Missing file: {file_path}")
        return still_missing

    try:
        filtered_data = parse_and_filter(
            station_code=station_id,
            file_path=file_path,
            correction_type="table",
            month=month,
            year=year
        )

        filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

        if filtered_df.is_empty():
            print(f"Skipping station {station_id} due to no data.")
            noDataCount += 1
            return still_missing

        if all_filtered_dfs:
            existing_columns = all_filtered_dfs[0].columns
            current_columns = filtered_df.columns

            missing_columns = set(existing_columns) - set(current_columns)
            for col in missing_columns:
                filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

            filtered_df = filtered_df.select(existing_columns)

        all_filtered_dfs.append(filtered_df)
        print(f"Parsed {len(filtered_df)} records from {station_id}")

    except Exception as e:
        print(f"Error parsing {station_id}: {e}")
        return still_missing

    if not all_filtered_dfs:
        print("No valid station files found.")
        return still_missing

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)
    json_data = json.loads(json_data)

    # # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)

    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        # List Comprehesion
        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"] # if row[f"day_{i}"] is not None else None
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))



    for i in range(32):

        try:
            try:
                pcn = prcp_data[station_id][i][0] if prcp_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn = None
            
            try:
                pcn_mdpr = mdpr_data[station_id][i][0] if mdpr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn_mdpr = -9999
            
            try:
                days = dapr_data[station_id][i][0] if dapr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                days = None
            

        except IndexError as err:
            print(f"Breaking from next month acc loop.")
            break # Exit early if data is shorter than expected
    
        # print(f"pcn={pcn} pcn_mdpr={pcn_mdpr} days={days}")

        if pcn_mdpr != -9999 and days != -9999:
            try:
                days = int(days)
                day_diff = days - (i + 1)
                if day_diff == ieommd:
                    still_missing = False
                
            except TypeError:
                pass   
            break # Exit loop if accumulated value found

        if pcn != -9999:
            break # Exit loop if direct value found

    return still_missing

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)

    # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}_flask.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)
    
    
    
    # print(f"Data saved to {output_file}")
    # print(json_data)


    ########################################
    #  Read the JSON file for Testing

    # json_data = None

    # with open(output_file) as f:
    #     json_data = json.load(f)
    #     # print(d)

    #########################################
    
    json_data = json.loads(json_data)
    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"]
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
            

    # for key, data in prcp_data.items():
    #     print(f"{key}:\t{data}")
    # print(f"-"*30)
    # for key, data in mdpr_data.items():
    #     print(f"{key}:\t{data}")
    # print(f"-"*30)
    # for key, data in dapr_data.items():
    #     print(f"{key}:\t{data}")


    ############################################


    # Update the PRCP dictionary with stations that have no PRCP data (but have MDPR and DAPR)
    


    for key in set(mdpr_data) | set(dapr_data) | set(full_station_id_list):  
        if key not in prcp_data:
            prcp_data[key] = None

    
    # Sort the prcp data in the same order as the station list from the DB.
    prcp_data = {key: prcp_data[key] for key in full_station_id_list if key in prcp_data}


    
# Daily Calculations

    daily_precip_table_rec = {}
    for station, data in prcp_data.items():
        print(station)

        pcnrec = [""] * 33 # Station Name, Precip Total, pcn record, pcn record, etc. 
    
        idy = 1     # Integer for current day
        inullct = 0 # Integer for null number of days??
        idyct = 0   # ???
        ptrace = False
        pcnFlagged = False
        total_pcn = 0
        pcn_count = 0 # each valid and non Q flagged PRCP data day
        ieommd = 0 # ???
        pcn_acc = False # Accumulated Precip flag
        pcn_missing = False
        
        pcnrec[0] = station

        for i in range(31):             
            if data is not None:
                try:    
                    pcn = data[i][0]
                    flg = data[i][1][:1]
                    qflg = data[i][1][1:2]
                    
                    # print(f"pcn:{pcn}  flg:{flg}  qflg:{qflg}")
                except IndexError as err:
                    # Handling months without 31 days
                    pcnrec[idy+1] = "  "
                    idy+=1
                    continue

                if pcn != -9999:
                    if qflg == " ":
                        d = float(pcn) * 0.1
                        d = round_it(get_mm_to_in(d),2)
                        pcnrec[idy+1] = d
                        
                        if pcnrec[idy+1] == '0.00':
                            pcnrec[idy+1] = " "

                        total_pcn += float(d) 
                        
                        total_pcn = float(round_it(total_pcn, 2)) #Is this rounding required? 

                        pcn_count += 1
                        ieommd = 0

                        if flg == "T":
                            ptrace = True


                    else:
                        d = float(pcn) * 0.1
                        d = get_mm_to_in(d)
                        pcnrec[idy+1] = round_it(d,2)
                        pcnFlagged = True

                    idyct += 1

                    if flg == "T":
                        pcnrec[idy+1] = "T  "

                    
                    # print(f"Line: pcnrec[idy+1]:{pcnrec[idy+1]} \ttotal_pcn:{total_pcn} \tpcn_count:{pcn_count} \tieommd:{ieommd}\tptrace:{ptrace} pcnFlagged:{pcnFlagged}")
                
                
                else:  # pcn == -9999
                    try:
                        if mdpr_data[station] is not None: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            pcn = mdpr_data[station][i][0]
                            flg = mdpr_data[station][i][1][:1]
                            qflg = mdpr_data[station][i][1][1:2]
                            ndays = 0


                            # print(f"MDPR: pcn {pcn}  flg {flg}  qflg {qflg}")

                            if pcn != -9999:
                                if qflg == " ":
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                    #  DAPR
                                    try:
                                        if dapr_data[station]:
                                            days = dapr_data[station][i][0]

                                            if days != -9999:
                                                ix = i+1 # Index for this calculation
                                                for i2 in range(1, days):
                                                    try:
                                                        pcnrec[ix] = "* "
                                                        ix -= 1
                                                        idyct += 1
                                                    except (ValueError, IndexError):
                                                        print("Too many days")

                                    except KeyError as err:
                                        pass 
                                
                                else:
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                if flg == "T":
                                    pcnrec[idy+1] = "Ta"
                            else:
                                pcnrec[idy+1] = "-  "

                            try:
                                if dapr_data[station]is not None:
                                    try:
                                        ndays = dapr_data[station][i][0]
                                        # print(f"ndays: {ndays}")
                                        if ndays != -9999:
                                            if i >= ndays - 1:
                                                pcn_count += ndays
                                                pcn_acc = True
                                            else:
                                                pcn_count = i + 1
                                                pcn_acc = True

                                            if pcn != -9999:
                                                if qflg == " ": 
                                                    d = float(pcn) * 0.1
                                                    d = get_mm_to_in(d)
                                                    d = float(round_it(d, 2))

                                                    total_pcn += d
                                                    ieommd = 0
                                                else:
                                                    pcnFlagged = True
                                            else:
                                                ieommd += 1
                                                pcn_missing = True    

                                        else:
                                            # print(f"ndays == -9999")
                                            ndays = 0
                                            pcn_missing = True
                                            ieommd += 1

                                    except ValueError as err:
                                        print("error: {}".format(traceback.format_exc()))
                                        pass
                                else:
                                    raise KeyError(f"{station} has DAPR key but no data.")

                            except KeyError as err:
                                # print("error: {}".format(traceback.format_exc()))
                                print(f"NOTE: No DAPR data")
                                pcn_missing = True
                                ieommd += 1
                        else:
                            raise KeyError(f"{station} has MDPR key but no data.")

                            
                    except KeyError as err:
                        # print("error: {}".format(traceback.format_exc()))
                        # print(f"No MDPR Data")
                        pcnrec[idy+1] = "-  "
                        pcn_missing = True
                        ieommd += 1

                # print(f"Line2: total_pcn: {total_pcn} pcn_count: {pcn_count} pcn_acc: {pcn_acc} pcn_missing:{pcn_missing} ieommd:{ieommd}")
            else:
                if i < num_days:
                    inullct += 1
                    pcnrec[idy+1] = "-  "

            
                    try:
                        if mdpr_data[station]is not None: # MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            try:
                                pcn = mdpr_data[station][i][0]
                                qflg = mdpr_data[station][i][1][1:2]
                            except IndexError as err: # Handling months with less than 31 days.
                                print("error: {}".format(traceback.format_exc()))
                                pcn = None 
                                qflg = None                             
                            try:
                                ndays = dapr_data[station][i][0] if dapr_data[station] is not None else 0
                            except KeyError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = 0
                            except IndexError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = None

                            # Index is past the number of days for a month.
                            if (pcn or qflg or ndays) is None: 
                                print(f"Skipping because of index {i}")
                                continue
                            

                            if pcn != -9999:
                                if qflg == " ":

                                    pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                    total_pcn = float(round_it(total_pcn +  pcn, 2))

                                    ndays = int(ndays)
                                    if ndays < i:
                                        pcn_count += ndays if ndays != -9999 else 0
                                    else:
                                        pcn_count = i
                                        pcn_acc = True

                                    if pcn != -9999:
                                        if qflg == " ":
                                            pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                            total_pcn = float(round_it(total_pcn +  pcn, 2))
                                            ieommd = 0
                                    else: # This clause is unreachable.
                                        ieommd += 1
                                        pcn_missing = True
                                else:
                                    pcnFlagged = True
                                    # ieommd += 1 # I feel like this should be here, but it isn't. 
                            else:
                                pcn_missing = True

                            # print(f"NOTE: Else.if mdpr_data[station].inches: pcn {pcn} qflg {qflg} ndays {ndays} total_pcn {total_pcn} " 
                            #         + f"\npcn_count {pcn_count} pcn_acc {pcn_acc} pcnFlagged {pcnFlagged} pcn_missing {pcn_missing} ieommd {ieommd} iteration {i}")
                        
                        else:
                            pcn_missing = True
                                
                    except KeyError as err:
                        print("error: {}".format(traceback.format_exc()))
                        pcn_missing = True





            
            idy+=1   


        
        # Add Flags to the Total Precip Calculation
        day_diff = monthrange(year, month)[1] - pcn_count

        if day_diff == 0:
            pcn_missing = False
        
        setAstr = False # Set Asterisk
        still_missing = True

        if day_diff <= 9:
            flag_total_pcn = round_it(total_pcn, 2)

            if day_diff == ieommd and ieommd > 0:
                still_missing = check_next_month_for_acc_pcn(station, month, year, ieommd)
                if not still_missing:
                    pcn_missing = False
                    setAstr = True

            # print(f"NOTE: flag_total_pcn={flag_total_pcn} ptrace={ptrace} day_diff={day_diff} pcn_missing={pcn_missing} setAstr={setAstr} still_missing={still_missing}" )

            if ptrace and flag_total_pcn == "0.00":
                flag_total_pcn = "T"

            label = ""


            #######
            # FOR TESTING FLAG LOGIC
            # label = True
            # setAstr = True

            #####
            if pcn_acc:
                if pcn_missing:
                    label = "FMA" if pcnFlagged else "MA"
                else:
                    label = "FA" if pcnFlagged else "A"
            else:
                if pcn_missing:
                    label = "FM" if pcnFlagged else "M"
                else:
                    label = "F" if pcnFlagged else ""

            if label:
                if setAstr:
                    flag_total_pcn = f"{label}* {flag_total_pcn}"
                else:
                    flag_total_pcn = f"{label} {flag_total_pcn}"
            elif setAstr:
                flag_total_pcn = f"* {flag_total_pcn}"
        
        else:
            flag_total_pcn = "M"


        # print(f"pcn_acc={pcn_acc} pcn_missing={pcn_missing} label={label} setAstr={setAstr}")
                    
        # print(f"NOTE: flag_total_pcn={flag_total_pcn} ptrace={ptrace} day_diff={day_diff} pcn_missing={pcn_missing} setAstr={setAstr} still_missing={still_missing}" )
        
        station_name = None
        if station in load_station_data():
            station_name = load_station_data()[station][1]

        
        # print(f"station: {station} {station_name}: {total_pcn} flag_total_pcn={flag_total_pcn}", "tprcp_output.txt")

        pcnrec[1] = flag_total_pcn

    
        # print(
        #     f"station={str(station):<13}  {str(station_name):<40}"
        #     f"  flag_total_pcn={str(flag_total_pcn):<5}"
        #     f"  total_pcn={str(total_pcn):<5}"
        #     f"  idy={str(idy):<3}"
        #     f"  inullct={str(inullct):<2}"
        #     f"  idyct={str(idyct):<3}"
        #     f"  ptrace={str(ptrace):<5}"
        #     f"  pcnFlagged={str(pcnFlagged):<5}"
        #     f"  pcn_count={str(pcn_count):<2}"
        #     f"  ieommd={str(ieommd):<1}"
        #     f"  pcn_acc={str(pcn_acc):<5}"
        #     f"  pcn_missing={str(pcn_missing):<5}"
        #     f"\n{str(station_name)} {str(pcnrec)}"
        # )

        ##############################
        # Printing the results for each station to a file to QA them. 
        # result = {}
        # result['station_id'] = pcnrec[0]
        # result['tprcp'] = pcnrec[1].strip()

        # for i, value in enumerate(pcnrec[2:], start=1):
        #     label = f"{i:02d}"
        #     result[label] = value.strip()

        # print(
        #      f"{str(station_name)} {str(result)}\n"
        #      , "tprcp_output.txt"
        # )
        ############################
        
        # End result format
        result = {}
        result['total_pcn'] = pcnrec[1].strip()

        daily_pcn = {}
        for i, value in enumerate(pcnrec[2:], start=1):
            label = f"{i:02d}"
            daily_pcn[label] = value.strip()

        result["daily_pcn"] = daily_pcn

        daily_precip_table_rec.setdefault(station, {}).update(result)



    return daily_precip_table_rec




def check_next_month_for_acc_pcn(station_id: str, month: int,year: int, ieommd: int) -> bool:
    """ Checks the next month for accumulated precipitation.

    Parameters
    ----------
    station_id : str
        GHCN-ID
    month, year : int

    ieommd : int
        Honestly I don't know what this stands for. I tried. 

    Returns
    -------
    bool
    """
    all_filtered_dfs = []
    noDataCount = 0

    still_missing = True

    # Parse month and increment
    month = month
    year = year

    if month < 12:
        month += 1
    else:
        month = 1
        year += 1

    ######################
    # Get next month's data

    file_path = f"/data/ops/ghcnd/data/ghcnd_all/{station_id}.dly"

    if not os.path.exists(file_path):
        print(f"Missing file: {file_path}")
        return still_missing

    try:
        filtered_data = parse_and_filter(
            station_code=station_id,
            file_path=file_path,
            correction_type="table",
            month=month,
            year=year
        )

        filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

        if filtered_df.is_empty():
            print(f"Skipping station {station_id} due to no data.")
            noDataCount += 1
            return still_missing

        if all_filtered_dfs:
            existing_columns = all_filtered_dfs[0].columns
            current_columns = filtered_df.columns

            missing_columns = set(existing_columns) - set(current_columns)
            for col in missing_columns:
                filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

            filtered_df = filtered_df.select(existing_columns)

        all_filtered_dfs.append(filtered_df)
        print(f"Parsed {len(filtered_df)} records from {station_id}")

    except Exception as e:
        print(f"Error parsing {station_id}: {e}")
        return still_missing

    if not all_filtered_dfs:
        print("No valid station files found.")
        return still_missing

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)
    json_data = json.loads(json_data)

    # # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)

    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        # List Comprehesion
        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"] # if row[f"day_{i}"] is not None else None
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))



    for i in range(32):

        try:
            try:
                pcn = prcp_data[station_id][i][0] if prcp_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn = None
            
            try:
                pcn_mdpr = mdpr_data[station_id][i][0] if mdpr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn_mdpr = -9999
            
            try:
                days = dapr_data[station_id][i][0] if dapr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                days = None
            

        except IndexError as err:
            print(f"Breaking from next month acc loop.")
            break # Exit early if data is shorter than expected
    
        # print(f"pcn={pcn} pcn_mdpr={pcn_mdpr} days={days}")

        if pcn_mdpr != -9999 and days != -9999:
            try:
                days = int(days)
                day_diff = days - (i + 1)
                if day_diff == ieommd:
                    still_missing = False
                
            except TypeError:
                pass   
            break # Exit loop if accumulated value found

        if pcn != -9999:
            break # Exit loop if direct value found

    return still_missing


def generateMonthlyPub():
    month = 2
    year = 2023
    # target_ghcn_id = "USC00049026"

    try:
        stations = QuerySoM("som")
        print("Station list retrieved.", stations)

        temperature_data = QuerySoM("temp")
        print("Temp data retrieved.", temperature_data)
        
        evaporation_data = QuerySoM("evap")
        print("Evap data retrieved.", evaporation_data)
        
         
        precipitation_data = QuerySoM("precip")
        print("Precipitation data retrieved.", precipitation_data)

        tobs_data = QuerySoM("tobs")
        # print("TOBS data retrieved.")

        soils_data = QuerySoM("soil")
        # print("Soil data retrieved.")
        # print("soils_data", soils_data)

        soils_ref_data = QuerySoM("soilref")
        # print("Soil REF metadata retrieved.")
        # print("soils_REF_data", soils_ref_data)

        # # Build TOBS metadata lookup by coop_id
        tobs_lookup = {row[0]: row[1:] for row in tobs_data}

        # Build combined_df for temperature stations
        combined_temp_df = build_combined_df(temperature_data, tobs_lookup, month, year)
        json_data = json.dumps(combined_temp_df.to_dicts(), indent=2)
        
        # Build combined_df for evaporation stations (som)
        combined_evap_df = build_combined_df(evaporation_data, tobs_lookup, month, year)
        json_data = json.dumps(combined_evap_df.to_dicts(), indent=2)

        soils_combined_df = getSoilsData(month, year)
        with open(f"soil_data_{month}_{year}.json", "w") as f:
            f.write(json.dumps(soils_combined_df.to_dicts(), indent=2))
        
        print("soilTemperatureTable", getSoilTemperatureTable(soils_combined_df))
        
        # # Write JSON to file for testing/viewing
        # output_file = f"SoMDATA_dly.json"
        # with open(output_file, "w") as f:
        #     f.write(json_data)
        
        # # Write JSON to file for testing/viewing
        # output_file = f"tempDATA_dly.json"
        # with open(output_file, "w") as f:
        #     f.write(json_data)

        # # Write JSON to file for testing/viewing
        # output_file = f"evapDATA_dly.json"
        # with open(output_file, "w") as f:
        #     f.write(json_data)
        
        # # Write JSON to file for testing/viewing
        # output_file = f"precipDATA_dly.json"
        # with open(output_file, "w") as f:
        #     f.write(json_data)
        
  
##########################################
############## EXTREMES ##################
##########################################

        print("Highest Temperature:", getHighestTemperatureExtreme(combined_som_df))
        print("Lowest Temperature:", getLowestTemperatureExtreme(combined_som_df))
        print("Greatst Total Precip:", getGreatestTotalPrecipitationExtreme(combined_som_df))
        print("Least Total Precip:", getLeastTotalPrecipitationExtreme(combined_som_df))
        print("Greatest 1-Day Precip:", getGreatest1DayPrecipitationExtreme(combined_som_df))
        print("Greatest Snowfall:", getGreatestTotalSnowfallExtreme(combined_som_df))
        print("Greatest Snow Depth:", getGreatestSnowDepthExtreme(combined_som_df))

        
#############################################
### MONTHLY STATION AND DIVISION SUMMARY ####
#############################################
        
        Highest = highestRecordedTemp(combined_som_df)
        print("Highest:", Highest)

        Lowest = lowestRecordedTemp(combined_som_df)
        print("Lowest:", Lowest)

        Average = calculate_station_avg(combined_som_df)
        print("Average:", Average)

        Total_IcePelletsAndSnow = getTotalSnowAndIcePellets(combined_som_df)
        print("Total_IcePelletsAndSnow:", Total_IcePelletsAndSnow)

        maxDepthOnGround = getMaxDepthOnGround(combined_som_df)
        print("maxDepthOnGround:", maxDepthOnGround)
        
        merged_SOM_data = merge_SOM_data(Highest, Lowest, Average, Total_IcePelletsAndSnow, maxDepthOnGround)
        print("merged_SOM_data:", merged_SOM_data)

        with open("SoMTable.json", "w") as f:
            json.dump(merged_SOM_data, f, indent=2)
            
#############################################
############ DAILY PRECIPITATION ############
#############################################
        
        
        ##TBD##
        
        
#############################################
########### DAILY TEMPERATURES ##############
#############################################

        TemperatureTable = getTemperatureTable(combined_temp_df)
        print("TemperatureTable:", TemperatureTable)

        tempTableData = getTemperatureTable(combined_temp_df)
        tempTableDataWithNames = add_station_names(tempTableData)
        print("TemperatureTable w names:", tempTableDataWithNames)
        
        with open("tempTable_wNames.json", "w") as f:
            json.dump(tempTableDataWithNames, f, indent=2)
            
#############################################
####### SNOWFALL AND SNOW ON GROUND #########
#############################################

        SnowAndSnwdTable = getSnowAndSnwdTable(combined_precip_df)
        print("SnowAndSnwdTable:", SnowAndSnwdTable)

        with open("SnowAndSnwdTable.json", "w") as f:
            json.dump(SnowAndSnwdTable, f, indent=2)


#############################################
######## DAILY SOIL TEMPERATURES ############
#############################################

        
        soils_combined_df = getSoilsData(month, year)
        
        # # Write JSON to file for testing/viewing
        # with open(f"soilDATA_dly.json", "w") as f:
        #     f.write(json.dumps(soils_combined_df.to_dicts(), indent=2))

        soilTemperatureTable = getSoilTemperatureTable(soils_combined_df)
        print("soilTemperatureTable", soilTemperatureTable)
        
        with open("soilTemperatureTable.json", "w") as f:
            json.dump(soilTemperatureTable, f, indent=2)
        
#############################################
######## SOIL REFERENCE NOTES ############
#############################################  
        
        
        soilRefNotes = get_soil_refernce_notes(soils_ref_data)
        print("SoilRefNotes: ", soilRefNotes)

        with open("soilRefNotes.json", "w") as f:
            json.dump(soilRefNotes, f, indent=2)

#############################################
######## PAN EVAPORATION AND WIND ###########
#############################################


        PanEvaporationTable = getPanEvapTable(combined_evap_df)
        print("PanEvapTable", PanEvaporationTable)


        with open("PanEvaporationTable.json", "w") as f:
            json.dump(PanEvaporationTable, f, indent=2)


    except Exception as e:
        print(f"Error in generateMonthlyPub: {e}")
        pass



################################################################################
# Temporary File for Norms functions.

# NOTE: replace these with external functions or variables. 
CoopToGhcn_defaultPath = "/" + os.path.join("data", "ops", "ghcndqi")
imo = 2
hddid=[]
hddval=[]



# This works differently than python's round().

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
  		@param id - STATE CODE + DIV NUMBER - 4 char string
  		@param atmp - Average Temperature
  		@param pcn - Total Precip
  		@param mo - Month, but its not used in the function?
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

                    tid = line[1:5] # STATE ID CODE + DIVISION NUMBER
                    tt1 = line[0:1] # ELEMENT CODE
                    tt2 = line[92:93] # STATISTIC CODE

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
            dfm = [] # Data for each month

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
            dfm = [] # Data for each month

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
                # This isn't used in this try block?
                ix = atmp.find("M")
                # print(ix)
                if (ix > -1):
                    atmp = atmp[:ix]
                

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

    cldd = "" # Cooling Degree Days Normal?
    hldd = "" # Heating Degree Days Normal?
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
        fn = "/" + os.path.join("data", "ops", "norms", "1991-2020", "products", "station", gid + ".normals.txt")


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
     @param pcn - Total Precip
     @param iMo - Month, integer. Note this is 0 indexed, not 1 indexed
     Return - list[str] - 0 = DFN Temperature
                        - 1 = DFN Precipitation
    """
    dfn = [''] * 2

    # TODO: Update with the external class
    mdfn = getMlyNormals8110(id)
    # mdfn = ghcnDataBrowser.getMlyNormals8110(id)

    trec = mdfn[2] # CSV of Temperature Normals for the year?

    if (trec): # Temperature DFN
        mt = trec.split(",")
        ntmp = mt[imo] # Get the temperature data for selected month

        # multiprint(mt=mt,ntmp=ntmp)

        if (atmp.find("M") >= 0):
            atmp= atmp[0:atmp.find("M")]

        try:
            d1 = float(atmp)
            d2 = float(ntmp)

            d3 = d1 - d2

            dfn[0] = round_it(d3, 1)
        except Exception as err: 
            # print("error: {}".format(traceback.format_exc()))
            dfn[0] = " "

        # multiprint(atmp=atmp, d1=d1, d2=d2, d3=d3, dfn=dfn[0])

    else:
        dfn[0] = " "

    # multiprint(dfn=dfn[0])

    prec = mdfn[5] #CSV of Precip Normals for the year?

    if (prec): # Precip DFN
        mt = prec.split(",")
        npcn = mt[imo]


        if( pcn.find("M") >= 0):
            pcn = pcn[pcn.find("M")+1:]
        if( pcn.find("F") >= 0):
            pcn = pcn[pcn.find("F")+1:]
        if( pcn.find("A") >= 0):
            pcn = pcn[pcn.find("A")+1:]



        try:
            d1 = float(pcn)
            d2 = float(npcn)

            d3 = d1- d2

            dfn[1] = round_it(d3, 2)
        except Exception as err: 
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

         @return list[str] - 0 = DFN Temperature
                            - 1 = DFN Precipitation
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

      @return - list[str] - 0 = DFN Temperature
                        - 1 = DFN Precipitation
    
    """
    
    dfn = [''] * 2

    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641C_1971-2000_NORM_CLIM81_MTH_STNNORM")

    line2 = ""
    line3 = ""

    try:
        with open(fn, "r") as file:
            for line in file:
                if (line):
                    
                    tid = line[:6] # STATION COOPERATIVE I.D. NUMBER (CD NUMBER)
                    tt1 = line[6:7] # ELEMENT CODE
                    tt2 = line[7:9] # DATA CODE

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
    
            

        # multiprint(line2=line2, line3_precip=line3)



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

            dfm[0]=line3[9:15]
            dfm[1]=line3[15:22]
            dfm[2]=line3[22:29]
            dfm[3]=line3[29:36]
            dfm[4]=line3[36:43]
            dfm[5]=line3[43:50]
            dfm[6]=line3[50:57]
            dfm[7]=line3[57:64]
            dfm[8]=line3[64:71]
            dfm[9]=line3[71:78]
            dfm[10]=line3[78:85]
            dfm[11]=line3[85:92]



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

def getNumOfDays(json_data) -> dict:
    """
    getNumOfDays -  Returns count of PRECIPITATION NO OF DAYS >= .01 IN, >= .10 IN, >= 1.00 IN for given data.
	  @param json_data - Json data returned from data parser

      @return - dict:
                    {"Station ID": {
                        '.01 OR MORE': int, 
                        '.10 OR MORE': int, 
                        '1.00 OR MORE': int
                        }
                    } 
    
    """

    if (not json_data):
        return {}
    
    # Determine the number of days in the month
    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        # List Comprehesion
        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]

        if obs_type == "PRCP":
            prcp_data.setdefault(station_id, []).extend(daily_values)

    nod_results = {}
    for station, data in prcp_data.items():

        nod_01 = 0      # Precip Number of Days >= .01 in
        nod_10 = 0      # Precip Number of Days >= .10 in
        nod_100 = 0     # Precip Number of Days >= 1.00 in
                
        for record in data:
    #         print(record)
            if record == -9999:
                continue
            # record*=0.1
            record= record * 0.1 * 0.03937
            record = float(round_it(record, 2))
            
            if record >= .01:
                nod_01+=1   
            if record >= .10:
                nod_10+=1
            if record >= 1.00:
                nod_100+=1

            # print(f"{record:<20} {str(float(record) >= .01):<8} {str(float(record) >= .10):<8} {str(float(record) >= 1.00):<8}")


        nod_results[station] = {
            ".01 OR MORE": nod_01,
            ".10 OR MORE": nod_10,
            "1.00 OR MORE": nod_100

        }
        print(f"{station}: nod_01={nod_01}\tnod_10={nod_10}\tnod_100={nod_100}")


    # print(nod_results)
    return nod_results

####################################

def write_to_file(obj, filename="program_output_fromflask.txt", path="temp/daily_precip/"):
    filename = os.path.join(path, filename)
    if not hasattr(write_to_file, "write_flags"):
        write_to_file.write_flags = {}

    if filename not in write_to_file.write_flags:
        mode = 'w'
        write_to_file.write_flags[filename] = True
    else:
        mode = 'a'

    with open(filename, mode, encoding='utf-8') as f:
        f.write(str(obj) + '\n')

def load_station_data( filename = os.path.join("/data/ops/onyx.imeh/datzilla-flask/", "ghcnd-stations.txt")):
    """Load station data into a dictionary keyed by station ID."""
    stations = {}
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue  # skip malformed lines
            station_id = parts[0]
            state = parts[4]
            name = " ".join(parts[5:])  # everything after the state is the name
            stations[station_id] = (state, name)
    return stations
