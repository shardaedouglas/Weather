import polars as pl
from GHCNreader import parse_fixed_width_file, read_station_list
import json

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
    # Concatenate country_code + network_code + station_code into a single station_code string
    df = df.with_columns([
        (pl.col("country_code") + pl.col("network_code") + pl.col("station_code")).alias("station_code")
    ])

    # Filter TMAX and TMIN separately
    tmax_df = df.filter(pl.col("observation_type") == "TMAX")
    tmin_df = df.filter(pl.col("observation_type") == "TMIN")

    # List of day columns (from day_1 to day_31)
    day_columns = [f"day_{i}" for i in range(1, 32)]

    # Unpivot (melt) both dataframes
    tmax_long = tmax_df.unpivot(
        index=["station_code"],
        on=day_columns,
        variable_name="day",
        value_name="tmax"
    ).filter(pl.col("tmax") != "-9999")  # Remove invalid values

    tmin_long = tmin_df.unpivot(
        index=["station_code"],
        on=day_columns,
        variable_name="day",
        value_name="tmin"
    ).filter(pl.col("tmin") != "-9999")  # Remove invalid values

    # Cast tmax and tmin to Int64 and convert to degrees Celsius
    tmax_long = tmax_long.with_columns([
        (pl.col("tmax").cast(pl.Int64) / 10).alias("tmax")
    ])
    tmin_long = tmin_long.with_columns([
        (pl.col("tmin").cast(pl.Int64) / 10).alias("tmin")
    ])

    # Compute averages for each station
    tmax_avg = tmax_long.group_by("station_code").agg(pl.col("tmax").mean().alias("tmax_avg"))
    tmin_avg = tmin_long.group_by("station_code").agg(pl.col("tmin").mean().alias("tmin_avg"))

    # Compute days where TMAX >= 90F (32.2C), TMAX <= 32F (0C), TMIN <= 32F (0C), TMIN <= 0F (-17.8C)
    tmax_90 = tmax_long.filter(pl.col("tmax") >= 32.2).group_by("station_code").len().rename({"len": "TMAX>=90"})
    tmax_32 = tmax_long.filter(pl.col("tmax") <= 0).group_by("station_code").len().rename({"len": "TMAX<=32"})
    tmin_32 = tmin_long.filter(pl.col("tmin") <= 0).group_by("station_code").len().rename({"len": "TMIN<=32"})
    tmin_0 = tmin_long.filter(pl.col("tmin") <= -17.8).group_by("station_code").len().rename({"len": "TMIN<=0"})

    # Merge all results
    result = tmax_avg.join(tmin_avg, on="station_code", how="full")
    result = result.join(tmax_90, on="station_code", how="left")
    result = result.join(tmax_32, on="station_code", how="left")
    result = result.join(tmin_32, on="station_code", how="left")
    result = result.join(tmin_0, on="station_code", how="left")

    # Fill nulls (for stations with 0 such days)
    result = result.fill_null(0)

    # Compute overall average
    result = result.with_columns(((pl.col("tmax_avg") + pl.col("tmin_avg")) / 2).alias("overall_avg"))

    return dataframe_to_json(result.drop(pl.col("station_code_right")))


if __name__ == '__main__':
    import time
    from readandfilterGHCN import get_state_for_ghcn_data

    # station_list = ["US1CALA0014.dly", "US1CALA0068.dly", "US1MAMD0185.dly", "USC00106705.dly", "USW00093991.dly"]
    
    # df = read_station_list(station_list)
    # df = get_state_for_ghcn_data("ME")
    # df = df.filter((pl.col("year") == 2022) & (pl.col("month") == 3))
    df = parse_fixed_width_file("USW00093991.dly")
    # print(df)
    # df.write_csv("test.csv")
    start_time = time.time()  # Start timer
    observation = "PRCP"
    selected_month = 12
    
    df = calculate_station_avg(df)
    # df = dataframe_to_json(df)
    end_time = time.time()
    print(df)
    # df.write_csv("test.csv")
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")