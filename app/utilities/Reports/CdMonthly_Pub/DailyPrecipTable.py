
import sys
import os

# Get the absolute path to the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..','..'))
print(parent_dir)
sys.path.insert(0, parent_dir)

# Now you can import from the parent directory
import polars as pl
import json, os
from collections import defaultdict
import calendar
from calendar import monthrange
from datetime import datetime
from app.utilities.Reports.HomrDB import ConnectDB, QueryDB, QuerySoM, DailyPrecipQuery
from app.dataingest.readandfilterGHCN import parse_and_filter

# print(f"{record:<20} {str(float(record) >= .01):<8} {str(float(record) >= .10):<8} {str(float(record) >= 1.00):<8}")

# Testing Multiline queries
# stations = QuerySoM("som")
# print(bool(stations))
# for station in stations[:20]:
#     print(station)

#Testing Daily Precip Query



#  Parameters in the future? Year, Month, Statecode?
def generateDailyPrecip():
    month = 2
    year = 2023
    
    # Get Station List
    stations = QueryDB(DailyPrecipQuery)
    print(bool(stations))
    for station in stations[-20:]:
        print(station)

    all_filtered_dfs = []
    noDataCount = 0

    for row in stations[:20]:
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

    print(json_data)


if __name__ == "__main__":
    generateDailyPrecip()
