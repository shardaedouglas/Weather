
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

def get_mm_to_in(mm: float) -> float:
    """Convert millimeters to inches."""
    return mm * 0.03937 # 1 inch = 25.4 mm


def generateDailyPrecip():
    month = 2
    year = 2023
    
    # # Get Station List
    # stations = QueryDB(DailyPrecipQuery)
    # print(bool(stations))
    # for station in stations[-20:]:
    #     print(station)

    # all_filtered_dfs = []
    # noDataCount = 0

    # for row in stations[:20]:
    #     ghcn_id = row[4]
    #     file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"

    #     if not os.path.exists(file_path):
    #         print(f"Missing file: {file_path}")
    #         continue

    #     try:
    #         filtered_data = parse_and_filter(
    #             station_code=ghcn_id,
    #             file_path=file_path,
    #             correction_type="table",
    #             month=month,
    #             year=year
    #         )

    #         filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

    #         if filtered_df.is_empty():
    #             print(f"Skipping station {ghcn_id} due to no data.")
    #             noDataCount += 1
    #             continue

    #         if all_filtered_dfs:
    #             existing_columns = all_filtered_dfs[0].columns
    #             current_columns = filtered_df.columns

    #             missing_columns = set(existing_columns) - set(current_columns)
    #             for col in missing_columns:
    #                 filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

    #             filtered_df = filtered_df.select(existing_columns)

    #         all_filtered_dfs.append(filtered_df)
    #         print(f"Parsed {len(filtered_df)} records from {ghcn_id}")

    #     except Exception as e:
    #         print(f"Error parsing {ghcn_id}: {e}")
    #         continue

    # if not all_filtered_dfs:
    #     print("No valid station files found.")
    #     return

    # combined_df = pl.concat(all_filtered_dfs, how="vertical")

    # json_data = json.dumps(combined_df.to_dicts(), indent=2)

    # # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)
    
    
    
    
    # print(f"Data saved to {output_file}")

    # print(json_data)


    #####################################################
    #  Read the JSON file. 


    json_data = None

    with open('combined_data_2_2023.json') as f:
        json_data = json.load(f)
        # print(d)


    #########################################

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
        daily_flags = [
            row[f"flag_{i}"] # if row[f"day_{i}"] is not None else None
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))

    for key, data in prcp_data.items():
        # print(f"{key}:\t{data}")
        write_to_file(f"{key}:\t{data}")
    # print(prcp_data)
    ############################################


    prcp_data = {
        'test_station_01': [(0, '  7'), (0, '  7'), (297, '  7'), (157, '  7'), (137, '  7'), (0, 'T 7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (51, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (8, '  7'), (424, '  7'), (0, 'T 7'), (272, '  7'), (244, '  7'), (-9999, '   ')] 
        }
    
    print(len(prcp_data['test_station_01']))

    # prcp_data = {
    #     'USC00040212': [(0, '  7'), (0, '  7'), (114, '  7'), (119, '  7'), (119, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (13, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (127, '  7'), (381, '  7'), (0, '  7'), (51, '  7'), (211, '  7'), (145, '  7')]
    # }
    pcnrec = [""] * 33 # Station Name, Precip Total, pcn record, pcn record, etc. 
    
    idy = 1     # Integer for current day
    inullct = 0 # Integer for null number of days??
    idyct = 0   # ???

    for station, data in prcp_data.items():
        print(station)
        if data is not None: #If there's precip data
            for item in data: # each day
                # print(item)
                pcn = item[0]
                flg = item[1][:1]
                qflg = item[1][2:3]

                print(f"pcn {pcn}  flg {flg}  qflg {qflg}")

                if pcn != -9999:
                    if qflg == " ":
                        d = float(pcn) * 0.1
                        # print(f"mm: {d}  in: {round_it(get_mm_to_in(d),2)}")
                        d = get_mm_to_in(d)
                        pcnrec[idy+1] = round_it(d,2)

                        if pcnrec[idy+1] == "0.00":
                            pcnrec[idy+1] = " "
                    else:
                        d = float(pcn) * 0.1
                        # print(f"mm: {d}  in: {round_it(get_mm_to_in(d),2)}")
                        d = get_mm_to_in(d)
                        pcnrec[idy+1] = round_it(d,2)

                    idyct += 1

                    if flg == "T":
                        pcnrec[idy+1] = "T  "
                
                
                else:  # pcn == -9999
                    if False:
                        pass
                    else:
                        pcnrec[idy] = "-  "
                idy+=1


    return pcnrec






#############################################################################

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

def write_to_file(obj, filename="program_output.txt"):
    if not hasattr(write_to_file, "has_written"):
        mode = 'w'
        write_to_file.has_written = True
    else:
        mode = 'a'

    with open(filename, mode, encoding='utf-8') as f:
        f.write(str(obj) + '\n')


if __name__ == "__main__":
    print(generateDailyPrecip())
