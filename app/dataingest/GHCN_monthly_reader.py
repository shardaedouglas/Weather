# import polars as pl
# import re

# def parse_fixed_width_file_for_month(file_path: str, year: int,  ghcn_id: str, ) -> pl.DataFrame:
#     # Define character offsets
#     metadata_offset = 21
#     day_data_length = 5
#     flag_length = 3
#     num_days = 31

#     # Prepare lists for extracted data
#     rows = []
#     print(ghcn_id, year, month)
    
#     with open(file_path, 'r') as file:
#         for line in file:
#             # Extract metadata (first 21 characters)
#             metadata = line[:metadata_offset]
#             match = re.match(r"^(.{11})(\d{4})(\d{2})(.{4})", metadata)
#             if match:
#                 station_id, line_year, line_month, observation_type = match.groups()
                
#                 # Skip lines that don't match the input year, month, or ghcn_id
#                 if int(line_year) != year or int(line_month) != month or station_id.strip() != ghcn_id.strip():
#                     continue
                
#                 # Split the station_id into components
#                 country_code = station_id[:2]
#                 network_code = station_id[2]
#                 station_code = station_id[3:]
                
#                 # Extract daily data and flags
#                 data_columns = []
#                 quality_flags = []
#                 position = metadata_offset
                
#                 for day in range(1, num_days + 1):
#                     # Extract day data (5 characters) and flag (3 characters)
#                     day_data = line[position:position + day_data_length].strip()
#                     position += day_data_length
                    
#                     day_flag = line[position:position + flag_length].strip()
#                     position += flag_length

#                     # Append data and flag
#                     data_columns.append(day_data)
#                     quality_flags.append(day_flag if day_flag else None)

#                 # Append the parsed row as a dictionary
#                 rows.append({
#                     "country_code": country_code,
#                     "network_code": network_code,
#                     "station_code": station_code,
#                     "year": int(line_year),
#                     "month": int(line_month),
#                     "observation_type": observation_type,
#                     **{f"day_{day}": data_columns[day - 1] for day in range(1, num_days + 1)},
#                     **{f"flag_{day}": quality_flags[day - 1] for day in range(1, num_days + 1)}
#                 })

#     # Convert to a Polars DataFrame
#     if rows:
#         df = pl.DataFrame(rows)
#     else:
#         # Return an empty DataFrame if no matching rows are found
#         df = pl.DataFrame()

#     return df

# # Usage
# if __name__ == '__main__':
#     file_path = "../../USW00093991.dly"
#     ghcn_id = "USW00093991"
#     year = 2020
#     month = 5
    
#     df = parse_fixed_width_file_with_filter(file_path, ghcn_id, year, month)
#     print(df)

#     # Optionally save to CSV for inspection
#     if not df.is_empty():
#         df.write_csv("monthly_test.csv")


import polars as pl
import re

def parse_fixed_width_file_for_month(file_path: str, month: int, year: int, ghcn_id: str) -> pl.DataFrame:
    # Define character offsets
    metadata_offset = 21
    day_data_length = 5
    flag_length = 3
    num_days = 31

    # Split the ghcn_id into components
    country_code = ghcn_id[:2]
    network_code = ghcn_id[2]
    station_code = ghcn_id[3:]

    # Prepare lists for extracted data
    rows = []

    with open(file_path, 'r') as file:
        for line in file:
            # Extract metadata (first 21 characters)
            metadata = line[:metadata_offset]
            match = re.match(r"^(.{11})(\d{4})(\d{2})(.{4})", metadata)
            if match:
                station_id, line_year, line_month, observation_type = match.groups()

                # Split the station_id into components
                file_country_code = station_id[:2]
                file_network_code = station_id[2]
                file_station_code = station_id[3:]

                # Debug print statements to verify data extraction
                # print(f"station_id: {station_id}, line_year: {line_year}, line_month: {line_month}, observation_type: {observation_type}")
                # print(f"file_country_code: {file_country_code}, file_network_code: {file_network_code}, file_station_code: {file_station_code}")
                # print(f"Matching with ghcn_id: {country_code} {network_code} {station_code}")

                # Filter by split ghcn_id, year, and month
                if (file_country_code == country_code and 
                    file_network_code == network_code and 
                    file_station_code == station_code and
                    int(line_year) == year and 
                    int(line_month) == month):

                    # Extract daily data and flags
                    data_columns = []
                    quality_flags = []
                    position = metadata_offset
                    
                    for day in range(1, num_days + 1):
                        # Extract day data (5 characters) and flag (3 characters)
                        day_data = line[position:position + day_data_length].strip()
                        position += day_data_length
                        
                        day_flag = line[position:position + flag_length].strip()
                        position += flag_length

                        # Append data and flag
                        data_columns.append(day_data)
                        quality_flags.append(day_flag if day_flag else None)

                    # Append the parsed row as a dictionary
                    rows.append({
                        "station_id": station_id.strip(),
                        "year": int(line_year),
                        "month": int(line_month),
                        "observation_type": observation_type.strip(),
                        {f"day_{day}": data_columns[day - 1] for day in range(1, num_days + 1)},
                        {f"flag_{day}": quality_flags[day - 1] for day in range(1, num_days + 1)}
                    })

    # Convert to a Polars DataFrame
    df = pl.DataFrame(rows)
    print(df)
    return df

# Usage
if __name__ == '__main__':
    # Inputs
    file_path = "../../USW00093991.dly"
    month = 10
    year = 2024
    ghcn_id = "USW00093991"

    # Parse the file
    df = parse_fixed_width_file_for_month(file_path, month, year, ghcn_id)

    # Show the parsed DataFrame
    print(df)
    df.write_csv("test.csv")



