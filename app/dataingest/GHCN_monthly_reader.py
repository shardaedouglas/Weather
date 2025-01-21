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

                # Filter by split ghcn_id, year, and month
                if (file_country_code == country_code and 
                    file_network_code == network_code and 
                    file_station_code == station_code and
                    int(line_year) == year and 
                    int(line_month) == month):
                    
                    # Append the parsed row as a dictionary
                    rows.append({
                        "station_id": station_id.strip(),
                        "year": int(line_year),
                        "month": int(line_month),
                        "observation_type": observation_type.strip(),
                    })

    # Convert to a Polars DataFrame
    df = pl.DataFrame(rows)
    print(df)
    return df

# # Usage
# if __name__ == '__main__':
#     # Inputs
#     file_path = "../../USW00093991.dly"
#     month = 10
#     year = 2024
#     ghcn_id = "USW00093991"

#     # Parse the file
#     df = parse_fixed_width_file_for_month(file_path, month, year, ghcn_id)

#     # Show the parsed DataFrame
#     print(df)
#     df.write_csv("test.csv")



