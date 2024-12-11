import polars as pl
import re

def parse_fixed_width_file(file_path: str) -> pl.DataFrame:
    # Define character offsets
    metadata_offset = 21
    day_data_length = 5
    flag_length = 3
    num_days = 31

    # Prepare lists for extracted data
    rows = []
    
    with open(file_path, 'r') as file:
        for line in file:
            # Extract metadata (first 21 characters)
            metadata = line[:metadata_offset]
            match = re.match(r"^(.{11})(\d{4})(\d{2})(.{4})", metadata)
            if match:
                station_id, year, month, observation_type = match.groups()
                
                # Split the station_id into components
                country_code = station_id[:2]
                network_code = station_id[2]
                station_code = station_id[3:]
                
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
                    "country_code": country_code,
                    "network_code": network_code,
                    "station_code": station_code,
                    "year": int(year),
                    "month": int(month),
                    "observation_type": observation_type,
                    **{f"day_{day}": data_columns[day - 1] for day in range(1, num_days + 1)},
                    **{f"flag_{day}": quality_flags[day - 1] for day in range(1, num_days + 1)}
                })

    # Convert to a Polars DataFrame
    df = pl.DataFrame(rows)

    return df

# Usage
if __name__ == '__main__':
    
    file_path = "/data/ops/elan.churavtsov/datzilla-flask/DataFiles/US1MOMA0004.dly"
    df = parse_fixed_width_file(file_path)

    # Show the parsed DataFrame
    print(df)
    df.write_csv("test.csv")