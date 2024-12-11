from GHCNfilter import filter_data
from GHCNreader import parse_fixed_width_file

def parse_and_filter(
    file_path: str,
    year=None,
    month=None,
    day=None,
    observation_type=None,
    country_code=None,
    network_code=None,
    station_code=None,
):
    """
    Reads and filters the data from a fixed-width file.

    Parameters:
    file_path (str): Path to the fixed-width data file.
    year (int, optional): Year to filter on.
    month (int, optional): Month to filter on.
    day (int, optional): Day to filter on.
    observation_type (str, optional): Observation type to filter on.
    country_code (str, optional): Country code to filter on.
    network_code (str, optional): Network code to filter on.
    station_code (str, optional): Station code to filter on.

    Returns:
    pl.DataFrame: The filtered DataFrame.
    """
    # Step 1: Parse the fixed-width file into a DataFrame
    df = parse_fixed_width_file(file_path)

    # Step 2: Apply filtering using filter_data
    filtered_df = filter_data(
        df,
        year=year,
        month=month,
        day=day,
        observation_type=observation_type,
        country_code=country_code,
        network_code=network_code,
        station_code=station_code,
    )

    return filtered_df

if __name__ == "__main__":

    file_path =  "/data/ops/elan.churavtsov/datzilla-flask/DataFiles/US1MOMA0004.dly"

    # Example 1: Filter by year and country code
    filtered_df = parse_and_filter(
        file_path=file_path,
        year=2023,
        country_code="US"
    )

    print(filtered_df)

    # Example 2: Filter by month, observation type, and network code
    filtered_df = parse_and_filter(
        file_path=file_path,
        month=1,
        observation_type="PRCP",
        network_code="1"
    )

    print(filtered_df)

    # Example 3: Filter by station code and specific day
    filtered_df = parse_and_filter(
        file_path=file_path,
        station_code="MOMA0004",
        day=15
    )

    print(filtered_df)

