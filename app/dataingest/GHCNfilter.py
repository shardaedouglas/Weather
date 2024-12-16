import polars as pl
from GHCNreader import parse_fixed_width_file

def filter_data(
    df: pl.DataFrame,
    year=None,
    month=None,
    day=None,
    observation_type=None,
    country_code=None,
    network_code=None,
    station_code=None,
):
    """
    Filters the data based on user input.

    Parameters:
    df (pl.DataFrame): The Polars DataFrame to filter.
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
    # Start with the full DataFrame
    filter_condition = pl.lit(True)  # Neutral condition

    # Add filters based on user inputs
    if year is not None:
        filter_condition &= pl.col("year") == year
    if month is not None:
        filter_condition &= pl.col("month") == month
    if observation_type is not None:
        filter_condition &= pl.col("observation_type") == observation_type
    if country_code is not None:
        filter_condition &= pl.col("country_code") == country_code
    if network_code is not None:
        filter_condition &= pl.col("network_code") == network_code
    if station_code is not None:
        filter_condition &= pl.col("station_code") == station_code

    # Apply the filter condition
    filtered_df = df.filter(filter_condition)

    # Select only the relevant day and flag columns
    if day is not None:
        day_column = f"day_{day}"
        flag_column = f"flag_{day}"
        relevant_columns = [
            "country_code",
            "network_code",
            "station_code",
            "year",
            "month",
            "observation_type",
            day_column,
            flag_column,
        ]
        filtered_df = filtered_df.select([col for col in relevant_columns if col in filtered_df.columns])

    return filtered_df


# Usage
# if __name__ == '__main__':
#     file_path = "/data/ops/elan.churavtsov/datzilla-flask/DataFiles/US1MOMA0004.dly"
#     df = parse_fixed_width_file(file_path)

#     # Filter example: year=2023, month=1, day=15, observation type="PRCP", station_id="US1MOMA0004"
#     # filtered_df = filter_data(df, year=1929, month=8, day=1, observation_type="PRCP", station_code="00006063")
#     filtered_df = filter_data(df, year=2010, day=21, observation_type="SNOW", station_code="MOMA0004")

#     filtered_df.write_csv("test_stuff.csv")
#     print(filtered_df)
