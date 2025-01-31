import polars as pl
# from GHCNreader import parse_fixed_width_file //FOR STANDALONE TESTING
from  .GHCNreader import parse_fixed_width_file

def filter_data(
    df: pl.DataFrame,
    year=None,
    month=None,
    day=None,
    observation_type=None,
    country_code=None,
    network_code=None,
    station_code=None,
    start_date: datetime = None,
    end_date: datetime = None,
):
    """
    Filters the data based on user input for a dataset where day information is stored horizontally.

    Parameters:
      df (pl.DataFrame): The Polars DataFrame to filter.
      year (int, optional): Year to filter on.
      month (int, optional): Month to filter on.
      observation_type (str, optional): Observation type to filter on.
      country_code (str, optional): Country code to filter on.
      network_code (str, optional): Network code to filter on.
      station_code (str, optional): Station code to filter on.
      start_date (datetime, optional): Start date for filtering (year & month used).
      end_date (datetime, optional): End date for filtering (year & month used).

    Returns:
      pl.DataFrame: The filtered DataFrame.
    """
    
    # Start with a neutral filter condition.
    filter_condition = pl.lit(True)
    
    # Add filters based on user-specified fields.
    if year is not None:
        filter_condition &= (pl.col("year") == year)
    if month is not None:
        filter_condition &= (pl.col("month") == month)
    if observation_type is not None:
        filter_condition &= (pl.col("observation_type") == observation_type)
    if country_code is not None:
        filter_condition &= (pl.col("country_code") == country_code)
    if network_code is not None:
        filter_condition &= (pl.col("network_code") == network_code)
    if station_code is not None:
        filter_condition &= (pl.col("station_code") == station_code)
    
    # If a start_date and end_date are provided, filter based on the combined year-month.
    if start_date is not None and end_date is not None:
        start_val = start_date.year * 100 + start_date.month
        end_val = end_date.year * 100 + end_date.month
        filter_condition &= ((pl.col("year") * 100 + pl.col("month")).is_between(start_val, end_val))
    
    # Apply the filter condition.
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
if __name__ == '__main__':
    file_path = "USW00093991.dly"
    df = parse_fixed_width_file(file_path)

    # Filter example: year=2023, month=1, day=15, observation type="PRCP", station_id="US1MOMA0004"
    # filtered_df = filter_data(df, year=1929, month=8, day=1, observation_type="PRCP", station_code="00006063")
    # Define a date range (filtering only by year and month)
    start_date = datetime(2020, 3, 1)
    end_date = datetime(2023, 12, 31)

    filtered_df = filter_data(df, observation_type="PRCP", start_date=start_date, end_date=end_date)

    filtered_df.write_csv("test_stuff.csv")
    print(filtered_df)
