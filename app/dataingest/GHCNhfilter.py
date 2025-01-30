import polars as pl
from GHCNhreader import parse

from datetime import datetime

def filter_data(
    df: pl.DataFrame,
    start_datetime=None,
    end_datetime=None,
    columns_to_keep=None,
    show_first_x_columns=None,
    show_first_y_rows=None,
):
    """
    Filters the data based on datetime, selected columns, and optionally shows the first x columns and y rows.

    Parameters:
    df (pl.DataFrame): The Polars DataFrame to filter.
    start_datetime (str, optional): Start datetime in ISO format (e.g., '2023-01-01T00:00').
    end_datetime (str, optional): End datetime in ISO format (e.g., '2023-12-31T23:59').
    columns_to_keep (list, optional): List of column names to keep in the final DataFrame.
    show_first_x_columns (int, optional): Number of columns to display in the resulting DataFrame.
    show_first_y_rows (int, optional): Number of rows to display in the resulting DataFrame.

    Returns:
    pl.DataFrame: The filtered DataFrame.
    """
    # Start with the full DataFrame
    filter_condition = pl.lit(True)  # Neutral condition

    # Add datetime filters if specified
    if start_datetime:
        start_datetime_obj = datetime.fromisoformat(start_datetime)
        filter_condition &= pl.col("datetime") >= pl.lit(start_datetime_obj)
    if end_datetime:
        end_datetime_obj = datetime.fromisoformat(end_datetime)
        filter_condition &= pl.col("datetime") <= pl.lit(end_datetime_obj)

    # Apply the datetime filter
    filtered_df = df.filter(filter_condition)

    # Select only the specified columns if provided
    if columns_to_keep:
        filtered_df = filtered_df.select([col for col in columns_to_keep if col in filtered_df.columns])

    # Limit the number of columns if `show_first_x_columns` is specified
    if show_first_x_columns:
        filtered_df = filtered_df.select(filtered_df.columns[:show_first_x_columns])

    # Limit the number of rows if `show_first_y_rows` is specified
    if show_first_y_rows:
        filtered_df = filtered_df.head(show_first_y_rows)

    return filtered_df


if __name__ == "__main__":
    df = parse(file_path='/data/ops/ghcnh/2019/GHCNh_USW00013993_2019.psv')

    columns_to_keep = ["datetime", "temperature", "Station_ID", "Station_name", "Latitude",]
    start_date = "2019-01-01T00:00"
    end_date = "2019-01-02T00:00"
    df = filter_data(df, columns_to_keep=columns_to_keep, start_datetime=start_date, end_datetime=end_date, show_first_y_rows=20)
    print(df)
    df.write_csv("TEST_PSV.csv")