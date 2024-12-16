import polars as pl

dtypes = {
    "date": pl.Datetime,
    "temperature": pl.Float32,
    "wind_speed": pl.Float32,
    "dew_point_temperature": pl.Float32,
    "wind_direction": pl.Float32,
    "precipitation": pl.Float32,
    "wind_gust": pl.Float32,
    "relative_humidity": pl.Float32,
    "snow_depth": pl.Float32,
    "station_level_pressure": pl.Float32
}


def parse(file_path, delimiter='|'):
    # Read CSV file with specified columns and delimiter
    result = pl.read_csv(file_path, separator=delimiter,  infer_schema_length=0)
# schema_overrides=dtypes,
    # Create a datetime column from year, month, day, hour, and minute
    result = result.with_columns(pl.datetime(pl.col('Year'), pl.col('Month'), pl.col('Day'), pl.col('Hour'), pl.col('Minute')).alias('datetime'))

    # Filter the rows between the full date range, including full years
    # result = result.filter(pl.col('datetime').is_between(start_date, end_date))

    # Changing checks for metadata. These may be removed later but are good to have for now.
    if result['Station_ID'].n_unique() != 1:
        print("Station ID has changed in the dataset!")
    if result['Station_name'].n_unique() != 1:
        print("Station name has changed in the dataset!")
    if result['Latitude'].n_unique() != 1:
        print("Latitude has changed in the dataset!")
    if result['Longitude'].n_unique() != 1:
        print("Longitude has changed in the dataset!")
    if result['Elevation'].n_unique() != 1:
        print("Elevation has changed in the dataset!")
            

    # Create metadata from the last entry in the dataset.
    site_id = result.select(pl.last("Station_ID"))
    site_name = result.select(pl.last("Station_name"))
    latitude = result.select(pl.last("Latitude"))
    longitude = result.select(pl.last("Longitude"))
    elevation = result.select(pl.last("Elevation"))
    metadata_df = pl.concat([site_id, site_name, latitude, longitude, elevation], how="horizontal")

    # Drop unnecessary columns
    result = result.drop(['Year', 'Month', 'Day', 'Hour', 'Minute'])
    # result = processGHCNh(result, start_date, end_date)

    return result, metadata_df

df = parse(file_path='/data/ops/elan.churavtsov/datzilla-flask/GHCNh_AAI0000TNCA_por.psv')[0]
print (df)
df.write_csv("TEST_PSV.csv")
