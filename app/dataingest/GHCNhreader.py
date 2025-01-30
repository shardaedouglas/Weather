import polars as pl

def parse(file_path, delimiter='|'):
    # Read CSV file with specified columns and delimiter
    result = pl.read_csv(file_path, separator=delimiter,  infer_schema_length=0)

    # Create a datetime column from year, month, day, hour, and minute
    result = result.with_columns(pl.datetime(pl.col('Year'), pl.col('Month'), pl.col('Day'), pl.col('Hour'), pl.col('Minute')).alias('datetime'))

    # Filter the rows between the full date range, including full years
    # result = result.filter(pl.col('datetime').is_between(start_date, end_date))

    # Drop unnecessary columns
    result = result.drop(['Year', 'Month', 'Day', 'Hour', 'Minute'])

    return result

if __name__ == "__main__":
    df = parse(file_path='/data/ops/ghcnh/2019/GHCNh_USW00013993_2019.psv')
    print (df)
    for column_name, column_type in df.schema.items():
        print(f"{column_name}")
    df.write_csv("TEST_PSV.csv")
