import polars as pl
import matplotlib.pyplot as plt
import pandas as pd
from GHCNreader import parse_fixed_width_file
from GHCNfilter import filter_data

pl.Config(tbl_rows=100)

df = parse_fixed_width_file("US1CALA0014.dly")
observation = "PRCP"
selected_month = 12


df = filter_data(df, observation_type=observation)

# Step 1: Melt day columns
df_long = df.unpivot(
    index=["country_code", "network_code", "station_code", "year", "month", "observation_type"],
    on=[f"day_{i}" for i in range(1, 32)],
    variable_name="day",
    value_name="value"
)

# Step 2: Extract the day number and convert to integer
df_long = df_long.with_columns(
    pl.col("day").str.extract(r"(\d+)").cast(pl.Int32()).alias("day"),
    pl.col("value").replace(-9999, None)  # Replace -9999 with None (NaN)
)


# Step 3: Convert date to datetime, cast value to int, filter out invalid dates, sort by date, filter out unneeded months
df_long = df_long.with_columns(
    pl.date(pl.col('year'), pl.col('month'), pl.col('day')).alias('datetime'), pl.col("value").cast(pl.Int32)
    ).filter(pl.col("datetime").is_not_null(), pl.col("value").is_not_null()
             ).sort("datetime").filter(pl.col("datetime").dt.month() == selected_month)

print(df_long)
df_long.write_csv("test.csv")
overall_mean = df_long['value'].mean()

# Step 4: Group by year and calculate average precipitation
df_long = df_long.with_columns(
    pl.col("datetime").dt.year().alias("year")  # Extract year
).group_by("year").agg(
    pl.col("value").mean().alias("avg_value")
).sort("year")




print(overall_mean)
# Convert to Pandas for plotting
df_plot = df_long.to_pandas()

# Step 5: Plot the data
plt.figure(figsize=(10, 5))
# Remove padding on x-axis
plt.xlim(df_plot["year"].min(), df_plot["year"].max())

plt.plot(df_plot["year"], df_plot["avg_value"], color="g", linestyle="-", label=observation)
plt.axhline(y=overall_mean, color="r", linestyle="--", label=f"Overall Avg {observation}")
plt.xlabel("Date")
plt.ylabel(observation)
plt.title(f"Yearly {observation} for month {selected_month}")
plt.legend()
plt.grid()

plt.savefig(f"{observation}_plot.png")
