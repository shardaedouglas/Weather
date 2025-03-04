from GHCNreader import parse_fixed_width_file
from GHCNfilter import filter_data
    
file_path = "US1MAMD0185.dly"
df = parse_fixed_width_file(file_path)
observation = "PRCP"
df = filter_data(df, observation_type="PRCP")
df.write_csv("test.csv")


import polars as pl
import matplotlib.pyplot as plt


day_columns = [col for col in df.columns if col.startswith("day_") and col[4:].isdigit()]

df = df.with_columns([pl.col(col).cast(pl.Int32) for col in day_columns])

print(df['observation_type'])

df = df.with_columns(pl.when(pl.col("day_1") == -9999)
                       .then(None)  # Polars uses None for missing values
                       .otherwise(pl.col("day_1"))
                       .alias("day_1"))

# Convert datetime to a format that Matplotlib understands
df_pandas = df.to_pandas()

# Plot
plt.figure(figsize=(8, 5))
plt.plot(df_pandas["year"], df_pandas["day_1"], marker="o", linestyle="-", label="Temperature")
plt.xlabel("Date")
plt.ylabel("Temperature (°C)")
plt.title("Temperature Over Time")
plt.legend()
plt.grid()

plt.savefig("temperature_plot.png", dpi=300, bbox_inches="tight")
