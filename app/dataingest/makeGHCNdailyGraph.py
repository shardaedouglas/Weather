import polars as pl
import matplotlib.pyplot as plt
import pandas as pd
from GHCNreader import parse_fixed_width_file, read_station_list
from GHCNfilter import filter_data

pl.Config(tbl_rows=100)

def make_yearly_per_month_graph(df, observation, selected_month):
    
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
        ).filter(pl.col("datetime").is_not_null(), pl.col("value").is_not_null(), pl.col("datetime").dt.month() == selected_month
                ).sort("datetime")


    overall_mean = df_long['value'].mean()

    # Step 4: First, group by date to remove multiple observations per day
    df_long = df_long.group_by("datetime").agg(
        pl.col("value").mean().alias("daily_avg_value")  # Compute average per day
    )
    # print(df_long)
    # df_long.write_csv("test.csv")
    # Step 5: Extract year and calculate the average for the selected month
    df_long = df_long.with_columns(
        pl.col("datetime").dt.year().alias("year")
    ).group_by("year").agg(
        pl.col("daily_avg_value").mean().alias("avg_value")  # Compute yearly average for the month
    ).sort("year")




    # print(overall_mean)
    # Convert to Pandas for plotting
    df_plot = df_long.to_pandas()

    # Step 6: Plot the data
    plt.figure(figsize=(13, 5))
    # Remove padding on x-axis
    plt.xlim(df_plot["year"].min(), df_plot["year"].max())

    plt.axhline(y=overall_mean, color="orangered", linestyle="-", linewidth=1.25, label=f"Avg {observation}")
    plt.plot(df_plot["year"], df_plot["avg_value"], color="lime", linestyle="-", linewidth=1.25, label=observation)
    
    plt.ylabel(observation)
    plt.title(f"Average {observation} in Month {selected_month} Over the Years")
    plt.legend()
    plt.grid(linestyle="--")

    # Move the legend below the graph
    plt.legend(
    loc="upper center", 
    bbox_to_anchor=(0.5, -0.075), 
    ncol=2, 
    frameon=True,  # Enable the legend frame
    edgecolor="black",  # Set border color
    framealpha=1,  # Make border fully opaque
    fancybox=False,  # Disable rounded corners
    borderpad=0.4,  # Add padding inside the box
    fontsize=12  # Adjust text size if needed
    )  

    plt.savefig(f"{observation}_plot.png", bbox_inches="tight")

# Usage
if __name__ == '__main__':
    import time
    from readandfilterGHCN import get_state_for_ghcn_data

    # station_list = ["US1CALA0014.dly", "US1CALA0068.dly", "US1MAMD0185.dly", "USC00106705.dly", "USW00093991.dly"]
    start_time = time.time()  # Start timer
    # df = read_station_list(station_list)
    df = get_state_for_ghcn_data("TX")
    # df = parse_fixed_width_file("US1CALA0014.dly")

    observation = "PRCP"
    selected_month = 12
    
    df = make_yearly_per_month_graph(df, observation, selected_month)
    end_time = time.time()  # End timer
    # print(df)
    # df.write_csv("test.csv")
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")