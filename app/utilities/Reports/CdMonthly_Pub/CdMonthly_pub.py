def get_state_for_GHCN_table_df():
    try:
        # Extract form data from the POST request
        state = request.form.get('state')  # Expecting 'FL', 'TX', etc.
        station_type = request.form.get('station_type')  # Observation type (e.g., TMAX, TMIN, etc.)
        correction_date = request.form.get('date')

        # Parse the date into components
        if correction_date:
            correction_year, correction_month, correction_day = map(int, correction_date.split('-'))
        else:
            correction_year, correction_month, correction_day = None, None, None
        
        file_path = '/data/ops/ghcnd/data/ghcnd-stations.txt'
        matching_stations = []

        # Read and filter stations by state
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.strip().split()  # Splitting by whitespace
                if len(parts) < 5:
                    continue  # Skip malformed lines
                
                ghcn_id = parts[0]  # First column is the station ID
                state_code = parts[4]  # Fifth column is the state code
                
                if state_code == state:
                    matching_stations.append(line)

        print(f"Found {len(matching_stations)} stations for state {state}")
        
        all_filtered_dfs = []  # List to accumulate filtered DataFrames
        noDataCount = 0

# Loop through the matching stations and parse their data
        for station in matching_stations[:100]:  # You can adjust how many you process here
            parts = station.strip().split()
            ghcn_id = parts[0]  # Station ID
            
            # Build the file path for each station's data
            station_file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
            print(f"Processing file for {ghcn_id}: {station_file_path}")
            
            # Run the custom parser with the correct observation_type
            filtered_data = parse_and_filter(
                station_code=ghcn_id,
                file_path=station_file_path,
                correction_type="graph",
                month=correction_month
            )
            
            # Ensure the filtered_data is converted to a Polars DataFrame (if it's not already one)
            if isinstance(filtered_data, dict):
                # If it's a dictionary, we should convert it to a Polars DataFrame
                filtered_df = pl.DataFrame(filtered_data)
            else:
                # Otherwise, assume it's already a Polars DataFrame
                filtered_df = filtered_data

            # Check if the filtered DataFrame is empty
            if filtered_df.is_empty():
                print(f"Skipping station {ghcn_id} due to no data.")
                noDataCount += 1
                continue  # Skip this station and move to the next
            
            # Align columns by adding missing columns to the DataFrame
            if all_filtered_dfs:
                # Get the columns of the first DataFrame
                existing_columns = all_filtered_dfs[0].columns
                current_columns = filtered_df.columns

                # Add missing columns to current DataFrame, with None values
                missing_columns = set(existing_columns) - set(current_columns)
                for col in missing_columns:
                    filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

                # Ensure columns are in the same order
                filtered_df = filtered_df.select(existing_columns)
            
            # Append to list of DataFrames
            all_filtered_dfs.append(filtered_df)
        

            save_path_limited = f"/data/ops/ghcnd/TestData_pub/limited_data/{state}/{month}/{state}_combined_data.json"
            save_path_limited_parquet = f"/data/ops/ghcnd/TestData_pub/limited_data/{state}/{month}/{state}_combined_data.parquet"

            # Ensure directories exist
            os.makedirs(os.path.dirname(save_path_limited), exist_ok=True)

            if all_filtered_dfs:
                combined_df = pl.concat(all_filtered_dfs, how="vertical")
                print("Combined DataFrame: ", combined_df)

                # Save JSON
                combined_df.write_json(save_path_limited)

                # Save Parquet
                combined_df.write_parquet(save_path_limited_parquet)
        else:
            return jsonify({"error": "No data available for the requested stations."}), 404

    except Exception as e:
        print(f"Error in get_state_for_GHCN_table_df: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    
import polars as pl

def makeGraph(df):
    """
    Placeholder function to process the DataFrame for graphing.
    """
    print("DF!!!!!!!!!!!!: ", df)
    print("Generating graph...")

def processDataForTable(date_param=None):
    """
    Placeholder function for chart processing.
    """
    print("Processing chart data!")

def generateMonthlyPub(date_param=None):
    """
    Reads a Parquet file using Polars, processes it for graphing, 
    and then prepares data for charting.
    
    Args:
        date_param (optional): A date parameter (currently unused).
    """
    parquet_path = "/data/ops/ghcnd/TestData_pub/limited_data/CA/2/CA_combined_data.parquet"

    try:
        # Load the data lazily using scan_parquet for efficiency
        df = pl.scan_parquet(parquet_path).collect()
        
        # Pass data to graphing function
        makeGraph(df)

        # Pass data to chart processing function
        processDataForTable()

    except Exception as e:
        print(f"Error reading or processing the Parquet file: {e}")

