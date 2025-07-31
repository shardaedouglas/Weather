from app.ghcndata import ghcndata_bp
from flask import render_template, request, jsonify
from app.extensions import mail #Move to utilities
from flask_mail import Message #Move to utilities
from app.ghcndata.forms import GhcnDataForm, GhcnDataHourlyForm
from app.dataingest.readandfilterGHCN import parse_and_filter
from app.utilities.Reports.CdMonthly_Pub.CdMonthly_pub import generateMonthlyPub, lowestRecordedTemp, getLowestTemperatureExtreme, getTemperatureTable, calculate_station_avg
from app.utilities.Reports.CdMonthly_Pub.CdMonthly_pub import calculate_station_avg, highestRecordedTemp, lowestRecordedTemp, getTotalSnowAndIcePellets, getMaxDepthOnGround, getGreatest1DayPrecipitationExtreme, getNumOfDays, getMonthlyHDD, generateDailyPrecip, generateSDThreshold, generateSFThreshold 

from datetime import datetime
import os
import re
import json
import polars as pl
import traceback


# Route to Render GHCN Station Page
@ghcndata_bp.route('/ghcn_data')
def view_ghcn_data():
    # Form instatialization
    form = GhcnDataForm(country='', state='')

    return render_template('/ghcn_data/ghcndata_form.html', ghcnForm=form)


# Route to Render Station Metadata Page
@ghcndata_bp.route('/station_metadata')
def view_ghcn_metadata():
    
    ghcn_id = request.args.get('ghcn_id')  # Retrieves 'ghcn_id' from the URL query params
    
    station_metadata_file_path = '/data/ops/ghcnd/data/ghcnd-stations.txt' # I THINK THIS IS HARD CODED IN THE PARSER STILL
    
    station_data = {
        'GHCN ID': None,
        'WMO ID': None,
        'State': None,
        'Country': None,
        'Longitude': None,
        'Latitude': None,
        'GSN Flag': None,
        'HCN Flag': None
    }
    
 # Open the file and read lines
    with open(station_metadata_file_path, 'r') as file:
        for line in file:
            # Check if the line contains the target GHCN ID
            if ghcn_id in line:
                # Split the line into components
                fields = line.split()
                
                # GHCN ID is the first part of the line
                station_data['GHCN ID'] = fields[0]
                
                # Country is derived from the first two characters of GHCN ID
                station_data['Country'] = station_data['GHCN ID'][:2]
                
                # Latitude and Longitude are the next two values
                station_data['Latitude'] = float(fields[1])
                station_data['Longitude'] = float(fields[2])
                
                # State is present only for US/Canadian stations and is the fourth value for those stations
                if len(fields) > 4 and len(fields[4]) == 2:  # Checks if state code is present
                    station_data['State'] = fields[4]
                
                # WMO ID, GSN Flag, and HCN Flag can be found at the end of the line
                if len(fields) > 5:
                    # Check if there's a WMO ID (numeric and 5 digits)
                    if len(fields[-1]) == 5 and fields[-1].isdigit():
                        station_data['WMO ID'] = fields[-1]
                        fields.pop()  # Remove the WMO ID from the list of fields
                
                # Look for the GSN and HCN flags in the remaining parts
                if 'GSN' in fields:
                    station_data['GSN Flag'] = 'GSN'
                if 'HCN' in fields or 'CRN' in fields:
                    station_data['HCN Flag'] = 'HCN' if 'HCN' in fields else 'CRN'
                
                return render_template('/ghcn_data/station_metadata.html', station_data=station_data)

    print("GHCN ID not found.")    
    return render_template('/ghcn_data/station_metadata.html', station_data=None)




# Route to Send Emails
@ghcndata_bp.route('/send_email') # Need to be removed from this directory and added into Utilities...
def send_email():
    msg = Message("Hello this is a test to myself!",
                  sender="NCEI.Datzilla@noaa.gov",
                  recipients=["matthew.kelly@noaa.gov"])
    msg.body = "This is the email body"
    mail.send(msg)
    return "Email sent!"    


def c_tenths_to_f(val: str) -> str:
    try:
        ivalue = int(val)
        if ivalue == -9999:
            return val
        celsius = ivalue / 10
        fahrenheit = (celsius * 9/5) + 32
        return str(round(fahrenheit, 1))  # You can change to `0` if you want whole degrees
    except:
        return val
    
def cm_tenths_to_inches(val: str) -> str:
    try:
        ivalue = int(val)
        if ivalue == -9999:
            return val
        cm = ivalue / 10
        inches = cm / 2.54
        return format(inches, ".2f")  # always 2 decimal places
    except:
        return val
    
def mm_to_inches(val: str) -> str:
    try:
        ivalue = int(val)
        if ivalue == -9999:
            return val
        inches = ivalue / 25.4
        return format(inches, ".2f")
    except:
        return val

def tenths_mm_to_inches(val: str) -> str:
    try:
        ivalue = int(val)
        if ivalue == -9999:
            return val
        mm = ivalue / 10
        inches = mm / 25.4
        return format(inches, ".2f")
    except:
        return val
    
def wind_tenths_to_mph(val: str) -> str:
    if val == "-9999":
        return val
    return f"{round((int(val) / 10) * 2.23694, 2):.2f}"

def cm_to_inches(val: str) -> str:
    if val == "-9999":
        return val
    return f"{round(float(val) / 2.54, 2):.2f}"

def km_to_miles(val: str) -> str:
    if val == "-9999":
        return val
    return f"{round(float(val) * 0.621371, 2):.2f}"
    
    
@ghcndata_bp.route('/get_data_for_GHCN_table', methods=['POST'])
def get_data_for_GHCN_table():
    try:
        # print("Form Data:", request.form)
        # Extract form data from the POST request
        ghcn_id = request.form.get('ghcn_id')

        # country = request.form.get('country')
        # state = request.form.get('state')
        station_type = request.form.get('station_type')
        correction_date = request.form.get('date')
        
        if correction_date:
            correction_year, correction_month, correction_day = correction_date.split('-')
            correction_year = int(correction_year)
            correction_month = int(correction_month)
            correction_day = int(correction_day)
        else:
            correction_year, correction_month, correction_day = None, None, None
        
        
        file_path = '/data/ops/ghcnd/data/ghcnd_all/' + ghcn_id + ".dly" # I THINK THIS IS HARD CODED IN THE PARSER STILL
        
        # print("ghcn_id:", ghcn_id)
        print("file_path: ", file_path)
        # print("correction_year: ", correction_year)
        # print("correction_month: ", correction_month)
        # print("station_type: ", station_type)


        # # Run parser with form data
        filtered_df = parse_and_filter(
            station_code = ghcn_id,
            file_path=file_path,
            year=correction_year,
            month=correction_month,
            # observation_type = station_type,
            correction_type=""
        )
        
        # # Print results
        # print("filtered_df in ghcndata routes",filtered_df)
        
        JSONformattedData = format_as_json(filtered_df, return_response=True)        
        print("JSONformattedData in ghcndata routes", JSONformattedData.get_data(as_text=True))
        
        data = JSONformattedData.get_json()  # this gives us the dict

        temp_keys = {"TMAX", "TMIN", "TAVG", "TAXN", "TOBS", "MDTX", "MDTN", "AWBT", "ADPT", "MNPN", "MXPN"}
        tenths_mm_keys = {"PRCP", "EVAP", "WESD", "WESF", "MDEV", "MDPR", "THIC"}
        mm_keys = {"SNOW", "SNWD", "MDSF"}
        wind_keys = {"AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"}
        cm_keys = {"FRGB", "FRGT", "FRTH", "GAHT"}
        km_keys = {"MDWM", "WDMV"}
        
        for day in data:
            for key in data[day]:
                if key in temp_keys or re.match(r"SN\d{2}|SX\d{2}", key):
                    data[day][key] = c_tenths_to_f(data[day][key])
                elif key in tenths_mm_keys:
                    data[day][key] = tenths_mm_to_inches(data[day][key])
                elif key in mm_keys:
                    data[day][key] = mm_to_inches(data[day][key])
                elif key in wind_keys:
                    data[day][key] = wind_tenths_to_mph(data[day][key])
                elif key in cm_keys:
                    data[day][key] = cm_to_inches(data[day][key])
                elif key in km_keys:
                    data[day][key] = km_to_miles(data[day][key])

        # Replace the original Response with new one containing converted values
        JSONformattedData = jsonify(data)
        print("New JSON in ghcndata routes", JSONformattedData.get_data(as_text=True))

        # Return
        return JSONformattedData
        # return jsonify({
        #     "message": f"Correction processed successfully for GHCN ID: {ghcn_id}!",
        #     "filtered_data": filtered_df
        # }), 201
        
    except Exception as e:
        print(f"Error in get_data_for_GHCN_table: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    

@ghcndata_bp.route('/get_state_for_GHCN_table', methods=['POST'])
def get_state_for_GHCN_table():
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
        
        # List to store parsed data for each station
        all_station_data = []
        noDataCount = 0

        # Loop through the matching stations and parse their data
        for station in matching_stations[:10]:  # You can adjust how many you process here
            parts = station.strip().split()
            ghcn_id = parts[0]  # Station ID
            
            # Build the file path for each station's data
            station_file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
            print(f"Processing file for {ghcn_id}: {station_file_path}")
            
            # Run the custom parser with the correct observation_type
            filtered_df = parse_and_filter(
                station_code=ghcn_id,
                file_path=station_file_path,
                year=correction_year,
                month=correction_month,
                correction_type="",
            )
            # print("filtered_df: ", filtered_df)
            if filtered_df.get('status') == 'skip':
                print(f"Skipping station {ghcn_id} due to no data.")
                noDataCount+= 1
                continue  # Skip this station and move to the next
            
            # filtered_df:  {'status': 'skip', 'station_code': 'US1FLAL0004'}
            # You can format the filtered dataframe as needed
            JSONformattedData = format_as_json(filtered_df, return_response=False)
            
            # Store the data for this station
            all_station_data.append({
                "ghcn_id": ghcn_id,
                "data": JSONformattedData
            })
            
        print("all_station_data: ", all_station_data)
        # Return the data for all processed stations
        print(f"noDataCount: ", noDataCount)
        
        
        filename = "station_data.json"
        # Overwrite file with new data
        with open(filename, "w") as file:
            json.dump(all_station_data, file, indent=2)

        print(f"Data successfully saved to {filename}")

        return jsonify({"stations": all_station_data}), 200

    except Exception as e:
        print(f"Error in get_state_for_GHCN_table: {e}")
        return jsonify({"error": "Internal server error"}), 500


    
@ghcndata_bp.route('/get_state_for_GHCN_table_df', methods=['POST'])
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
        for station in matching_stations[:10]:  # You can adjust how many you process here
            parts = station.strip().split()
            ghcn_id = parts[0]  # Station ID
            
            # Build the file path for each station's data
            station_file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
            print(f"Processing file for {ghcn_id}: {station_file_path}")
            
            # Run the custom parser with the correct observation_type
            filtered_data = parse_and_filter(
                station_code=ghcn_id,
                file_path=station_file_path,
                year=correction_year,
                month=correction_month,
                correction_type="",
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
        

        # Combine all the filtered DataFrames into a single Polars DataFrame
        if all_filtered_dfs:
            combined_df = pl.concat(all_filtered_dfs, how="vertical")
            print("Combined DataFrame: ", combined_df)
            combined_df.write_json("combined_data.json")

            return combined_df.to_dicts()  # Returning the combined DataFrame as JSON
        else:
            return jsonify({"error": "No data available for the requested stations."}), 404

    except Exception as e:
        print(f"Error in get_state_for_GHCN_table_df: {e}")
        return jsonify({"error": "Internal server error"}), 500

@ghcndata_bp.route('/get_station_calc_for_GHCND', methods=['POST'])
def  get_station_calc_for_GHCND():

    ghcn_id = request.form.get('ghcn_id')
    # country = request.form.get('country')
    # state = request.form.get('state')
    station_type = request.form.get('station_type')
    correction_date = request.form.get('date')
    
    if correction_date:
        correction_year, correction_month, correction_day = correction_date.split('-')
        correction_year = int(correction_year)
        correction_month = int(correction_month)
        correction_day = int(correction_day)
    else:
        correction_year, correction_month, correction_day = None, None, None
    
    # FOR TESTING
    # ghcn_id = 'USC00040212' # Angwin
    # ghcn_id = 'USC00040820' #ASPENDELL
    # correction_year = 2023
    # correction_month = 2

    file_path = '/data/ops/ghcnd/data/ghcnd_all/' + ghcn_id + ".dly"
    
    # print("ghcn_id:", ghcn_id)
    # print("file_path: ", file_path)
    # print("correction_year: ", correction_year)
    # print("correction_month: ", correction_month)
    # print("station_type: ", station_type)

    # # Run parser with form data
    filtered_data = parse_and_filter(
        station_code = ghcn_id,
        file_path=file_path,
        year=correction_year,
        month=correction_month,
        # observation_type = station_type,
        correction_type="table"
    )
    
    
    filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data
    filtered_json = filtered_df.to_dicts()

    try:
        station_avgs = calculate_station_avg(filtered_df)[ghcn_id]
    except Exception as err:
        print("error in calculate_station_avg():\n{}".format(traceback.format_exc()))
        station_avgs = None
    try:
        max_temp = highestRecordedTemp(filtered_df)[ghcn_id]
    except Exception as err:
        print("error in highestRecordedTemp():\n{}".format(traceback.format_exc()))
        max_temp = None
    try:
        min_temp = lowestRecordedTemp(filtered_df)[ghcn_id]
    except Exception as err:
        print("error in lowestRecordedTemp():\n{}".format(traceback.format_exc()))
        min_temp = None
    try:
        max_snow = getTotalSnowAndIcePellets(filtered_df)[ghcn_id]
    except Exception as err:
        print("error in lowestRecordedTemp():\n{}".format(traceback.format_exc()))
        max_snow = None
    try:
        max_snow_depth = getMaxDepthOnGround(filtered_df)[ghcn_id]
    except Exception as err:
        print("error in getMaxDepthOnGround():\n{}".format(traceback.format_exc()))
        max_snow_depth = [None]
    try:
        max_24hr_prcp = getGreatest1DayPrecipitationExtreme(filtered_df)
    except Exception as err:
        print("error in getGreatest1DayPrecipitationExtreme(): \n{}".format(traceback.format_exc()))
        max_24hr_prcp = None
    try:
        nod_prcp = getNumOfDays(filtered_json)[ghcn_id]
    except Exception as err:
        print("error in getNumOfDays(): \n{}".format(traceback.format_exc()))
        nod_prcp = None
    try:
        hdd = getMonthlyHDD(filtered_df)[ghcn_id]['total_HDD']
    except Exception as err:
        print("error in getMonthlyHDD(): \n{}".format(traceback.format_exc()))
        hdd = None
    try:
        total_pcn = generateDailyPrecip(filtered_json, [str(ghcn_id)])[ghcn_id]['total_pcn']
    except Exception as err:
        print("error in generateDailyPrecip(): \n{}".format(traceback.format_exc()))
        total_pcn = None
    try:
        sd_threshold = generateSDThreshold(filtered_json)[ghcn_id]['1.00 OR MORE']
    except Exception as err:
        print("error in generateSDThreshold(): \n{}".format(traceback.format_exc()))
        sd_threshold = None
    try:
        sf_threshold = generateSFThreshold(filtered_json)[ghcn_id]['1.00 OR MORE']
    except Exception as err:
        print("error in generateSFThreshold(): \n{}".format(traceback.format_exc()))
        sf_threshold = None
    
    
    
    # print(sd_threshold, sf_threshold)
    
    
    
    

    # print(filtered_json)
    # result = generateDailyPrecip(filtered_json, [str(ghcn_id)])
    # print(f"{type(result)}\n{result}")


    comp_calcs = { 
        "AvMax" : station_avgs.get("Average Maximum"),
        "AvMin" : station_avgs.get("Average Minimum"),
        "AvTmp" : station_avgs.get("Average"),
        "MaxTp" : {
            "MaxTp": max_temp.get('value'),
            "Day": max_temp.get('date')
        },
        "MinTp" : {
            "MinTp": min_temp.get('value'),
            "Day": min_temp.get('date')
        },
        "Max24Hr" : {
            "Max24Hr": max_24hr_prcp.get('value'),
            "Day": max_24hr_prcp.get('day')
        },
        "TotPcn" : total_pcn,
        "Snow" : max_snow,
        "S Depth" : max_snow_depth[0],
        "HDD" : hdd,
        "CDD " : None,
        "NOD Pcn": {
            ">.01": nod_prcp.get('.01 OR MORE'),
            ">.10": nod_prcp.get('.10 OR MORE'),
            ">1": nod_prcp.get('1.00 OR MORE'),
        },
        "NOD Tmp": {
            "MxT>=90": station_avgs.get(">=90_MAX"),
            "MxT<=32": station_avgs.get("<=32_MAX"),
            # "MnT<=32": station_avgs.get("<=32_MIN"),
            "MnT<=0": station_avgs.get("<=0_MIN"),
        },
        "SF>=1" : sd_threshold, # NOD Snowfall >= one inch
        "SD>=1" : sf_threshold, # NOD Snow Depth >= one inch
    }


    # for k, v in comp_calcs.items():
    #     if v is not None:
    #         print(f"{k}: {v}")
    return comp_calcs
    
@ghcndata_bp.route('/test_monthlyPub')
def test_monthlyPub():
    try:
        
        
        generateMonthlyPub()

        return jsonify({"message": "generateMonthlyPub() executed successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500  

    
    
    
@ghcndata_bp.route('/ghcn_hourly')
def view_ghcn_hourly_data(): 
    ghcn_id = request.args.get('ghcn_id', '')
    date = request.args.get('date','')
    hour= request.args.get('hour','')

    if date:
        date = datetime.strptime(date, '%Y-%m-%d').date()

    # Read the GHCN Hourly header list from file
    data = []
    with open("GHCNh_psv_column_names.txt") as load_file:
        # temp_list = []
        for line in load_file:
            temp_list = ['']
            line = line.split()
            # print(temp_list + line)
            data.append(tuple(line + line))

    # print(data)

    # print("ID{}\ndate{}\nhour{}".format(ghcn_id, date, hour))
    form = GhcnDataHourlyForm(
        ghcn_id=ghcn_id,
        date=date,
        hour=hour
    )

    form.element.choices = data
    
    return render_template('/ghcn_data/hourly/ghcn_hourly_data.html', ghcnHourlyForm=form)

def format_as_json(filtered_df, return_response=True):
    # Create a dictionary to hold the formatted data
    formatted_data = {}

    # Ensure the number of rows is the same for 'observation_type' and 'day_X'
    num_rows = len(filtered_df['observation_type'])

    # Iterate through each day (1 to 31) to structure the data by day
    for day in range(1, 32):
        day_key = f"Day {day}"
        
        # Initialize a dictionary for each day
        formatted_data[day_key] = {}

        # Add all observation types and their values for the current day
        for i in range(num_rows):
            observation_type = filtered_df['observation_type'][i]
            day_column = f"day_{day}"
            day_value = filtered_df[day_column][i] if i < len(filtered_df[day_column]) else None
            
            # Add the observation type as a key for the day
            formatted_data[day_key][observation_type] = day_value

    # If return_response is True, return a JSON response (for Flask routes)
    if return_response:
        return jsonify(formatted_data)
    else:
        return formatted_data  # Return raw dictionary


