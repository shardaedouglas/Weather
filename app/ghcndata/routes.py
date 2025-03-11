from app.ghcndata import ghcndata_bp
from flask import render_template, request, jsonify
from app.extensions import mail #Move to utilities
from flask_mail import Message #Move to utilities
from app.ghcndata.forms import GhcnDataForm, GhcnDataHourlyForm
from app.dataingest.readandfilterGHCN import parse_and_filter
from datetime import datetime
import os
import re


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
        
        JSONformatedData = format_as_json(filtered_df)
        # print("JSONformatedData in ghcndata routes",JSONformatedData)

        # Return
        return JSONformatedData
        # return jsonify({
        #     "message": f"Correction processed successfully for GHCN ID: {ghcn_id}!",
        #     "filtered_data": filtered_df
        # }), 201
        
    except Exception as e:
        print(f"Error in get_data_for_GHCN_table: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
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

def format_as_json(filtered_df):
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

    return jsonify(formatted_data)

