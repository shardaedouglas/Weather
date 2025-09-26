from app.corrections import correction_bp
from urllib.parse import urlparse, parse_qs, parse_qsl
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections, HourlyCorrections
from flask import render_template, request, jsonify, flash
from app.extensions import get_db, find_stations, parse_station_file, get_station_lat_long, find_nearest_station
from app.dataingest.readandfilterGHCN import parse_and_filter
from app.corrections.models.corrections import Corrections
from app.ghcndata.routes import mm_to_inches, tenths_mm_to_inches, c_tenths_to_f, cm_tenths_to_inches, km_to_miles, wind_tenths_to_mph, cm_to_inches
from datetime import date, datetime
import os
from flask_login import login_required
from flask import session
import traceback
from app.utilities.JSON_DataStore import JSON_DataStore as js_ds



file_path = os.path.join(os.getcwd(), 'USW00093991.dly')

# All Corrections Landing Page
@correction_bp.route('/')
@correction_bp.route('/corrections')
# @login_required
def index():
    js = js_ds()
    page_settings = js.get_admin_settings()
    username = "NCEI User"
    if "_username" in session:
        username= session["_username"]
    page_settings["username"] = username
    #print( session["_user_id"] )
    return render_template("landing_page.html", page_settings=page_settings)



      



######################################################


''' 
_____________________________________________

Daily (One Day) Corrections
_____________________________________________

'''

# Daily Correction Form & Landing Page
@correction_bp.route('/corrections/daily')
def daily_corrections():

        # Extract query parameters for default values
    selected_form = request.args.get('correction_type', 'daily')  # Default to 'daily'
    ghcn_id = request.args.get('ghcn_id', '')
    correction_date = request.args.get('date', '')
    datzilla_number = request.args.get('datzilla_number', '')
    element = request.args.get('element', '')
    action = request.args.get('action', '')
    o_value = request.args.get('o_value', '')
    e_value = request.args.get('e_value', '')
    begin_date = request.args.get('begin_date', '')
    end_date = request.args.get('end_date', '')

    # Convert dates if needed
    if correction_date:
        correction_date = datetime.strptime(correction_date, '%Y-%m-%d').date()
    if begin_date:
        begin_date = datetime.strptime(begin_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
    # Initialize forms with default data
    daily_form = DailyCorrections(
        ghcn_id=ghcn_id,
        date=correction_date,  
        datzilla_number=datzilla_number,
        element=element,
        action=action,
        o_value=o_value,
        e_value=e_value
    )
    
    return render_template(
        '/corrections/forms/daily_correction_form.html',
        selected_form=selected_form,
        daily_form=daily_form
    )

# Compare Target to Neighboring Stations
@correction_bp.route('/get_data_for_daily_corrections', methods=['POST'])
def process_correction():
    try:
        
        # Extract form data from the POST request
        correction_type = request.form.get('correction_type') 
        # print(f"Correction Type: {correction_type}")
        ghcn_id = request.form.get('ghcn_id')
        correction_date = request.form.get('date')
        element = request.form.get('element')
        action = request.form.get('action')
        o_value = request.form.get('o_value')
        e_value = request.form.get('e_value')
        defaults = request.form.get('defaults') == 'on'  # Convert 'on' string to boolean
        datzilla_number = request.form.get('datzilla_number')
        
        if correction_date:
            correction_year, correction_month, correction_day = correction_date.split('-')
            correction_year = int(correction_year)
            correction_month = int(correction_month)
            correction_day = int(correction_day)
        else:
            correction_year, correction_month, correction_day = None, None, None
        

        base_file_path = '/data/ops/ghcnd/data/'
        stations_list_file_path = base_file_path + 'ghcnd-stations.txt'
        
        # Ensure the file exists
        if not os.path.exists(stations_list_file_path):
            return jsonify({"error": f"Stations file not found: {stations_list_file_path}"}), 500

        stations = parse_station_file(stations_list_file_path)
        
        latitude, longitude, elevation = get_station_lat_long(stations_list_file_path, ghcn_id)
        
        search_radius = 50  #Radius in kilometers

        nearby_stations = find_stations(latitude, longitude, search_radius, stations)
        
        # print("nearby_stations", nearby_stations)
        results = []

        nearby_file_paths = []
        
        # Add file paths for each nearby station
        for station in nearby_stations:
            station_code = station[0]
            station_file_path = base_file_path + 'ghcnd_all/' + station_code + '.dly'
            nearby_file_paths.append((station_code, station_file_path))

        # Iterate through each station_code and file_path in nearby_file_paths
        for nearby_station_codes, nearby_paths in nearby_file_paths:
            #print("Checking: ", nearby_station_codes)

            # Run parser with form data for each station
            filtered_json = parse_and_filter(
                correction_type=correction_type,
                station_code=nearby_station_codes,
                file_path=nearby_paths,
                year=correction_year,
                month=correction_month,
                observation_type=element,
                day=correction_day
            )
            
           # Check if filtered_json has data, if not skip
            if filtered_json.get('status') != "skip":
                
                
                # Find the matching station in nearby_stations
                for station in nearby_stations:
                    
                    if station[0] == nearby_station_codes:  # Match station code
                        
                        filtered_json["elevation"] = station[3]  # Add elevation
                        filtered_json["distance"] = station[4]   # Add distance
                        
                        break  # Exit loop once found
                results.append(filtered_json)
                print('Fileter json here: ', filtered_json)
                print('results  here: ', results)


        # Convert Units
        temp_keys = {"TMAX", "TMIN", "TAVG", "TAXN", "TOBS", "MDTX", "MDTN", "AWBT", "ADPT", "MNPN", "MXPN"}
        tenths_mm_keys = {"PRCP", "EVAP", "WESD", "WESF", "MDEV", "MDPR", "THIC"}
        mm_keys = {"SNOW", "SNWD", "MDSF"}
        wind_keys = {"AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"}
        cm_keys = {"FRGB", "FRGT", "FRTH", "GAHT"}
        km_keys = {"MDWM", "WDMV"}


        if element in temp_keys:
            for entry in results:
                entry['dayMinus'] = c_tenths_to_f(entry['dayMinus'])
                entry['dayPlus'] = c_tenths_to_f(entry['dayPlus'])
                entry['day'] = c_tenths_to_f(entry['day'])
        elif element in tenths_mm_keys:
            for entry in results:
                entry['dayMinus'] = tenths_mm_to_inches(entry['dayMinus'])
                entry['dayPlus'] = tenths_mm_to_inches(entry['dayPlus'])
                entry['day'] = tenths_mm_to_inches(entry['day'])
        elif element in mm_keys:
            for entry in results:
                entry['dayMinus'] = mm_to_inches(entry['dayMinus'])
                entry['dayPlus'] = mm_to_inches(entry['dayPlus'])
                entry['day'] = mm_to_inches(entry['day'])
        elif element in wind_keys:
            for entry in results:
                entry['dayMinus'] = wind_tenths_to_mph(entry['dayMinus'])
                entry['dayPlus'] = wind_tenths_to_mph(entry['dayPlus'])
                entry['day'] = wind_tenths_to_mph(entry['day'])
        elif element in cm_keys:
            for entry in results:
                entry['dayMinus'] = cm_tenths_to_inches(entry['dayMinus'])
                entry['dayPlus'] = cm_tenths_to_inches(entry['dayPlus'])
                entry['day'] = cm_tenths_to_inches(entry['day'])
        elif element in km_keys:
            for entry in results:
                entry['dayMinus'] = km_to_miles(entry['dayMinus'])
                entry['dayPlus'] = km_to_miles(entry['dayPlus'])
                entry['day'] = km_to_miles(entry['day'])
        #Km to Miles
        for entry in results:
                entry['distance'] = km_to_miles(entry['distance'])

        # Return
        return jsonify({
            "message": f"Correction processed successfully for GHCN ID: {ghcn_id}!",
            "returnData": results
        }), 201         
    except Exception as e:
        print(f"Error in process_correction: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Auto-populate O-Value when Form input is given    
@correction_bp.route('/get_o_value', methods=['POST'])
def get_o_value():

    try:
        # Extract the data sent in the request body
        ghcn_id = request.form.get('ghcn_id')
        correction_date = request.form.get('correction_date')
        element = request.form.get('element')


        if correction_date:
            correction_year, correction_month, correction_day = correction_date.split('-')
            correction_year = int(correction_year)
            correction_month = int(correction_month)
            correction_day = int(correction_day)
        else:
            correction_year, correction_month, correction_day = None, None, None
            
        
        base_file_path = '/data/ops/ghcnd/data/'
        station_file_path = base_file_path + 'ghcnd_all/' + ghcn_id + '.dly'
        # print(f"Station file path: {station_file_path}")
        # print(f"correction_year: {correction_year}")
        # print(f"correction_month: {correction_month}")
        # print(f"correction_day: {correction_day}")

        # Run parser with form data for each station
        filtered_json = parse_and_filter(
            correction_type = "o_value",
            file_path=station_file_path,
            station_code=ghcn_id,
            year=correction_year,
            month=correction_month,
            observation_type=element,
            day=correction_day,
        )
        
        print("filtered_json", filtered_json)
        
        # Check if 'status' exists in filtered_json and is 'skip'
        if 'status' in filtered_json and filtered_json['status'] == 'skip':
            return jsonify({
                "o_value": "No Value",
                "o_flag_value": "   "
            })
        temp_keys = {"TMAX", "TMIN", "TAVG", "TAXN", "TOBS", "MDTX", "MDTN", "AWBT", "ADPT", "MNPN", "MXPN"}
        tenths_mm_keys = {"PRCP", "EVAP", "WESD", "WESF", "MDEV", "MDPR", "THIC"}
        mm_keys = {"SNOW", "SNWD", "MDSF"}
        wind_keys = {"AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"}
        cm_keys = {"FRGB", "FRGT", "FRTH", "GAHT"}
        km_keys = {"MDWM", "WDMV"}
        
        ovalue=''
        if element in temp_keys :
            ovalue = c_tenths_to_f(filtered_json[0])
        elif element in mm_keys:
            ovalue = mm_to_inches(filtered_json[0])
        elif element in tenths_mm_keys:
            ovalue = tenths_mm_to_inches(filtered_json[0])
        elif element in wind_keys:
            ovalue = wind_tenths_to_mph(filtered_json[0])
        elif element in km_keys: 
            ovalue = km_to_miles(filtered_json[0])
        elif element in cm_keys:
            ovalue = cm_to_inches(filtered_json[0])

        return jsonify({
            
            "o_value": ovalue,
            "o_flag_value": filtered_json[1]
            })
                
               

        
    except Exception as e:
        print(f"Error in get_o_value: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
# Save Daily Correction to File
@correction_bp.route('/submit_daily_corrections', methods=['POST'])
def submit_daily_corrections():
    try:


        data = request.get_json()  # Get the parsed JSON data
        # Extract the list of corrections from the JSON data & Parse Query string to Dict
        correction_data = parse_qs(data.get('correction_data'), keep_blank_values=True)

        # Print out the correction data for logging
        print(correction_data)
        correction_list = ['ghcn_id', 'date', 'element','action', 'o_value', 'e_value', 'datzilla_number']

        # Process each correction entry
        for key in correction_list:
            if key not in correction_data:
                correction_data[key] =''

        correction_date = correction_data['date'][0]


        if correction_date:
            correction_year, correction_month, correction_day = correction_date.split('-')
        else:
            correction_year, correction_month, correction_day = None, None, None

        if correction_date:
            date_obj = datetime.strptime(correction_date, "%Y-%m-%d")
            yyyymm = date_obj.strftime("%Y%m")
            dd = date_obj.strftime("%d")
        else:
            yyyymm, dd = "", ""

        # Get today's date in yyyymmdd format
        todays_date = datetime.today().strftime("%Y%m%d")

        # Prepare the line for the text file
        line = f"{correction_data['ghcn_id'][0]}, {yyyymm}, {dd}, {correction_data['element'][0]}, {correction_data['action'][0]}, {correction_data['o_value'][0]}, XX, XX, XX, {correction_data['e_value'][0]}, XX, XX, {todays_date}, {correction_data['datzilla_number'][0]}, XX\n"

        # Append to the text file
        with open("/data/ops/ghcndqi/corr/corrections.txt", "a") as file:
            file.write(line)
        flash('Correction successfully written')
        print("Correction successfully written to daily_corrections.txt")

        return jsonify({"message": "Daily correction submitted successfully!"}), 201

    except Exception as e:
        print(f"Error in submit_daily_corrections: {e}")
        return jsonify({"error": "Internal server error"}), 500

'''
_____________________________________________

End Daily (One Day) Corrections
_____________________________________________


'''

######################################################

''' 
_____________________________________________

Monthly Corrections
_____________________________________________

'''

@correction_bp.route('/corrections/monthly')
def monthly_corrections():
    # Extract query parameters for default values
    selected_form = request.args.get('correction_type', 'daily')  # Default to 'daily'
    ghcn_id = request.args.get('ghcn_id', '')
    correction_date = request.args.get('date', '')
    element = request.args.get('element', '')
    action = request.args.get('action', '')
    o_value = request.args.get('o_value', '')
    e_value = request.args.get('e_value', '')
    begin_date = request.args.get('begin_date', '')
    end_date = request.args.get('end_date', '')
    datzilla_number = request.args.get('datzilla_number', '')

    
    # Convert dates if needed
    if correction_date:
        correction_date = datetime.strptime(correction_date, '%Y-%m-%d').date()
    if begin_date:
        begin_date = datetime.strptime(begin_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
    # Initialize forms with default data
    monthly_form = MonthlyCorrections(
        ghcn_id=ghcn_id,
        date=correction_date,  
    )
    

    return render_template(
        '/corrections/forms/monthly_correction_form.html',
        selected_form=selected_form,
        monthly_form=monthly_form
    )

# Save Monthly Correction to File

@correction_bp.route('/submit_monthly_corrections', methods=['POST'])
def submit_monthly_corrections():
    try:  
        data = request.get_json()  # Get the parsed JSON data
        formData = parse_qs(data.get('form_input'), keep_blank_values=True)
        monthlyInputData = data.get('monthly_input')
        print(formData)
        print(monthlyInputData)
        ghcn_id = formData.get('ghcn_id')
        ghcn_id = ghcn_id[0]
        
        correction_date = formData.get('date')
        print(correction_date)
        element = formData.get('element')
        element = element[0]
        action = formData.get('action')
        action = action[0]
        datzilla_number = formData.get('datzilla_number')
        print(ghcn_id, action, datzilla_number, element)
        if datzilla_number != '':
            datzilla_number = datzilla_number[0]
            print(datzilla_number)
        else:
            datzilla_number = 'null'

        # Parse the correction date into year, month, and day
        if correction_date:
            correction_year = correction_date[0:3]
            correction_month = correction_date[4:5]
            yyyymm = correction_date[0]
        else:
            correction_year, correction_month = None, None, None
            yyyymm = ""

        # if correction_date:
        #     date_obj = datetime.strptime(correction_date[0], "%Y-%m-%d")
        #     yyyymm = date_obj.strftime("%Y%m")

        # else:
        #     yyyymm = ""

        # Get today's date in yyyymmdd format
        todays_date = datetime.today().strftime("%Y%m%d")
            

        day = 0
        for entry in monthlyInputData:
            if entry != '':
                # Extract relevant fields for each record           
                o_value = 'placeholder'
                e_value = monthlyInputData[day]            
                day += 1

                
                # Prepare the line for the text file
                line = f"{ghcn_id}, {yyyymm}, {day}, {element}, {action}, {o_value}, XX, XX, XX, {e_value}, XX, XX, {todays_date}, {datzilla_number}, XX\n"

                # Append to the text file for each correction
                with open("/data/ops/ghcndqi/corr/corrections.txt", "a") as file:
                    file.write(line)

                print(f"Correction for {ghcn_id} on {correction_date} successfully written to corrections.txt")
            else:
                day += 1

        # Respond with success
        return jsonify({"message": f"{len(monthlyInputData)} corrections submitted successfully!"}), 201



    except Exception as e:
        print(f"Error in submit_monthly_corrections: {e}")
        return jsonify({"error": "Internal server error"}), 500
''' 
_____________________________________________

End Monthly Corrections
_____________________________________________

'''

######################################################

''' 
_____________________________________________

Invalidate Range Corrections Section
_____________________________________________

'''

#Invalidate Range Form & Landing Page
@correction_bp.route('/corrections/range')
def range_corrections():

    # Extract query parameters for default values
    selected_form = request.args.get('correction_type', 'range')  # Default to 'daily'
    ghcn_id = request.args.get('ghcn_id', '')
    datzilla_number = request.args.get('datzilla_number', '')
    element = request.args.get('element', '')
    action = request.args.get('action', '')
    # o_value = request.args.get('o_value', '')
    # e_value = request.args.get('e_value', '')
    begin_date = request.args.get('begin_date', '')
    end_date = request.args.get('end_date', '')

    # Convert dates
    if begin_date:
        begin_date = datetime.strptime(begin_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
    # Initialize forms with default data    
    range_form = RangeCorrections(
        ghcn_id=ghcn_id,
        begin_date=begin_date,
        end_date=end_date,
        datzilla_number=datzilla_number,
        element=element,
        action=action
    )


    return render_template(
        '/corrections/forms/range_correction_form.html',
        selected_form=selected_form,
        range_form=range_form
    )



# Render Invalidated Range Table (Post Form Submittal)
@correction_bp.route('/get_ranged_values', methods=['POST'])
def get_ranged_values():
    try:
        
        
        # Extract the data sent in the request body
        selected_form = request.form.get('correction_type', 'range')  # Default to 'daily'
        ghcn_id = request.form.get('ghcn_id', '')
        datzilla_number = request.form.get('datzilla_number', '')
        element = request.form.get('element', '')
        action = request.form.get('action', '')
        # o_value = request.form.get('o_value', '')
        # e_value = request.form.get('e_value', '')
        #print(request.form.get('date'))
        #begin_date = request.form.get('begin_date', '')
        #end_date = request.form.get('end_date', '')
        #date = request.form.get('date')
        date = request.form.get('date', '')
        begin_date = date[0:10]
        end_date = date[13:23]
        print(begin_date, end_date)

        # Convert dates
        # if begin_date:
        #     begin_date = datetime.strptime(begin_date, '%Y-%m-%d').date()
        # if end_date:
        #     end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if begin_date:
            begin_date = datetime.strptime(begin_date, '%m-%d-%Y').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%m-%d-%Y').date()


       
        
        base_file_path = '/data/ops/ghcnd/data/'
        station_file_path = base_file_path + 'ghcnd_all/' + ghcn_id + '.dly'

        # Run parser with form data for each station
        filtered_json = parse_and_filter(
            file_path= station_file_path,
            correction_type="range",
            begin_date = begin_date,
            end_date = end_date,
            observation_type=element,
            station_code=ghcn_id,
            
        )
        
        # Check if 'status' exists in filtered_json and is 'skip'
        if 'status' in filtered_json and filtered_json['status'] == 'skip':
            print("SKIPPING")

            return jsonify({
                "o_value": "No Value",
            })
            
            
          # Process data
        station_data = {
            "stationID": ghcn_id,
            "element": element,
            "action": action,
            "datzilla_number": datzilla_number
        }

        days_data = []
        # print("filtered json:" , filtered_json)
        has_value_key = any('Value' in d for d in filtered_json)
        # print("has_value_key: " , has_value_key)
        if not has_value_key:
            error_str = f"Error in get_ranged_values: No data for Element {element}"
            return jsonify({"error": f"Internal server error {error_str}"}), 500
        for entry in filtered_json:
            # print("Entry: ", entry)
            # Extract year, month, and day from the 'Date' field
            full_date = datetime.strptime(entry['Date'], '%Y-%m-%d').date()
            year, month, day = full_date.year, full_date.month, full_date.day
            
                    
            # Filter by date range
            if begin_date <= full_date <= end_date:
                days_data.append({
                    'date': full_date.strftime('%Y-%m-%d'),
                    'value': entry.get('Value', "N/A")
                })
                        
        response_data = {
            "stationData": station_data,
            "days_data": days_data
        }

        # Return a simple response with the data
        return response_data
    
    except Exception as e:
        print(f"Error in get_ranged_values: {e} {traceback.format_exc()}")
        return jsonify({"error": f"Internal server error {traceback.format_exc()}"}), 500
    

@correction_bp.route('/submit_ranged_corrections', methods=['POST'])
def submit_ranged_corrections():

    try:
        # Extract JSON data from the request body
        data = request.get_json()  # Get the parsed JSON data

        # Extract the list of corrections from the JSON data
        correction_data = data.get('correction_data', [])

        # Process each correction entry
        for correction in correction_data:
            # Extract relevant fields for each record
            ghcn_id = correction.get('stationID')
            correction_date = correction.get('date')
            element = correction.get('element')
            action = correction.get('action')
            o_value = correction.get('o_value')
            e_value = correction.get('e_value')
            datzilla_number = correction.get('datzilla_number')

            # Parse the correction date into year, month, and day
            if correction_date:
                correction_year, correction_month, correction_day = correction_date.split('-')
            else:
                correction_year, correction_month, correction_day = None, None, None

            if correction_date:
                date_obj = datetime.strptime(correction_date, "%Y-%m-%d")
                yyyymm = date_obj.strftime("%Y%m")
                dd = date_obj.strftime("%d")
            else:
                yyyymm, dd = "", ""

            # Get today's date in yyyymmdd format
            todays_date = datetime.today().strftime("%Y%m%d")
            
            # Prepare the line for the text file
            line = f"{ghcn_id}, {yyyymm}, {dd}, {element}, {action}, {o_value}, XX, XX, XX, {e_value}, XX, XX, {todays_date}, {datzilla_number}, XX\n"

            # Append to the text file for each correction
            with open("/data/ops/ghcndqi/corr/corrections.txt", "a") as file:
                file.write(line)

            print(f"Correction for {ghcn_id} on {correction_date} successfully written to corrections.txt")

        # Respond with success
        return jsonify({"message": f"{len(correction_data)} corrections submitted successfully!"}), 201

    except Exception as e:
        print(f"Error in submit_ranged_corrections: {e}")
        return jsonify({"error": "Internal server error"}), 500

''' 
_____________________________________________

End Invalidate Range Corrections Section
_____________________________________________

'''

######################################################

''' 
_____________________________________________

Hourly Corrections Section
_____________________________________________

'''
@correction_bp.route('/corrections/hourly')
def hourly_corrections():

    # Extract query parameters for default values
    selected_form = request.args.get('correction_type', 'daily')  # Default to 'daily'
    ghcn_id = request.args.get('ghcn_id', '')
    correction_date = request.args.get('date', '')
    datzilla_number = request.args.get('datzilla_number', '')
    element = request.args.get('element', '')
    action = request.args.get('action', '')
    o_value = request.args.get('o_value', '')
    e_value = request.args.get('e_value', '')
    begin_date = request.args.get('begin_date', '')
    end_date = request.args.get('end_date', '')

    # Convert dates if needed
    if correction_date:
        correction_date = datetime.strptime(correction_date, '%Y-%m-%d').date()
    if begin_date:
        begin_date = datetime.strptime(begin_date, '%Y-%m-%d').date()
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Read the GHCN Hourly header list from file
    data = []
    with open("GHCNh_psv_column_names.txt") as load_file:
        # temp_list = []
        for line in load_file:
            temp_list = ['']
            line = line.split()
            # print(temp_list + line)
            data.append(tuple(line + line))

    # Initialize forms with default data    
    hourly_form = HourlyCorrections(
        ghcn_id=ghcn_id,
        date=correction_date,  
        datzilla_number=datzilla_number
        # element=element

    )

    hourly_form.element.choices = data


    return render_template(
        '/corrections/forms/hourly_correction_form.html',
        selected_form=selected_form,
        hourly_form=hourly_form
    )

''' 
_____________________________________________

End Hourly Corrections Section
_____________________________________________

'''
######################################################


''' 
_____________________________________________

Show Previous Corrections Section
_____________________________________________

'''
@correction_bp.route('/corrections/previous',  methods=['GET','POST'])
def previous_corrections():

    return render_template(
        "/corrections/previous_corrections.html",
    )


# @correction_bp.route('/corrections/previous/get',  methods=['GET','POST']) #If the username needs to be hidden, POST can be used instead.
@correction_bp.route('/corrections/previous/get',  methods=['GET'])
def get_previous_corrections():

    data = []
    folder_path = '/data/ops/ghcndqi/corr/'
    file_name = 'corrections.txt'
    with open( os.path.join(folder_path, file_name), "r") as file:
        for line in file: 
            if line:
                print(line)

                records = [entry.strip() for entry in line[:-1].split(",") if 'XX' not in entry] # Lines end in \n
                records[1] = records[1] + records.pop(2) # Combine YYYYMM + DD
                records.insert(0, "User") # Placeholder for Username   

                for col in [2,7]: # Reformat dates to YYYY-MM-DD for display only
                    try:
                        records[col] = datetime.strptime(records[col], "%Y%m%d").date().strftime('%Y-%m-%d')
                    except Exception as err:
                        print(
                            f"Error reading Date" +
                            f"{traceback.format_exc()}"
                            )
                        records[col] = ""
                    pass

                # print(records)
                data.append(records)
    
    # print(data)
    return data
