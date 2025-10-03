from app.corrections import correction_bp
from urllib.parse import urlparse, parse_qs, parse_qsl
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections, HourlyCorrections, MultiDayCorrections
from flask import render_template, request, jsonify, flash
from app.extensions import get_db, find_stations, parse_station_file, get_station_lat_long, find_nearest_station
from app.dataingest.readandfilterGHCN import parse_and_filter
from app.corrections.models.corrections import Corrections
from app.ghcndata.routes import mm_to_inches, tenths_mm_to_inches, c_tenths_to_f, cm_tenths_to_inches, km_to_miles, wind_tenths_to_mph, cm_to_inches
from datetime import date, datetime, timedelta
import os
from flask_login import login_required, current_user
from flask import session
import traceback
from app.utilities.JSON_DataStore import JSON_DataStore as js_ds


file_path = os.path.join(os.getcwd(), 'USW00093991.dly')

# All Corrections Landing Page
@correction_bp.route('/')
@correction_bp.route('/corrections')
@login_required
def index():
    user = current_user.username
    print(user)
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def submit_daily_corrections():
    try:
        user = current_user.username

        data = request.get_json()  # Get the parsed JSON data
        # Extract the list of corrections from the JSON data & Parse Query string to Dict
        correction_data = parse_qs(data.get('correction_data'), keep_blank_values=True)
        ghcn_id = correction_data['ghcn_id'][0]
        element = correction_data['element'][0]
        correction_date = correction_data['date'][0]
        action = correction_data['action'][0]
        o_value = correction_data['o_value']
        e_value = correction_data['e_value'][0]
        eflag = correction_data['eflag'][0]
        source = correction_data['source'][0]
        datzilla_number = correction_data['datzilla_number'][0]
        # Print out the correction data for logging
        print(correction_data)

        correction_list = ['ghcn_id', 'date', 'element','action', 'o_value', 'e_value', 'eflag', 'source', 'datzilla_number']
        # Process each correction entry
        for key in correction_list:
            if key not in correction_data:
                correction_data[key] =''

        
        if datzilla_number == '':
            datzilla_number = 'None'

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
        
        # Process for retrieving the O-Value and associated Flags
        GhcnDataID = [
            ghcn_id,
            int(correction_year),
            int(correction_month),
            element,
        ]

        o_value_data = get_oval(GhcnDataID, dd)
        o_value = o_value_data[0]
        mflag = o_value_data[1]
        qflag = o_value_data[2]
        sflag = o_value_data[3]
        print('mflag:', mflag,'end')
        print('qflag:', qflag,'end')
        print('sflag:', sflag,'end')
       
       
       # Account For Missing Values
        if mflag == ' ':
            mflag = 'None'

        if qflag == ' ':
            qflag = 'None'

        if sflag == ' ':
            sflag = 'None'
        if source == '':
            source = 'None'

        # Get today's date in yyyymmdd format
        todays_date = datetime.today().strftime("%Y%m%d")

        # Prepare the line for the text file
        line = f"{ghcn_id}, {yyyymm}, {dd}, {element}, {action}, {o_value}, {mflag}, {qflag}, {sflag}, {e_value}, {eflag}, {source}, {todays_date}, {datzilla_number}, 0\n"

        # Append to the text file
        with open(f'/data/ops/ghcndqi/corr/{user}corrections.txt', "a") as file:
            file.write(line)
        flash('Correction successfully written', 'success')
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
@login_required
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
@login_required
def submit_monthly_corrections():
    try:  
        user = current_user.username
        data = request.get_json()  # Get the parsed JSON data
        formData = parse_qs(data.get('form_input'), keep_blank_values=True)
        monthlyInputData = data.get('monthly_input')
        print(monthlyInputData)
        ghcn_id = formData.get('ghcn_id')
        ghcn_id = ghcn_id[0]
        
        correction_date = formData.get('date')
        correction_date = correction_date[0]
        print(correction_date)
        element = formData.get('element')
        element = element[0]
        action = formData.get('action')
        action = action[0]
        datzilla_number = formData.get('datzilla_number') 
        datzilla_number = datzilla_number[0]
        source = ''
        eflag=''

        if datzilla_number == '':
            datzilla_number = 'None'
        
        
        # Parse the correction date into year, month, and day
        if correction_date:
            correction_year = correction_date[0:4]
            correction_month = correction_date[4:6]
            yyyymm = correction_date
        else:
            correction_year, correction_month = None, None, None
            yyyymm = ""

        # Process for retrieving the O-Value and associated Flags
        GhcnDataID = [
            ghcn_id,
            int(correction_year),
            int(correction_month),
            element,
        ]

        # Get today's date in yyyymmdd format
        todays_date = datetime.today().strftime("%Y%m%d")
        

        day = 0
        for entry in monthlyInputData:
            if entry != '':
                # Extract relevant fields for each record           
                
                e_value = monthlyInputData[day]            
                day += 1
                o_value_data = get_oval(GhcnDataID, day)
                print(o_value_data)
                o_value = o_value_data[0]
                mflag = o_value_data[1]
                qflag = o_value_data[2]
                sflag = o_value_data[3]
                print('mflag:', mflag,'end')
                print('qflag:', qflag,'end')
                print('sflag:', sflag,'end')
                
                # Account For missing fields
                if mflag == ' ':
                    mflag = 'None'

                if qflag == ' ':
                    qflag = 'None'

                if sflag == ' ':
                    sflag = 'None'
                if source == '':
                    source = 'None'  
                if eflag == '':
                    eflag = 'None'

                if (day <= 9):
                    correction_day = f'0{day}'
                else:
                    correction_day = day
                
                # Prepare the line for the text file
                line = f"{ghcn_id}, {yyyymm}, {correction_day}, {element}, {action}, {o_value}, {mflag}, {qflag}, {sflag}, {e_value}, {eflag}, {source}, {todays_date}, {datzilla_number}, 0\n"


                # Append to the text file for each correction
                with open(f'/data/ops/ghcndqi/corr/{user}corrections.txt', "a") as file:
                    file.write(line)

                print(f"Correction for {ghcn_id} on {correction_date} successfully written to corrections.txt")
            else:
                day += 1

        # Respond with success
        return jsonify({"message": f"{len(monthlyInputData)} corrections submitted successfully!"}), 201



    except Exception as e:
        print(f"Error in submit_monthly_corrections: {e}")
        return jsonify({"error": "Internal server error"}), 500


def get_oval(correctionData, day):
        try:
            base_file_path = '/data/ops/ghcnd/data/'
            station_file_path = base_file_path + 'ghcnd_all/' + correctionData[0] + '.dly'

            # Run parser with form data for each station
            filtered_json = parse_and_filter(
                correction_type = "o_value",
                file_path=station_file_path,
                station_code=correctionData[0],
                year=correctionData[1],
                month=correctionData[2],
                observation_type=correctionData[3],
                day=day,
            )
            
            print("filtered_json", filtered_json)
            
            # Check if 'status' exists in filtered_json and is 'skip'
            if 'status' in filtered_json and filtered_json['status'] == 'skip':
                return jsonify({
                    "o_value": "No Value",
                    "o_flag_value": "   "
                })
            
            flags = filtered_json[1]
            print(flags)
            mflag = flags[0]
            qflag = flags[1]
            sflag = flags[2]
            

            element = correctionData[3]
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
            print("This is the o valu ", ovalue)   
            o_valueData = [ovalue, mflag, qflag, sflag]
            return o_valueData
        except Exception as e:
                print(f"Error in get_o_value: {e}")
                return jsonify({"error": "Internal server error"}), 500


''' 
_____________________________________________

End Monthly Corrections
_____________________________________________

'''

######################################################

''' 
_____________________________________________

Multi-Day Corrections
_____________________________________________

'''

# Multi-Day Correction Form & Landing Page
@correction_bp.route('/corrections/multiday')
@login_required
def multiday_corrections():
    # Extract query parameters for default values
    selected_form = request.args.get('correction_type', 'multiday')
    ghcn_id = request.args.get('ghcn_id', '')
    begin_date = request.args.get('begin_date', '')
    end_date = request.args.get('end_date', '')
    elements = request.args.get('elements', '')
    action = request.args.get('action', '')
    datzilla_number = request.args.get('datzilla_number', '')
    
    # Create form with default values
    multiday_form = MultiDayCorrections(
        form_type=selected_form,
        ghcn_id=ghcn_id,
        begin_date=begin_date,
        end_date=end_date,
        datzilla_number=datzilla_number,
    )
    
    return render_template(
        '/corrections/forms/multi_day_correction_form.html',
        selected_form=selected_form,
        multiday_form=multiday_form
    )

# Save Multi-Day Correction to File
@correction_bp.route('/submit_multiday_corrections', methods=['POST'])
@login_required
def submit_multiday_corrections():
    try:  
        user = current_user.username
        data = request.get_json()
        correction_entries = data.get('correction_entries', [])
        entry_count = data.get('entry_count', 0)
        
        print("Correction Entries:", correction_entries)
        print("Entry Count:", entry_count)
        
        if not correction_entries or len(correction_entries) == 0:
            return jsonify({"error": "No correction entries provided"}), 400
        
        corrections_written = 0
        
        for entry in correction_entries:
            try:
                # Validate required fields
                if not all([entry.get('ghcn_id'), entry.get('date'), entry.get('element'), entry.get('action')]):
                    print(f"Skipping incomplete entry: {entry}")
                    continue
                
                # Parse the date
                correction_date = datetime.strptime(entry.get('date'), '%Y-%m-%d').date()
                
                # Get O-value using the existing get_o_value route logic
                o_value = ''
                try:
                    # Create a mock request to get_o_value function
                    from flask import g
                    
                    ghcn_id = entry.get('ghcn_id')
                    correction_date_str = entry.get('date')
                    element = entry.get('element')
                    
                    # Use the same logic as get_o_value route
                    correction_year, correction_month, correction_day = correction_date_str.split('-')
                    correction_year = int(correction_year)
                    correction_month = int(correction_month)
                    correction_day = int(correction_day)
                    
                    base_file_path = '/data/ops/ghcnd/data/'
                    station_file_path = base_file_path + 'ghcnd_all/' + ghcn_id + '.dly'
                    
                    # Run parser with form data for each station
                    filtered_json = parse_and_filter(
                        correction_type="o_value",
                        file_path=station_file_path,
                        station_code=ghcn_id,
                        year=correction_year,
                        month=correction_month,
                        observation_type=element,
                        day=correction_day,
                    )
                    
                    print(f"O-value filtered_json for {ghcn_id}: {filtered_json}")
                    
                    # Check if 'status' exists in filtered_json and is 'skip'
                    if 'status' in filtered_json and filtered_json['status'] == 'skip':
                        o_value = "No Value"
                    else:
                        # Apply unit conversions based on element type
                        temp_keys = {"TMAX", "TMIN", "TAVG", "TAXN", "TOBS", "MDTX", "MDTN", "AWBT", "ADPT", "MNPN", "MXPN"}
                        tenths_mm_keys = {"PRCP", "EVAP", "WESD", "WESF", "MDEV", "MDPR", "THIC"}
                        mm_keys = {"SNOW", "SNWD", "MDSF"}
                        wind_keys = {"AWND", "WSF1", "WSF2", "WSF5", "WSFG", "WSFI", "WSFM"}
                        cm_keys = {"FRGB", "FRGT", "FRTH", "GAHT"}
                        km_keys = {"MDWM", "WDMV"}
                        
                        if element in temp_keys:
                            o_value = c_tenths_to_f(filtered_json[0])
                        elif element in mm_keys:
                            o_value = mm_to_inches(filtered_json[0])
                        elif element in tenths_mm_keys:
                            o_value = tenths_mm_to_inches(filtered_json[0])
                        elif element in wind_keys:
                            o_value = wind_tenths_to_mph(filtered_json[0])
                        elif element in km_keys:
                            o_value = km_to_miles(filtered_json[0])
                        elif element in cm_keys:
                            o_value = cm_to_inches(filtered_json[0])
                        else:
                            o_value = str(filtered_json[0]) if filtered_json[0] is not None else "No Value"
                    
                    print(f"Retrieved O-value for {ghcn_id} {element} on {correction_date_str}: {o_value}")
                    
                except Exception as o_value_error:
                    print(f"Error retrieving O-value for entry {entry}: {o_value_error}")
                    o_value = "Error retrieving O-value"
                
                # Handle element value - use sub-element if provided for specific element types
                element_value = entry.get('element')
                if entry.get('sub_element') and element_value in ["SN*#", "SX*#", "WT**", "WV**"]:
                    element_value = entry.get('sub_element')
                    print(f"Using sub-element value: {element_value} instead of {entry.get('element')}")
                
                # Create correction entry with retrieved O-value
                correction = Corrections(
                    ghcn_id=entry.get('ghcn_id'),
                    correction_date=correction_date,
                    element=element_value,  # Use sub-element if applicable
                    action=entry.get('action'),
                    e_value=entry.get('e_value', ''),  
                    o_value=o_value,  # Now using retrieved O-value
                    datzilla_number=entry.get('datzilla_number', '')
                )
                
                # Save to database
                if correction.save_to_db():
                    corrections_written += 1
                    print(f"Correction for {entry.get('ghcn_id')} on {correction_date} for element {entry.get('element')} successfully written")
                else:
                    print(f"Failed to save correction for {entry.get('ghcn_id')} on {correction_date}")
                    
            except Exception as entry_error:
                print(f"Error processing entry {entry}: {entry_error}")
                continue
        
        if corrections_written == 0:
            return jsonify({"error": "No valid corrections were processed"}), 400
        
        # Respond with success
        return jsonify({
            "message": f"{corrections_written} correction entries submitted successfully!",
            "processed": corrections_written,
            "total": len(correction_entries)
        }), 201

    except Exception as e:
        print(f"Error in submit_multiday_corrections: {e}")
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

'''
_____________________________________________

End Multi-Day Corrections
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
@login_required
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
@login_required
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
@login_required
def submit_ranged_corrections():

    try:
        user = current_user.username
        # Extract JSON data from the request body
        data = request.get_json()  # Get the parsed JSON data

        # Extract the list of corrections from the JSON data
        correction_data = data.get('correction_data', [])
        source = ''
        eflag=''

        # Process each correction entry
        for correction in correction_data:
            # Extract relevant fields for each record
            ghcn_id = correction.get('stationID')
            correction_date = correction.get('date')
            element = correction.get('element')
            action = correction.get('action')
            o_value = correction.get('value')
            e_value = correction.get('newValue')
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

            # Process for retrieving the O-Value and associated Flags
            GhcnDataID = [
            ghcn_id,
            int(correction_year),
            int(correction_month),
            element,
        ]
            
            

            o_value_data = get_oval(GhcnDataID, int(dd))
            print("here's the o-value data", o_value_data)
            o_value = o_value_data[0]
            mflag = o_value_data[1]
            qflag = o_value_data[2]
            sflag = o_value_data[3]
            print('mflag:', mflag,'end')
            print('qflag:', qflag,'end')
            print('sflag:', sflag,'end')
            
            # Account for missing Fields
            if mflag == ' ':
                mflag = 'None'
            if qflag == ' ':
                qflag = 'None'
            if sflag == ' ':
                sflag = 'None'
            if source == '':
                source = 'None'
            if eflag == '':
                eflag = 'None'

            # Get today's date in yyyymmdd format
            todays_date = datetime.today().strftime("%Y%m%d")
            
            # Prepare the line for the text file
            line = f"{ghcn_id}, {yyyymm}, {dd}, {element}, {action}, {o_value}, {mflag}, {qflag}, {sflag}, {e_value}, {eflag}, {source}, {todays_date}, {datzilla_number}, 0\n"


            # Append to the text file for each correction
            with open(f'/data/ops/ghcndqi/corr/{user}corrections.txt', "a") as file:
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
@login_required
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
@login_required
def previous_corrections():
    """Display previous corrections page with user correction data"""
    try:
        # Get all previous corrections data
        corrections_data = get_previous_corrections()
        
        return render_template(
            "/corrections/previous_corrections.html",
            corrections_data=corrections_data
        )
    except Exception as e:
        print(f"Error loading previous corrections: {e}")
        flash('Error loading previous corrections. Please try again.', 'error')
        return render_template(
            "/corrections/previous_corrections.html",
            corrections_data=[]
        )


# @correction_bp.route('/corrections/previous/get',  methods=['GET','POST']) #If the username needs to be hidden, POST can be used instead.
@correction_bp.route('/corrections/previous/get',  methods=['GET'])
@login_required
def get_previous_corrections():
    """
    Read correction files from file system for all users and return formatted data.
    
    Returns:
        list: List of correction records with format:
              [username, correction_date, begin_date, end_date, ghcn_id, element, action, o_value, e_value, datzilla_number]
    """
    all_corrections_data = []
    
    try:
        # Get admin settings to determine the corrections folder path
        js = js_ds()
        admin_settings = js.get_admin_settings()
        
        # Use configured path or default path
        corrections_folder = admin_settings.get('corrections_path', '/data/ops/ghcndqi/corr/')
        
        # Get all users from the datastore
        all_users = js.get_users()
        
        print(f"Looking for correction files in: {corrections_folder}")
        
        # Process each user's correction file
        for user_data in all_users:
            username = user_data.get('username', '')
            if not username:
                continue
                
            # Construct the correction file path for this user
            correction_file_path = os.path.join(corrections_folder, f'{username}corrections.txt')
            
            print(f"Checking correction file for user {username}: {correction_file_path}")
            
            # Check if the correction file exists for this user
            if os.path.exists(correction_file_path):
                try:
                    user_corrections = _read_user_correction_file(correction_file_path, username)
                    all_corrections_data.extend(user_corrections)
                    print(f"Successfully loaded {len(user_corrections)} corrections for user {username}")
                except Exception as file_error:
                    print(f"Error reading correction file for user {username}: {file_error}")
                    print(f"Traceback: {traceback.format_exc()}")
                    continue
            else:
                print(f"No correction file found for user {username} at {correction_file_path}")
        
        # Sort corrections by correction date (most recent first)
        all_corrections_data.sort(key=lambda x: x[1] if x[1] else '', reverse=True)
        
        print(f"Total corrections loaded: {len(all_corrections_data)}")
        return all_corrections_data
        
    except Exception as e:
        print(f"Error in get_previous_corrections: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return []


def _read_user_correction_file(file_path, username):
    """
    Read and parse a single user's correction file.
    
    Args:
        file_path (str): Path to the user's correction file
        username (str): Username for the corrections
        
    Returns:
        list: List of parsed correction records for this user
    """
    user_corrections = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, 1):
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                try:
                    # Parse the correction line
                    correction_record = _parse_correction_line(line, username)
                    if correction_record:
                        user_corrections.append(correction_record)
                        
                except Exception as parse_error:
                    print(f"Error parsing line {line_number} in {file_path}: {parse_error}")
                    print(f"Problematic line: {line}")
                    continue
                    
    except FileNotFoundError:
        print(f"Correction file not found: {file_path}")
    except PermissionError:
        print(f"Permission denied reading file: {file_path}")
    except Exception as e:
        print(f"Unexpected error reading file {file_path}: {e}")
        
    return user_corrections


def _parse_correction_line(line, username):
    """
    Parse a single correction line from a user's correction file.
    
    Expected format: GHCNID,YYYYMM,DD,ELEMENT,ACTION,O_VALUE,E_VALUE, E_FLAG, SOURCE,YYYYMMDD,DATZILLA_NUMBER
    
    Args:
        line (str): Raw line from correction file
        username (str): Username for this correction
        
    Returns:
        list: Parsed correction record or None if parsing fails
    """
    try:
        # Split the line by commas and clean up entries
        parts = [entry.strip() for entry in line.split(',') if entry.strip()]
        print(parts)
        # Filter out entries containing 'XX' (invalid data markers)
        parts = [entry for entry in parts if 'XX' not in entry.upper()]
        
        # Ensure we have enough parts for a valid correction record
        if len(parts) < 12:
            print(f"Insufficient data in correction line: {line}")
            return None
        
        # Extract and format the correction data
        ghcn_id = parts[0] if len(parts) > 0 else ''
        year_month = parts[1] if len(parts) > 1 else ''
        day = parts[2] if len(parts) > 2 else ''
        element = parts[3] if len(parts) > 3 else ''
        action = parts[4] if len(parts) > 4 else ''
        o_value = parts[5] if len(parts) > 5 else ''
        mflag = parts[6] if len(parts) > 6 else ''
        qflag = parts[7] if len(parts) > 7 else ''
        sflag = parts[8] if len(parts) > 8 else ''
        e_value = parts[9] if len(parts) > 9 else ''
        eflag = parts[10] if len(parts) > 10 else ''
        source = parts[11] if len(parts) > 11 else ''
        submission_date = parts[12] if len(parts) > 12 else ''
        datzilla_number = parts[13] if len(parts) > 13 else ''
        # Handle correction date
        correction_date = ''
        submission_date = datetime.strptime(submission_date, "%Y%m%d").date().strftime('%Y-%m-%d')
        
        
        # Combine YYYYMM + DD to create correction date
        if year_month and day:
            try:
                correction_date_str = year_month + day.zfill(2)
                correction_date = datetime.strptime(correction_date_str, "%Y%m%d").date().strftime('%Y-%m-%d')
            except ValueError:
                print(f"Invalid date format: {year_month}{day}")
                correction_date = f"{year_month}-{day}"
        

        
        # Return the formatted correction record
        # Format: [username, ghcn_id, correction_date, element, action, o_value, mflag, qflag, sflag, e_value, submission_date, datzilla_number]
        return [
            username,
            ghcn_id,
            correction_date,
            element,
            action,
            o_value,
            mflag,
            qflag,
            sflag,
            e_value,
            eflag,
            source,
            submission_date,  
            datzilla_number
        ]
        
    except Exception as e:
        print(f"Error parsing correction line '{line}': {e}")
        return None


@correction_bp.route('/corrections/previous/api', methods=['GET'])
@login_required
def get_previous_corrections_api():
    """
    API endpoint to return previous corrections data as JSON.
    This can be used for AJAX requests to populate tables dynamically.
    
    Returns:
        JSON response with corrections data and metadata
    """
    try:
        corrections_data = get_previous_corrections()
        
        # Create response with metadata
        response_data = {
            'success': True,
            'total_corrections': len(corrections_data),
            'corrections': corrections_data,
            'columns': [
                'Username',
                'GHCN ID',
                'Date',
                'Element',
                'Action',
                'Original Value',
                "mflag",
                "qflag",
                "sflag",
                'Expected Value',
                "eflag",
                'Source'
                'Correction Date',
                'Datzilla Number'
            ]
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"Error in previous corrections API: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to load corrections data',
            'total_corrections': 0,
            'corrections': []
        }), 500
