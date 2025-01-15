from app.ghcndata import ghcndata_bp
from flask import render_template, request, jsonify
from app.extensions import mail #Move to utilities
from flask_mail import Message #Move to utilities
from app.ghcndata.forms import GhcnDataForm
from app.dataingest.readandfilterGHCN import parse_and_filter


# Route to Render GHCN Station Page
@ghcndata_bp.route('/ghcn_data')
def view_ghcn_data():
    # Form instatialization
    form = GhcnDataForm(country='', state='')

    return render_template('/ghcn_data/ghcndata_form.html', ghcnForm=form)


# Route to Render Station Metadata Page
@ghcndata_bp.route('/station_metadata')
def view_ghcn_metadata():

    return render_template('/ghcn_data/station_metadata.html')




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
        print("Form Data:", request.form)
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
        
        file_path = './USW00093991.dly' # I THINK THIS IS HARD CODED IN THE PARSER STILL

        # print(file_path)
        # print(f"GHCN ID: {ghcn_id}")
        # print(f"correction_date: {correction_date}")
        # print("correction_year: ", correction_year)
        # print("correction_month: ", correction_month)
        # print(f"station_type: {station_type}")

        # # Run parser with form data
        filtered_df = parse_and_filter(
            # ghcn_id = ghcn_id,
            file_path=file_path,
            year=correction_year,
            month=correction_month,
            observation_type = station_type
        )
        
        # # Print results
        print("filtered_df in ghcndata routes",filtered_df)
        
        JSONformatedData = format_as_json(filtered_df)
        print("JSONformatedData in ghcndata routes",JSONformatedData)

        # Return
        return JSONformatedData
        # return jsonify({
        #     "message": f"Correction processed successfully for GHCN ID: {ghcn_id}!",
        #     "filtered_data": filtered_df
        # }), 201
        
    except Exception as e:
        print(f"Error in process_correction: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
    
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

