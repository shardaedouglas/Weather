from app.corrections import correction_bp
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections
from flask import render_template, request, jsonify
from app.extensions import get_db
from app.corrections.models.corrections import Corrections
from datetime import datetime
from app.dataingest.readandfilterGHCN import parse_and_filter
import os
file_path = os.path.join(os.getcwd(), 'USW00093991.dly')


@correction_bp.route('/')
def index():
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
    
    monthly_form = MonthlyCorrections(
        ghcn_id=ghcn_id,
        date=correction_date,  
    )
    
    range_form = RangeCorrections(
        ghcn_id=ghcn_id,
        begin_date=begin_date,
        end_date=end_date,
        datzilla_number=datzilla_number,
        element=element,
        action=action
    )

    return render_template(
        '/corrections/correction_form.html',
        selected_form=selected_form,
        daily_form=daily_form,
        monthly_form=monthly_form,
        range_form=range_form
    )
       
@correction_bp.route('/process_correction', methods=['POST'])
def process_correction():
    try:
        # Extract form data from the POST request
        correction_type = request.form.get('correction_type', 'daily')  # Default to 'daily' if not provided
        ghcn_id = request.form.get('ghcn_id')
        correction_date = request.form.get('date')
        begin_date = request.form.get('begin_date')
        end_date = request.form.get('end_date')
        element = request.form.get('element')
        action = request.form.get('action')
        o_value = request.form.get('o_value')
        e_value = request.form.get('e_value')
        defaults = request.form.get('defaults') == 'on'  # Convert 'on' string to boolean
        datzilla_number = request.form.get('datzilla_number')
        
        print(f"Correction Type: {correction_type}")
        print(f"GHCN ID: {ghcn_id}")
        print(f"Correction Date: {correction_date}")
        print(f"Begin Date: {begin_date}")
        print(f"End Date: {end_date}")
        print(f"Element: {element}")
        print(f"Action: {action}")
        print(f"O-Value: {o_value}")
        print(f"E-Value: {e_value}")
        print(f"Defaults: {defaults}")
        print(f"Datzilla Number: {datzilla_number}")
        
        if correction_date:
            correction_year, correction_month, correction_day = correction_date.split('-')
            correction_year = int(correction_year)
            correction_month = int(correction_month)
            correction_day = int(correction_day)
        else:
            correction_year, correction_month, correction_day = None, None, None
        
        file_path = './USW00093991.dly' # I THINK THIS IS HARD CODED IN THE PARSER STILL

        # print(file_path)
        # print(correction_year)
        # print(correction_month)
        # print(correction_day)
        # print("!", element)

        # Run parser with form data
        filtered_df = parse_and_filter(
            correction_type=correction_type,
            file_path=file_path,
            year=correction_year,
            month=correction_month,
            day=correction_day,
            observation_type=element,  # Assuming 'element' corresponds to observation_type
            country_code=None,  # This can be passed if needed
            network_code=None,  # This can be passed if needed
            station_code=None,  # This can be passed if needed
        )
        
        # Print results
        print("!",filtered_df)
        
        # Return
        return jsonify({
            "message": f"Correction processed successfully for GHCN ID: {ghcn_id}!",
            "filtered_data": filtered_df
        }), 201
        
    except Exception as e:
        print(f"Error in process_correction: {e}")
        return jsonify({"error": "Internal server error"}), 500
