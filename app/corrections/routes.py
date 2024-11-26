from app.corrections import correction_bp
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections
from flask import render_template, request, jsonify
from app.extensions import get_db
from app.corrections.models.corrections import Corrections
from datetime import datetime


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
    )     # Default initialization

    return render_template(
        '/corrections/correction_form.html',
        selected_form=selected_form,
        daily_form=daily_form,
        monthly_form=monthly_form,
        range_form=range_form
    )
       
@correction_bp.route('/process_correction', methods=['GET'])
def process_correction():
    try:
        # Extract query parameters from the URL
        # correction_type = request.args.get('correction_type')
        correction_type = "daily"
        ghcn_id = request.args.get('ghcn_id')
        correction_date = request.args.get('date')
        begin_date = request.args.get('begin_date')
        end_date = request.args.get('end_date')
        element = request.args.get('element')
        action = request.args.get('action')
        o_value = request.args.get('o_value')
        e_value = request.args.get('e_value')
        defaults = request.args.get('defaults') == 'on' 
        datzilla_number = request.args.get('datzilla_number')

        # Create Corrections instance
        correction = Corrections(
            ghcn_id=ghcn_id,
            correction_date=correction_date,
            begin_date=begin_date,
            end_date=end_date,
            element=element,
            action=action,
            o_value=o_value,
            e_value=e_value,
            defaults=defaults,
            datzilla_number=datzilla_number
        )

        # Save to the database
        if correction.save_to_db():
            return jsonify({"message": f"{correction_type.capitalize()} correction added successfully!"}), 201
        else:
            return jsonify({"error": "Failed to save correction."}), 500
    except Exception as e:
        print(f"Error in process_correction: {e}")  # Log error for debugging
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500
