from app.corrections import correction_bp
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections
from flask import render_template, request, jsonify
from app.extensions import get_db
from app.corrections.models.corrections import Corrections


# Enter Corrections AKA Home
@correction_bp.route('/')
def index():
    daily_form = DailyCorrections()
    monthly_form = MonthlyCorrections()
    range_form = RangeCorrections()

    return render_template('/corrections/correction_form.html', daily_form=daily_form, monthly_form = monthly_form, range_form = range_form)


@correction_bp.route('/process_correction', methods=['POST'])
def process_correction():
    try:
        # Extract form data
        # correction_type = request.form.get('correction_type')
        correction_type = "daily"
        ghcn_id = request.form.get('ghcn_id')
        correction_date = request.form.get('date')
        begin_date = request.form.get('begin_date')
        end_date = request.form.get('end_date')
        element = request.form.get('element')
        action = request.form.get('action')
        o_value = request.form.get('o_value')
        e_value = request.form.get('e_value')
        defaults = request.form.get('defaults') == 'on'
        datzilla_number = request.form.get('datzilla_number')

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
        print(f"Error in process_correction: {e}")  # Log error for debugging.
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500