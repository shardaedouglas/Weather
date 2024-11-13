from app.corrections import correction_bp
from app.corrections.forms import DailyCorrections, MonthlyCorrections, RangeCorrections
from flask import render_template


# Enter Corrections AKA Home
@correction_bp.route('/')
def index():
    daily_form = DailyCorrections()
    monthly_form = MonthlyCorrections()
    range_form = RangeCorrections()

    return render_template('/corrections/correction_form.html', daily_form=daily_form, monthly_form = monthly_form, range_form = range_form)

