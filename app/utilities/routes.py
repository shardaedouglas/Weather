from app.utilities import utilities_bp
from app.utilities.Reports.CdMonthly_Pub.CdMonthly_pub import *
from flask_login import login_required

# Enter utilities AKA Home
@utilities_bp.route('/utilities')
@login_required
def utilities():
    return '<h1>Utilities</h1>' # Link html templates here instead..
