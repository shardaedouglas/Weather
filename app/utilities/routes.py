from app.utilities import utilities_bp
from app.utilities.Reports.CdMonthly_Pub.CdMonthly_pub import *

# Enter utilities AKA Home
@utilities_bp.route('/utilities')
def utilities():
    return '<h1>Utilities</h1>' # Link html templates here instead..
