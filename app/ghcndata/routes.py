from app.ghcndata import ghcndata_bp
from flask import render_template

# Enter Corrections AKA Home
@ghcndata_bp.route('/ghcn_data')
def view_ghcn_data():
    return render_template('ghcndata.html')