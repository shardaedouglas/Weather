from app.ghcndata import ghcndata_bp
from flask import render_template
from app.ghcndata.forms import GhcnDataForm


# Enter Corrections AKA Home
@ghcndata_bp.route('/ghcn_data')
def view_ghcn_data():
    #form = GhcnDataForm()

    form = GhcnDataForm(country='', state='')


    return render_template('ghcndata.html', ghcnForm=form)
    