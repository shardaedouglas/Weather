from app.ghcndata import ghcndata_bp
from flask import render_template
from app.extensions import mail #Move to utilities
from flask_mail import Message #Move to utilities
from app.ghcndata.forms import GhcnDataForm


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

# Route to Render Station Metadata Page
@ghcndata_bp.route('/ghcn_data/show')
def view_ghcn_station_data():

    return render_template('/ghcn_data/ghcn_station_data.html')



# Route to Send Emails
@ghcndata_bp.route('/send_email') # Need to be removed from this directory and added into Utilities...
def send_email():
    msg = Message("Hello this is a test to myself!",
                  sender="NCEI.Datzilla@noaa.gov",
                  recipients=["matthew.kelly@noaa.gov"])
    msg.body = "This is the email body"
    mail.send(msg)
    return "Email sent!"    