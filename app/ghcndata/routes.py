from app.ghcndata import ghcndata_bp
from flask import render_template
from app.extensions import mail
from flask_mail import Message
from app.ghcndata.forms import GhcnDataForm


# Enter Corrections AKA Home
@ghcndata_bp.route('/ghcn_data')
def view_ghcn_data():
    #form = GhcnDataForm()

    form = GhcnDataForm(country='', state='')

    return render_template('ghcndata.html', ghcnForm=form)

@ghcndata_bp.route('/send_email')
def send_email():
    msg = Message("Hello this is a test to myself!",
                  sender="NCEI.Datzilla@noaa.gov",
                  recipients=["matthew.kelly@noaa.gov"])
    msg.body = "This is the email body"
    mail.send(msg)
    return "Email sent!"    