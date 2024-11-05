from wtforms.validators import InputRequired, Length
from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, DateField, SelectField


#Options for multiple choice fields

#Station States
with open('ghcnd-states.txt', 'r') as f:
        STATES = [line.strip() for line in f]
STATES = [letter[3:] for letter in STATES]

# Station Countries
with open('ghcnd-countries.txt', 'r') as f:
        COUNTRIES = [line.strip() for line in f]
COUNTRIES = [letter[3:] for letter in COUNTRIES]

# Station Type
STATION_TYPES = (
    ('1','A-All'),
    ('2', '0-Unspecified'),
    ('3', '1-CoCoRaHS'),
    ('4' ,'C-US Coop'),
    ('5', 'M-WMO'),
    ('6', 'N-NMHC'),
    ('7', 'W-WBAN ID'),
)

# Form Classes

class GhcnDataForm(FlaskForm):
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    state = SelectField('State', choices=STATES, default = 'Select')
    country = SelectField('Country', choices=COUNTRIES, default = 'Select')
    station_type = SelectField('Type', choices=STATION_TYPES, default = 'Select')
    submit = SubmitField('Submit')
    test = SelectField('My Field', choices=[('value1', 'Label 1'), ('value2', 'Label 2')], default='value2')