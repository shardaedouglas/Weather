from wtforms.validators import InputRequired, Length
from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, DateField, SelectField, SelectMultipleField


#Options for multiple choice fields
# ELEMENTS = (
#     ("TMAX", "Max Temp" ),
#     ("TMIN", "Min Temp"),
#     ("TOBS", "TOBS"),
#     ("PRCP", "Precipitation"),
#     ("SNOW", "Snow"),
#     ("SNWD", "SnowD"),
#     ("WT**", "WT##cd "),
# )
#Station States
with open('app/ghcndata/ghcnd-weather-elements.txt', 'r') as f:
        ELEMENTS = [line.strip() for line in f]
# STATES = [letter[:3] for letter in STATES]
        # ELEMENTS = [(line[:4], line[5:]) for line in ELEMENTS] # Short Description
        ELEMENTS = [(line[:4], line[:4]) for line in ELEMENTS] # CODE only


ACTIONS = (
    ("1", "1A" ),
    ("2", "2A"),
    ("3", "3A"),
    ("4", "3B"),
    ("5", "4A"),
    ("6", "4B"),
    ("7", "5"),
)

CORRECTION_TYPE = (
    ("1", "Daily Correction" ),
    ("2", "Monthly Correction"),
    ("3", "Correction Range"),
)

HOURLY_ELEMENTS = (

)


class DailyCorrections(FlaskForm):

    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    action = SelectField('Action', choices=ACTIONS)
    o_value = StringField('O-Value')
    e_value = StringField('E-Value')
    datzilla_number = StringField('Datzilla #')
    submit = SubmitField('Submit')

class MonthlyCorrections(FlaskForm):
    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    datzilla_number = StringField('Datzilla #')
    action = SelectField('Action', choices=ACTIONS)
    submit = SubmitField('Submit')

class RangeCorrections(FlaskForm):
    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    begin_date = DateField(validators=[InputRequired()])
    end_date = DateField(validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    action = SelectField('Action', choices=ACTIONS)
    defaults = BooleanField(default="checked")
    datzilla_number = StringField('Datzilla #')
    submit = SubmitField('Submit')

class HourlyCorrections(FlaskForm):
    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    element = SelectMultipleField('Element')