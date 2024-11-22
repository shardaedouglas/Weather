from wtforms.validators import InputRequired, Length
from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, DateField, SelectField


#Options for multiple choice fields
ELEMENTS = (
    ("1", "Max Temp" ),
    ("2", "Min Temp"),
    ("3", "TOBS"),
    ("4", "Precipitation"),
    ("5", "Snow"),
    ("6", "SnowD"),
    ("7", "WT##cd "),
)
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


class DailyCorrections(FlaskForm):

    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    action = SelectField('Action', choices=ACTIONS)
    o_value = StringField('O-Value')
    e_value = StringField('E-Value')
    datzilla_number = StringField('Datzilla #')
    submit = SubmitField('Submit')

class MonthlyCorrections(FlaskForm):
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', format='%Y-%m', validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    datzilla_number = StringField('Datzilla #')
    action = SelectField('Action', choices=ACTIONS)
    submit = SubmitField('Submit')

class RangeCorrections(FlaskForm):
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    begin_date = DateField(validators=[InputRequired()])
    end_date = DateField(validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    action = SelectField('Action', choices=ACTIONS)
    defaults = BooleanField(default="checked")
    datzilla_number = StringField('Datzilla #')
    submit = SubmitField('Submit')