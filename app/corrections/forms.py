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

# ELEMENTS
with open('app/corrections/corr-weather-elements.txt', 'r') as f:
        ELEMENTS = [line.strip() for line in f]
        # ELEMENTS = [(line[:4], line[5:]) for line in ELEMENTS] # Short Description
        ELEMENTS = [(line[:4], line[:4]) for line in ELEMENTS] # CODE only

# SUB ELEMENTS
with open('app/corrections/corr-sub-weather-elements.txt', 'r') as f:
        SUB_ELEMENTS = [line.strip() for line in f]
        # ELEMENTS = [(line[:4], line[5:]) for line in ELEMENTS] # Short Description
        SUB_ELEMENTS = [(line[:4], line[:4]) for line in SUB_ELEMENTS] # CODE only


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
    source = StringField('Source')
    submit = SubmitField('Submit')
    
    sub_element_SN = SelectField('SubElement', choices=SUB_ELEMENTS[:63])
    sub_element_SX = SelectField('SubElement', choices=SUB_ELEMENTS[63:126])
    sub_element_WT = SelectField('SubElement', choices=SUB_ELEMENTS[126:147])
    sub_element_WV = SelectField('SubElement', choices=SUB_ELEMENTS[147:])

class MonthlyCorrections(FlaskForm):
    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    element = SelectField('Element', choices=ELEMENTS)
    datzilla_number = StringField('Datzilla #')
    action = SelectField('Action', choices=ACTIONS)
    submit = SubmitField('Submit')

    sub_element_SN = SelectField('SubElement', choices=SUB_ELEMENTS[:63])
    sub_element_SX = SelectField('SubElement', choices=SUB_ELEMENTS[63:126])
    sub_element_WT = SelectField('SubElement', choices=SUB_ELEMENTS[126:147])
    sub_element_WV = SelectField('SubElement', choices=SUB_ELEMENTS[147:])

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

    sub_element_SN = SelectField('SubElement', choices=SUB_ELEMENTS[:63])
    sub_element_SX = SelectField('SubElement', choices=SUB_ELEMENTS[63:126])
    sub_element_WT = SelectField('SubElement', choices=SUB_ELEMENTS[126:147])
    sub_element_WV = SelectField('SubElement', choices=SUB_ELEMENTS[147:])

class HourlyCorrections(FlaskForm):
    form_type = ghcn_id = StringField('TYPE', validators=[InputRequired()])
    ghcn_id = StringField('GHCN ID', validators=[InputRequired()])
    date = DateField('Date', validators=[InputRequired()])
    # Reference corrections.routes.hourly_corrections for hourly element list
    element = SelectMultipleField('Element')