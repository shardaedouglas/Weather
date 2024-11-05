from wtforms.validators import InputRequired, Length
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField



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


class CorrectionForm(FlaskForm):
