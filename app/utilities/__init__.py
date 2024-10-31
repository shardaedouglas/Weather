from flask import Blueprint

utilities_bp = Blueprint('utilities', __name__)
from app.utilities import routes