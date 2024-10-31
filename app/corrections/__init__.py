from flask import Blueprint

correction_bp = Blueprint('corrections', __name__)
from app.corrections import routes