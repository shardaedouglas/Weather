from flask import Blueprint

ghcndata_bp = Blueprint('ghcndata', __name__)
from app.ghcndata import routes