from flask import Blueprint

resources_bp = Blueprint('resources', __name__)
from app.resources import routes