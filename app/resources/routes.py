from app.resources import resources_bp
from flask import render_template
from flask_login import login_required


@resources_bp.route('/resources/readme')
@login_required
def readme():
    return render_template('resources/readme.html')

@resources_bp.route('/resources/country_codes')
@login_required
def country_codes():
    return render_template('resources/country_codes.html')

@resources_bp.route('/resources/class_definitions')
@login_required
def class_definitions():
    return render_template('resources/class_definitions.html')

@resources_bp.route('/resources/accessibility')
@login_required
def accessibility():
    return render_template('resources/accessibility.html')