from app.resources import resources_bp
from flask import render_template


@resources_bp.route('/resources/readme')
def readme():
    return render_template('resources/readme.html')

@resources_bp.route('/resources/country_codes')
def country_codes():
    return render_template('resources/country_codes.html')

@resources_bp.route('/resources/class_definitions')
def class_definitions():
    return render_template('resources/class_definitions.html')

@resources_bp.route('/resources/accessibility')
def accessibility():
    return render_template('resources/accessibility.html')