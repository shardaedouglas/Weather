from app.resources import resources_bp


@resources_bp.route('/resources/readme')
def readme():
    return '<h1>README</h1>' # Link html templates here instead..

@resources_bp.route('/resources/country_codes')
def country_codes():
    return '<h1>Country Codes</h1>' # Link html templates here instead..

@resources_bp.route('/resources/class_definitions')
def class_definitions():
    return '<h1>Class Definitions</h1>' # Link html templates here instead..

@resources_bp.route('/resources/accessibility')
def accessibility():
    return '<h1>Accessibility</h1>' # Link html templates here instead..