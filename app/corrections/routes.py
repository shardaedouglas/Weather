from app.corrections import correction_bp


# Enter Corrections AKA Home
@correction_bp.route('/')
def index():
    return '<h1>Hello, World!</h1>' # Link html templates here instead..

