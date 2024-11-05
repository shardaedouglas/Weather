from flask import Flask

from config import Config

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    app.config['SECRET_KEY'] = '835a34ce875ddfbcb911ac278b03701191c79ee9b0019466160fa498c00c72d1'
    # Initialize Flask extensions here


    # Register blueprints here
    from app.corrections import correction_bp as corrections_bp
    app.register_blueprint(corrections_bp)
    
    from app.ghcndata import ghcndata_bp as ghcndata_bp
    app.register_blueprint(ghcndata_bp)

    from app.resources import resources_bp as resources_bp
    app.register_blueprint(resources_bp)

    from app.utilities import utilities_bp as utilities_bp
    app.register_blueprint(utilities_bp)



    # @app.route('/test')
    # def enterCorrections():
        # return '<h1>Test</h1>' # Link html templates here instead..

    return app