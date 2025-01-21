from flask import Flask
from config import Config
from .extensions import mail, get_db, close_db, find_stations, parse_station_file
from flask import render_template
import os

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    app.config['SECRET_KEY'] = '835a34ce875ddfbcb911ac278b03701191c79ee9b0019466160fa498c00c72d1'
    
    
    ###### DB SETTINGS ######
    #DB path
    app.config['DATABASE'] = "app.db"

    # Close the database when the context closes
    @app.teardown_appcontext
    def teardown_db(exception):
        close_db()
        
        
    # Initialize the corrections table
    @app.before_request
    def initialize_corrections_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS corrections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            correction_date DATE,
            begin_date DATE,
            end_date DATE,
            element TEXT,
            action TEXT,
            o_value TEXT,
            e_value TEXT,
            defaults BOOLEAN DEFAULT 1,
            datzilla_number TEXT
        )
        """)
        db.commit()
        
    # Initialize the GHCN data table
    @app.before_request
    def initialize_ghcn_data_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS ghcn_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            country TEXT NOT NULL,
            state TEXT,
            date DATE NOT NULL,
            type TEXT NOT NULL
        )
        """)
        db.commit()
        
    # Initialize the GHCN_metadata table
    @app.before_request
    def initialize_ghcn_metadata_table():
        db = get_db()
        db.execute("""
        CREATE TABLE IF NOT EXISTS ghcn_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghcn_id TEXT NOT NULL,
            wmo_id TEXT NOT NULL,            
            state TEXT,
            country TEXT NOT NULL,
            Latitude REAL,
            Longitude REAL,
            gsn_flag TEXT,
            hcn_flag TEXT
            ghcnd_inventory TEXT
        )
        """)
        db.commit()
        
    ###### SMTP SETTINGS ######
    app.config['MAIL_SERVER'] = "smtp.gmail.com"
    app.config['MAIL_PORT'] = 587
    app.config['MAIL_USERNAME'] = "NCEI.Datzilla@noaa.gov"
    app.config['MAIL_PASSWORD'] = "ycgn gcyk ljzx uaoh"
    app.config['MAIL_USE_TLS'] = True
    app.config['MAIL_USE_SSL'] = False
    
    mail.init_app(app)
    # Register blueprints here
    from app.corrections import correction_bp as corrections_bp
    app.register_blueprint(corrections_bp)
    
    from app.ghcndata import ghcndata_bp as ghcndata_bp
    app.register_blueprint(ghcndata_bp)

    from app.resources import resources_bp as resources_bp
    app.register_blueprint(resources_bp)

    from app.utilities import utilities_bp as utilities_bp
    app.register_blueprint(utilities_bp)

    @app.route('/test')
    def test_page():
        # return '<h1>Test</h1>' # Link html templates here instead..
        # return render_template('test.html')
        return render_template('landing_page.html')


    return app