import os
from dotenv import load_dotenv

load_dotenv()

#secret_key = os.getenv('SECRET_KEY')
secret_key = str(os.getenv('SECRET_KEY'))

basedir = os.path.abspath(os.path.dirname(__file__))
DEBUG = False
class Config:
    SECRET_KEY = secret_key
    DATABASE = "app.db" # Change and test with sqlite:///app.db
    SQLALCHEMY_DATABASE_URI = 'sqlite:///app.db'
    
    # Email configuration for error reporting
    MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    MAIL_PORT = int(os.getenv('MAIL_PORT', 587))
    MAIL_USE_TLS = os.getenv('MAIL_USE_TLS', 'true').lower() in ['true', 'on', '1']
    MAIL_USERNAME = os.getenv('MAIL_USERNAME', 'NCEI.Datzilla@noaa.gov')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', 'ycgn gcyk ljzx uaoh')
    MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER', 'shardae.douglas@noaa.gov')
    
    # Error reporting email
    ERROR_REPORT_EMAIL = 'shardae.douglas@noaa.gov'