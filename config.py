import os
from dotenv import load_dotenv

load_dotenv()

secret_key = os.getenv('SECRET_KEY')
basedir = os.path.abspath(os.path.dirname(__file__))
DEBUG = False
class Config:
    SECRET_KEY = secret_key
    DATABASE = "app.db" # Change and test with sqlite:///app.db
    SQLALCHEMY_DATABASE_URI = 'sqlite:///app.db'
