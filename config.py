import os
from dotenv import load_dotenv

secret_key = os.getenv('SECRET_KEY')
basedir = os.path.abspath(os.path.dirname(__file__))
DEBUG = False
class Config:
    SECRET_KEY = secret_key