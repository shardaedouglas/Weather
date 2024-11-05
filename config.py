import os

basedir = os.path.abspath(os.path.dirname(__file__))
SECRET_KEY = '835a34ce875ddfbcb911ac278b03701191c79ee9b0019466160fa498c00c72d1'
DEBUG = False
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')