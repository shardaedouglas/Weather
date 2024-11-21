# extensions.py
from flask import g
from flask_mail import Mail
import sqlite3

### DEFINE SQLITE DB ###

DATABASE = "app.db"  # Path to your SQLite database

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # Makes results like dictionaries
    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

### DEFINE MAIL ###

mail = Mail()
