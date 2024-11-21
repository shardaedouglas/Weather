from flask import g
from app.extensions import get_db

class Corrections:
    def __init__(self, ghcn_id, correction_date=None, begin_date=None, end_date=None, 
                 element=None, action=None, o_value=None, e_value=None, defaults=True, datzilla_number=None):
        self.ghcn_id = ghcn_id
        self.correction_date = correction_date
        self.begin_date = begin_date
        self.end_date = end_date
        self.element = element
        self.action = action
        self.o_value = o_value
        self.e_value = e_value
        self.defaults = defaults
        self.datzilla_number = datzilla_number

    def save_to_db(self):
        db = get_db()
        try:
            db.execute("""
            INSERT INTO corrections (ghcn_id, correction_date, begin_date, end_date, 
                                    element, action, o_value, e_value, defaults, datzilla_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.ghcn_id, self.correction_date, self.begin_date, self.end_date,
                self.element, self.action, self.o_value, self.e_value, self.defaults, self.datzilla_number
            ))
            db.commit()
            return True
        except Exception as e:
            print(f"Error saving to database: {e}") 
            raise e
