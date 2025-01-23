from app.auth import auth_bp
from flask import render_template


@auth_bp.route('/login')
def login():
    # return 
    return render_template('auth/login.html')

# @auth_bp.route('/signup')
# def signup():
#     return 'Signup'

# @auth_bp.route('/logout')
# def logout():
#     return 'Logout'