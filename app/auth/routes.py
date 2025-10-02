import polars as pl
from app.auth import auth_bp
from app.utilities.JSON_DataStore import JSON_DataStore as js_ds
from flask import render_template, redirect, url_for, session, request, flash
from .models.auth_models import User
from werkzeug.security import generate_password_hash, check_password_hash
from .. import db
from flask_login import login_user, login_required, logout_user, current_user


@auth_bp.route('/login')
def login():
    # If user is already logged in, redirect to corrections page
    if current_user.is_authenticated:
        return redirect(url_for('corrections.index'))
    return render_template('auth/login.html')

@auth_bp.route('/login_service', methods=['POST'])
def login_post():
    # Get form data
    username = request.form.get('username')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False

    # Get user from datastore
    user = User.get(username)

    # Check if user exists and password is valid
    if not user or not user.check_password(password):
        flash('Please check your login details and try again.', 'error')
        return redirect(url_for('auth.login'))

    # Log the user in with Flask-Login
    login_user(user, remember=remember)
    
    # Get the next page to redirect to (if any)
    next_page = request.args.get('next')
    if not next_page or not next_page.startswith('/'):
        next_page = url_for('corrections.index')
    
    return redirect(next_page)

@auth_bp.route('/signup')
def signup():
    # If user is already logged in, redirect to corrections page
    if current_user.is_authenticated:
        return redirect(url_for('corrections.index'))
    return render_template('auth/signup.html')

@auth_bp.route('/signup', methods=['POST'])
def signup_post():
    # Get form data
    username = request.form.get('username')
    name = request.form.get('name')
    password = request.form.get('password')

    # Validate input
    if not username or not name or not password:
        flash('Username, name, and password are required.', 'error')
        return redirect(url_for('auth.signup'))

    # Check if user already exists
    if User.user_exists_by_username(username):
        flash('Username already exists. Please choose a different username.', 'error')
        return redirect(url_for('auth.signup'))

    # Create new user (email is not required)
    new_user = User.create_user(username, name, '', password)
    
    if new_user:
        flash('Account created successfully! Please log in.', 'success')
        return redirect(url_for('auth.login'))
    else:
        flash('Error creating account. Please try again.', 'error')
        return redirect(url_for('auth.signup'))

@auth_bp.route('/logout')
@login_required
def logout():
    # Clear any session data (for backward compatibility)
    session.pop('_username', None)
    # Log out with Flask-Login
    logout_user()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('auth.login'))