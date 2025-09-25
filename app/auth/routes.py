import polars as pl
from app.auth import auth_bp
from app.utilities.JSON_DataStore import JSON_DataStore as js_ds
from flask import render_template, redirect, url_for, request, flash
from .models.auth_models import User
from werkzeug.security import generate_password_hash, check_password_hash
from .. import db
from flask_login import login_user, login_required, logout_user


@auth_bp.route('/login')
def login():
    # return 
    return render_template('auth/login.html')

@auth_bp.route('/login_service', methods=['POST'])
def login_post():
    
    #Query the DataStore for the user list and convert to a table
    js = js_ds()
    users = js.get_users()
    user_table = pl.DataFrame(users)

    # login code goes here
    username = request.form.get('username')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False

    user_found = User.user_exists(user_table, username)
    user_validated = False

    if user_found:
        user_validated = User.password_is_valid(user_table, username, password)

    user = {
        "username": username,
        "password": password
    }

    # check if the user actually exists
    # take the user-supplied password, hash it, and compare it to the hashed password in the database
    if not user_found or not user_validated:
        flash('Please check your login details and try again.')
        return redirect(url_for('auth.login')) # if the user doesn't exist or password is wrong, reload the page

    # if the above check passes, then we know the user has the right credentials
    # return redirect(url_for('index'))
    login_user(user, remember=remember)

    return redirect(url_for('corrections.index'))

@auth_bp.route('/signup')
def signup():
    return render_template('auth/signup.html')

@auth_bp.route('/signup', methods=['POST'])
def signup_post():
    
    #Query the DataStore for the user list and convert to a table
    js = js_ds()
    users = js.get_users()
    user_table = pl.DataFrame(users)    
    
    # code to validate and add user to database goes here
    username = request.form.get('username')
    name = request.form.get('name')
    password = request.form.get('password')

    # print(username + password)
    user_found = User.user_exists(user_table, username)
    #user = User.query.filter_by(username=username).first() # if this returns a user, then the email already exists in database

    if user_found: # if a user is found, we want to redirect back to signup page so user can try again
        flash('User already exists')
        return redirect(url_for('auth.signup'))

    # create a new user with the form data. Hash the password so the plaintext version isn't saved.
    #new_user = User(username=username, name=name, password=generate_password_hash(password, method='scrypt'))
    js.save_user({"username":username, "name":name, "password":password})
    # add the new user to the database
    #db.session.add(new_user)
    #db.session.commit()


    
    return redirect(url_for('auth.login'))

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))