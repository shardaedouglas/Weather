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
    #json_ds = JSON_DataStore()
    d = {"foo":"bar", "True":1}
    js = js_ds()
    js.run_test(  )
    
    # login code goes here
    username = request.form.get('username')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False

    user = {
        "username": username,
        "password": password
    }
    
    user_found = True
    # check if the user actually exists
    # take the user-supplied password, hash it, and compare it to the hashed password in the database
    if not user_found:
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
    # code to validate and add user to database goes here
    username = request.form.get('username')
    name = request.form.get('name')
    password = request.form.get('password')

    # print(username + password)

    user = User.query.filter_by(username=username).first() # if this returns a user, then the email already exists in database

    if user: # if a user is found, we want to redirect back to signup page so user can try again
        flash('User already exists')
        return redirect(url_for('auth.signup'))

    # create a new user with the form data. Hash the password so the plaintext version isn't saved.
    new_user = User(username=username, name=name, password=generate_password_hash(password, method='scrypt'))

    # add the new user to the database
    db.session.add(new_user)
    db.session.commit()


    
    return redirect(url_for('auth.login'))

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))