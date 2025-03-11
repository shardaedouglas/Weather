import os
import tempfile

import pytest
from app import create_app, db
from flask import current_app
from flask import session as fsession

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc

from pathlib import Path
from ..models.auth_models import User


@pytest.fixture
def app():
    db_fd, db_path = tempfile.mkstemp()

    app = create_app({
        'TESTING': True,
        'DATABASE': db_path
    })

    # Setup
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()
    

    # with app.app_context():
    #     init_db()
    #     get_db().executescript(_data_sql)
    # print(app.config)
    

    #Teardown
    



@pytest.fixture
def client(app):
    return app.test_client()




def test_pytest(client):
    """Test the home route."""
    response = client.get('/')
    # print("current app config.{}".format(current_app.config))
    
    assert response.status_code == 200

# is the database made? 
# This uses pathlib, which I should remove later. 
# Probably don't even keep this test. 
def test_database_exists(client):
    """Test db creation."""
    file_path = Path(current_app.config['DATABASE'])
    assert file_path.exists()
    

###############################################




# # Define the base for declarative models
# Base = declarative_base()


# # Define a simple model
# class User(Base):
#     __tablename__ = 'users'
#     id = Column(Integer, primary_key=True)
#     name = Column(String)




# Example test
def test_db_add_user(client):
    """Test adding user to db."""
    new_user = User(name='Alice', username="TEST",password="password")
    db.session.add(new_user)
    db.session.commit()


    retrieved_user = db.session.query(User).filter_by(name='Alice').first()
    assert retrieved_user.name == 'Alice'

@pytest.mark.parametrize("name, username, password", [
    ('Bob',None,None),
    (None,'username', None),
    (None, None, None)
])
def test_db_add_invalid_user(client, name, username, password):
    """Test adding incomplete users to db."""
    with pytest.raises(exc.IntegrityError):
            
        new_user = User(name=name, username=username, password=password)
        db.session.add(new_user)
        db.session.commit()


        # retrieved_user = db.session.query(User).filter_by(name='Bob').first()
        # assert retrieved_user.name == 'Bob'

# @pytest.mark.wip
def test_login_route(client):
    """Test the login route."""
    response = client.get('/login')
    assert response.status_code == 200





# @pytest.mark.wip
def test_signup_route(client):
    """Test the signup route via client."""
    assert client.get('/signup').status_code == 200
    
    response = client.post(
        '/signup', data={'name': 'Test_user', 'username': 'Test_user01', 'password': 'password'}
    )    
    
    assert response.headers["Location"] == "/login"

    assert db.session.query(User).filter_by(username='Test_user01').first() is not None

    # Test signing up with same user again.
    response = client.post(
        '/signup', data={'name': 'Test_user', 'username': 'Test_user01', 'password': 'password'}
    )

    # OK this works, but it just checks that the client was redirected to the sign up page. 
    # Does not check that the exact problem was "user already defined."
    assert response.status_code == 302 and '<a href="/signup">/signup</a>'.encode('utf-8') in response.data

    # print(db.session.info)
    # for item in db.session.info:
    #     print(item)
    # for keys in fsession.keys():
    #     print(key)


# @pytest.mark.wip
@pytest.mark.xfail(reason="Empty password should prompt a failure")
@pytest.mark.parametrize("name, username, password", [
    ('Bob',None,''),
    (None,'username', ''), # This passes. it should not. 
    (None, None, "")
])
def test_signup_invalid_user(client, name, username, password):
    """Test registering an invalid new user via client"""
    with pytest.raises(exc.IntegrityError) as excinfo:
        response = client.post(
            '/signup', data={'name': name, 'username': username, 'password': password}
        ) 
        
    # print(excinfo.value)
    # print(response.data)


