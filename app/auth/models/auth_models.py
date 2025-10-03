import polars as pl
from flask_login import UserMixin
from app.utilities.JSON_DataStore import JSON_DataStore as js_ds
from werkzeug.security import check_password_hash, generate_password_hash

class User(UserMixin):
    def __init__(self, username, name=None, email=None, password_hash=None):
        self.id = username  # Flask-Login requires an 'id' attribute
        self.username = username
        self.name = name
        self.email = email
        self.password_hash = password_hash
    
    @staticmethod
    def get(username):
        """Get user by username from JSON datastore"""
        try:
            js = js_ds()
            users = js.get_users()
            
            for user_data in users:
                if user_data.get('username') == username:
                    return User(
                        username=user_data.get('username'),
                        name=user_data.get('name'),
                        email=user_data.get('email'),
                        password_hash=user_data.get('password')  # This might be plain text in existing data
                    )
            return None
        except Exception as e:
            print(f"Error getting user {username}: {e}")
            return None
    
    @staticmethod
    def create_user(username, name, email, password):
        """Create a new user and save to JSON datastore"""
        try:
            js = js_ds()
            
            # Check if user already exists
            if User.user_exists_by_username(username):
                return None
            
            # Hash the password
            password_hash = generate_password_hash(password, method='scrypt')
            
            # Save user to JSON datastore
            user_data = {
                "username": username,
                "name": name,
                "email": email,
                "password": password_hash
            }
            js.save_user(user_data)
            
            return User(username=username, name=name, email=email, password_hash=password_hash)
        except Exception as e:
            print(f"Error creating user {username}: {e}")
            return None
    
    def check_password(self, password):
        """Check if provided password matches user's password"""
        try:
            # Handle both hashed and plain text passwords (for backward compatibility)
            if self.password_hash.startswith('scrypt:'):
                return check_password_hash(self.password_hash, password)
            else:
                # For existing plain text passwords, do direct comparison
                # In production, you should migrate these to hashed passwords
                return self.password_hash == password
        except Exception as e:
            print(f"Error checking password for {self.username}: {e}")
            return False
    
    @staticmethod
    def user_exists(user_table, username):
        """Legacy method for backward compatibility"""
        user_exists = False
        matched_user = user_table.filter(pl.col("username")==username)
        
        if matched_user.shape[0] > 0:
            user_exists = True
        
        return user_exists
    
    @staticmethod
    def user_exists_by_username(username):
        """Check if user exists by username"""
        return User.get(username) is not None
    
    @staticmethod
    def password_is_valid(user_table, username, password):
        """Legacy method for backward compatibility"""
        is_valid = False
        matched_user = user_table.filter(pl.col("username")==username)
        
        if matched_user.shape[0] > 0:
            saved_password = matched_user.head()["password"].item()
            
            if password == saved_password:
                is_valid = True
        
        return is_valid