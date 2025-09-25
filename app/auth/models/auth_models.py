import polars as pl

class User():
    
    def user_exists(user_table, username):
        
        user_exists = False
        matched_user = user_table.filter(pl.col("username")==username)
        
        if matched_user.shape[0] > 0:
            user_exists = True
        
        return user_exists
    
    def password_is_valid(user_table, username, password):
        
        is_valid = False
        matched_user = user_table.filter(pl.col("username")==username)
        
        if matched_user.shape[0] > 0:
            saved_password = matched_user.head()["password"].item()
            
            if password == saved_password:
                is_valid = True
        
        return is_valid