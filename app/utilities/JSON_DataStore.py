import json
from pathlib import Path

class JSON_DataStore():
    
    path = "../datastore/store.json"
    base_path = Path(__file__).parent
    file_path = (base_path / path ).resolve()

    #Takes a Python dictionary, which will be saved as JSON
    #Careful, you're updating the WHOLE THING
    def update_datastore(self, json_data):

        with open(self.file_path, 'w') as file_out:
            json.dump(json_data, file_out)
    
    def get_datastore(self):
        
        with open(self.file_path) as file_in:
            return json.load(file_in)

    def get_users(self):

        # read in existing JSON
        db = self.get_datastore()
        user_list = db["users"]
        return user_list
    
    def get_admin_settings(self):

        # read in existing JSON
        db = self.get_datastore()
        admin_settings = db["admin_settings"]
        return admin_settings

        