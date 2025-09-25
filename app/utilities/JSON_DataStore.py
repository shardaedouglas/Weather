import json
from pathlib import Path

class JSON_DataStore():

    def write_json(self, path, json_data):
        
        base_path = Path(__file__).parent
        file_path = (base_path / path ).resolve()

        with open(file_path, 'w') as file_out:
            json.dump(json_data, file_out)

    def read_json(self, path):
        
        base_path = Path(__file__).parent
        file_path = (base_path / path ).resolve()
        
        with open(file_path) as file_in:
            return json.load(file_in)

    def run_test(self):
        
        path = "../datastore/store.json"

        # input from form or wherever your new JSON is coming from...
        # It could also be coming from a REST API etc:
        #input = request.form['data']
        py_dict = {"new": "data"}

        # read in existing JSON
        existing_json = self.read_json(path)
        # {"existing": "json"}

        # add new JSON to existing JSON however you see fit
        [(k, v)] = py_dict.items()
        existing_json[k] = v
        {"existing": "json", "new": "data"}

        # now update datastore
        self.write_json(path, existing_json)