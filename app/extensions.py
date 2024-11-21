# extensions.py
from flask import g
from flask_mail import Mail
import sqlite3
import math
import os


### DEFINE SQLITE DB ###

DATABASE = "app.db"  # Path to your SQLite database

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row  # Makes results like dictionaries
    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

### DEFINE MAIL ###

mail = Mail()



### COMPUTE NEIGHBORING STATIONS ###
#Calculate the great-circle distance between two points on the Earth's surface.

# Function to parse the station file
def parse_station_file(file_path):
    
    stations = []
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.split()
            if len(parts) < 3:
                continue  # Skip invalid lines
            station_id = parts[0]
            try:
                latitude = float(parts[1])
                longitude = float(parts[2])
                stations.append((station_id, latitude, longitude))
            except ValueError:
                continue  # Skip lines with invalid numbers
    return stations

def getStations(lat1, lon1, lat2, lon2):
    
    # Radius of Earth in kilometers
    R = 6371.0  
    
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    #calculate
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    # Distance in kilometers
    return R * c

def find_stations(lat, lon, radius, station_data):

    result = []
    for station_id, station_lat, station_lon in station_data:
        distance = getStations(lat, lon, station_lat, station_lon)
        if distance <= radius:
            result.append((station_id, station_lat, station_lon, distance))
    
    return sorted(result, key=lambda x: x[3])  # Sort by distance

