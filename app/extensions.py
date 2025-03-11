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

# Function to parse ENTIRE station file and return list of lat, long, and elevation
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
                elevation = float(parts[3])
                stations.append((station_id, latitude, longitude, elevation))
            except ValueError:
                continue  # Skip lines with invalid numbers
    return stations


# Function to parse the station file and return lat, lng, and elevation for SINGLE station.
def get_station_lat_long(file_path, station_id):
    print ("file_path: ", file_path)
    print ("station_id: ", station_id)

    with open(file_path, 'r') as f:
        for line in f:
            parts = line.split()

            if len(parts) < 3:
                continue  # Skip invalid lines
            current_station_id = parts[0]
            if current_station_id == station_id:
                try:                    
                    latitude = float(parts[1])
                    longitude = float(parts[2])
                    elevation = float(parts[3])
                    return latitude, longitude, elevation  # Return as soon as the station is found
                except ValueError:
                    break  # Stop processing if numbers are invalid
    return None  # Return None if the station is not found


# Finds distance between 2 lat lng coordinates (Great Circle Distance)
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

# Finds stations distances, checks to make sure they are within the radius, and appends the elevation and distance to result.
def find_stations(lat, lon, radius, station_data):
    import math  # Ensure math is imported in case it's used outside

    result = []
    for station_id, station_lat, station_lon, station_elevation in station_data:
        
        # Calculate the distance using the getStations function
        distance = getStations(lat, lon, station_lat, station_lon)
        
        if distance <= radius:
            # Append station ID, latitude, longitude, distance, and elevation
            result.append((station_id, station_lat, station_lon, station_elevation, round(distance, 2)))
    
    # Sort the results by distance (5th element in the tuple)
    return sorted(result, key=lambda x: x[4])


#Unused, finds nearest station, but we use multiple stations within a radius not just nearest. 
def find_nearest_station(lat, lon, radius, station_data):
    nearest_station = None
    min_distance = float('inf')

    for station_id, station_lat, station_lon in station_data:
        if lat == station_lat and lon == station_lon:
            continue
        
        distance = getStations(lat, lon, station_lat, station_lon)
        if distance <= radius and distance < min_distance:
            nearest_station = (station_id, station_lat, station_lon, distance)
            min_distance = distance

    return nearest_station  # Return the nearest station or None if no station is within the radius
