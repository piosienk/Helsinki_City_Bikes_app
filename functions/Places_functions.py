import pandas as pd
import numpy as np
import requests
import json
import os
import geopy.distance
from types import SimpleNamespace


def places_data_download(radius = 0.5,bikes_data_file="./Data/Bikes/database.csv", places_data_path="./Data/Places_data/"):
    
    """
    radius - maximum distance between station and place (in km)
    Function checks whether places data for each station is available. If not, downloads required data
    
    returns list of unique station coordinates
    """
    
    df_bikes = pd.read_csv(bikes_data_file)   
    dep_coordinates = np.unique(np.stack((df_bikes.departure_latitude, df_bikes.departure_longitude), axis= -1), axis=0)
    ret_coordinates = np.unique(np.stack((df_bikes.return_latitude, df_bikes.return_longitude), axis= -1), axis=0)

    unique_coordinates = np.unique(np.vstack((dep_coordinates, ret_coordinates)), axis=0)
    
    path_file = places_data_path + "station_"
    
    # api-endpoint
    URL = "https://open-api.myhelsinki.fi/v1/places/"
    station = 1

    for coordinate in unique_coordinates[:-1]:
        
        path_full = path_file + str(station) + ".txt"
        
        if not os.path.exists(path_full):
            print("Station data: ", station, " does not exist. Downloading places info.")
    
            location = str(list(coordinate)[0]) + "," + str(list(coordinate)[1]) + ", " + str(radius)
            PARAMS = {'distance_filter':location}
    
            r = requests.get(url = URL, params=PARAMS)

            data_places = r.json()
            data_places['meta']['station_location'] = {"lat" : list(coordinate)[0], "lon": list(coordinate)[1]}
            data_places['meta']['station_number'] = station
    
    
            with open(path_full, 'w') as file:
                json.dump(data_places, file, ensure_ascii=False, indent=4)
    
            file.close()
    
        else:
            print("Station data: ", station, " already exists")
            
        station += 1

            
    return unique_coordinates


def places_data_transformation(unique_coordinates, places_data_path="./Data/Places_data/"):
    
    path_file = places_data_path + "station_"
    station = 1
    
    df_all_stations = pd.DataFrame(columns=["station_lat", "station_lon","ACCOMMODATION", "RESTAURANTS & CAFES", "SAUNA & WELLNESS",
                                           "MEETING PLACES", "VENUES", "WORK & STUDY", "SERVICES", 
                                           "BARS & NIGHTLIFE", "BANQUET VENUES", "SIGHTS & ATTRACTIONS",
                                           "NATURE & SPORTS", "MUSEUMS & GALLERIES", "SHOPPING", "TOTAL"])
    
    for coordinate in unique_coordinates[:-1]:
        
        station_index = station - 1
        
        path_full = path_file + str(station) + ".txt"
        
        with open(path_full) as station_file:    
            station_data = json.load(station_file)  

        df = pd.json_normalize(station_data)
        
        station_values = [df['meta.station_location.lat'][0], df['meta.station_location.lon'][0]] + [0 for i in range(14)]
        df_all_stations.loc[station_index] = station_values

        for places_list in [x["tags"] for x in df.data[0]]:
            for place in places_list:
                if place["id"][5] == "1":
                    column_number = df_all_stations.columns.get_loc(place["name"])
                    df_all_stations.iloc[station_index, column_number] += 1                   
                    df_all_stations.iloc[station_index, -1] += 1                   

        station += 1
        
        
    return df_all_stations

def get_distance_to_attractions(unique_coordinate, attractions_path = "./Data/Main_Attractions/Attractions.csv"):
    
    df = pd.read_csv(attractions_path, sep=";")
    df.columns = ['Name', 'Lat', 'Lon', 'Type', ' Year']
    df_with_distance = df.copy()
    df_with_distance.loc[:,"distance"] = 0
    
    for i in range(len(df_with_distance)):
        df_with_distance.iloc[i,5] = geopy.distance.distance(
            (df_with_distance.iloc[i,1], df_with_distance.iloc[i,2]), unique_coordinate).km
    
    return df_with_distance

    

