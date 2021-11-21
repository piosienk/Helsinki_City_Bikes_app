import pandas as pd
import numpy as np
import requests
import json
import os
import geopy.distance
import time

def places_data_download(radius=0.5, bikes_data_file="./Data/Bikes/database.csv",
                         places_data_path="./Data/Places_data/",
                         new_places_file="./Data/Locations/Locations_final_list.csv"):
    """
    radius - maximum distance between station and place (in km)
    Function checks whether places data for each station is available. If not, downloads required data

    returns list of unique station coordinates
    """

    df_bikes = pd.read_csv(bikes_data_file)
    dep_coordinates = np.unique(np.stack((df_bikes.departure_latitude, df_bikes.departure_longitude), axis=-1), axis=0)
    ret_coordinates = np.unique(np.stack((df_bikes.return_latitude, df_bikes.return_longitude), axis=-1), axis=0)
    unique_coordinates = np.unique(np.vstack((dep_coordinates, ret_coordinates)), axis=0)

    df_new_coordinates = pd.read_csv(new_places_file)
    new_coordinates = df_new_coordinates.loc[:, ["Lat", "Lon"]].to_numpy()
    merged_coordinates = np.vstack((unique_coordinates, new_coordinates))
    merged_coordinates = merged_coordinates[~np.isnan(merged_coordinates).any(axis=1), :]
    np.savetxt("./Data/Locations/merged_coordinates.csv", merged_coordinates,
               delimiter=",")

    required_coord = pd.DataFrame(merged_coordinates, columns=["lat", "lon"])
    existing_coord = pd.DataFrame(columns=["lat", "lon"])

    existing_coord_path_file = places_data_path + "merged_places_0" + str(int(radius * 10)) + ".txt"

    with open(existing_coord_path_file) as places_file:

        json_data = places_file.read()
        exisiting_data = json.loads(json_data)
        places_file.close()

    for i in range(len(exisiting_data)):
        existing_coord = existing_coord.append(exisiting_data[i]['meta']['station_location'], ignore_index=True)

    needed_coord = required_coord.merge(existing_coord, on=['lat', 'lon'], how='left', indicator=True)
    not_existing_needed_coord = needed_coord.loc[needed_coord['_merge'] == 'left_only', :]

    # api-endpoint
    URL = "https://open-api.myhelsinki.fi/v1/places/"

    station = 1
    for i in range(len(not_existing_needed_coord)):

        coordinate = not_existing_needed_coord.iloc[i, :]
        print("Loop nr: ", station)

        time.sleep(0.26)

        print("Station data: ", str(list(coordinate)[0]) + "," + str(list(coordinate)[1]),
              " does not exist. Downloading places info.")

        location = str(list(coordinate)[0]) + "," + str(list(coordinate)[1]) + ", " + str(radius)
        PARAMS = {'distance_filter': location}
        r = requests.get(url=URL, params=PARAMS)

        data_places = r.json()
        data_places['meta']['station_location'] = {"lat": list(coordinate)[0], "lon": list(coordinate)[1]}
        data_places['meta']['station_number'] = station

        exisiting_data.append(data_places)
        station += 1

        if station % 500 == 0:
            with open(existing_coord_path_file, mode='w', encoding='utf-8') as places_file:
                json.dump(exisiting_data, places_file)
                places_file.close()

    with open(existing_coord_path_file, mode='w', encoding='utf-8') as places_file:

        json.dump(exisiting_data, places_file)
        places_file.close()

    return required_coord.loc[:, ["lat", "lon"]]


def places_data_transformation(radius=0.5, places_data_path="./Data/Places_data/"):

    existing_coord_path_file = places_data_path + "merged_places_0" + str(int(radius * 10)) + ".txt"

    with open(existing_coord_path_file) as places_file:

        json_data = places_file.read()
        json_data = "[" + json_data + "]"

        exisiting_data = json.loads(json_data)
        places_file.close()

    station = 1

    df_all_stations = pd.DataFrame(
        columns=["station_lat", "station_lon", "ACCOMMODATION", "RESTAURANTS & CAFES", "SAUNA & WELLNESS",
                 "MEETING PLACES", "VENUES", "WORK & STUDY", "SERVICES",
                 "BARS & NIGHTLIFE", "BANQUET VENUES", "SIGHTS & ATTRACTIONS",
                 "NATURE & SPORTS", "MUSEUMS & GALLERIES", "SHOPPING", "TOTAL"])

    for i in range(len(exisiting_data[0])):

        station_index = station - 1

        df = pd.json_normalize(exisiting_data[0][i])

        station_values = [df['meta.station_location.lat'][0], df['meta.station_location.lon'][0]] + [0 for i in
                                                                                                     range(14)]
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



