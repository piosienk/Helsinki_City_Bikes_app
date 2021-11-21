import time
import pandas as pd
import numpy as np
from math import pi, cos
import requests
from requests.structures import CaseInsensitiveDict
import ast
import geopy.distance


def create_coordinates_grid(path="./Data/Bikes/", city_center_bound=None):
    np.random.seed(123)

    if city_center_bound is None:
        city_center_bound = [24.8976, 60.147921, 24.9859, 60.20659]

    bikes_path = path + "database.csv"

    df_bikes = pd.read_csv(bikes_path)

    df_coord = df_bikes.iloc[:, 9:11]
    df_coord = df_coord.drop_duplicates()

    # Calculate max and min coordinates in current city bikes network
    min_lat = min(df_coord.departure_latitude)
    min_lon = min(df_coord.departure_longitude)

    max_lat = max(df_coord.departure_latitude)
    max_lon = max(df_coord.departure_longitude)

    print("Current network boundaries")
    print([min_lon, min_lat])
    print([max_lon, max_lat])

    # Calculate Steps
    dy = 0.150  # in km
    dx = 0.150
    r_earth = 6362

    step_lat = (dy / r_earth) * (180 / pi)
    step_lon = (dx / r_earth) * (180 / pi) / cos(60.18 * pi / 180)

    # Searching boundaries
    min_lon_border = min_lon - 20 * step_lon
    min_lat_border = min_lat - 10 * step_lat

    max_lon_border = max_lon + 20 * step_lon
    max_lat_border = max_lat + 20 * step_lat

    print("Searching network boundaries")
    print([min_lon_border, min_lat_border])
    print([max_lon_border, max_lat_border])

    # Create Lists of coordinates
    float_range_lon = np.arange(min_lon_border, max_lon_border, step_lon)
    float_range_lat = np.arange(min_lat_border, max_lat_border, step_lat)

    df_new_coord = pd.DataFrame(columns=["Lon", "Lat"])

    for x in float_range_lon:
        for y in float_range_lat:
            df_new_coord = df_new_coord.append({"Lon": x, "Lat": y}, ignore_index=True)

    # Add 1000 random points in the city centre
    random_lon = np.random.uniform(low=city_center_bound[0], high=city_center_bound[2], size=(1000,))
    random_lat = np.random.uniform(low=city_center_bound[1], high=city_center_bound[3], size=(1000,))
    np.random.shuffle(random_lon)
    np.random.shuffle(random_lat)

    df_new_coord_random = pd.DataFrame(data={"Lon": random_lon, "Lat": random_lat}, columns=["Lon", "Lat"])

    # Save generated points
    gen_coord_path = "./Data/Locations/all_new_coord.csv"

    df_new_coord = df_new_coord.append(df_new_coord_random).reset_index(drop=True)
    df_new_coord.to_csv(gen_coord_path, index=False)

    return df_new_coord


def check_if_water(df_coord, path="./Data/Locations/"):
    time.sleep(5)

    df_is_water_coord = pd.DataFrame(columns=["Lat", "Lon", "is_water"])

    n_obs = len(df_coord)
    batches_total = n_obs // 3
    remainder = n_obs % 3

    i = 0
    batch_n = 1

    url = "https://api.onwater.io/api/v1/results"
    headers = CaseInsensitiveDict()
    params = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    params["access_token"] = "fDwwWjq5jNzPkWzUhff5"

    while batch_n <= batches_total:
        time.sleep(5)

        data = "[" + '"' + str(df_coord.Lat.iloc[i]) + "," + str(df_coord.Lon.iloc[i]) + '",'
        data += '"' + str(df_coord.Lat.iloc[i + 1]) + "," + str(df_coord.Lon.iloc[i + 1]) + '",'
        data += '"' + str(df_coord.Lat.iloc[i + 2]) + "," + str(df_coord.Lon.iloc[i + 2]) + '"' + "]"

        resp = requests.post(url, headers=headers, data=data, params=params)

        resp_text = resp.text.replace("true", "True")
        resp_text = resp_text.replace("false", "False")
        query_dict = ast.literal_eval(resp_text)

        for obs in query_dict:
            df_is_water_coord = df_is_water_coord.append({
                "Lon": obs["lon"],
                "Lat": obs["lat"],
                "is_water": obs["water"]}, ignore_index=True)

        print(resp)
        i += 3
        batch_n += 1

        if batch_n % 1000 == 0:
            path_temp_file = path + str(batch_n) + "_batch_is_water.csv"
            df_is_water_coord.to_csv(path_temp_file, index=False)

    if remainder == 1:
        time.sleep(5)
        data = "[" + '"' + str(df_coord.Lat.iloc[-1]) + "," + str(df_coord.Lon.iloc[-1]) + '"' + "]"

        resp = requests.post(url, headers=headers, data=data, params=params)

        resp_text = resp.text.replace("true", "True")
        resp_text = resp_text.replace("false", "False")
        query_dict_single = ast.literal_eval(resp_text)

        for obs in query_dict_single:
            df_is_water_coord = df_is_water_coord.append({
                "Lon": obs["lon"],
                "Lat": obs["lat"],
                "is_water": obs["water"]}, ignore_index=True)

        print(resp)

    if remainder == 2:
        time.sleep(5)
        data = "[" + '"' + str(df_coord.Lat.iloc[-2]) + "," + str(df_coord.Lon.iloc[-2]) + '",'
        data += '"' + str(df_coord.Lat.iloc[- 1]) + "," + str(df_coord.Lon.iloc[-1]) + '"' + "]"

        resp = requests.post(url, headers=headers, data=data, params=params)

        resp_text = resp.text.replace("true", "True")
        resp_text = resp_text.replace("false", "False")
        query_dict = ast.literal_eval(resp_text)

        for obs in query_dict:
            df_is_water_coord = df_is_water_coord.append({
                "Lon": obs["lon"],
                "Lat": obs["lat"],
                "is_water": obs["water"]}, ignore_index=True)

        print(resp)

    path_all_obs = path + "is_water.csv"
    df_is_water_coord.to_csv(path_all_obs, index=False)

    return df_is_water_coord


def get_distance_to_other(df_all_new):

    for i in range(len(df_all_new)):
        print(i)
        for j in range(max(0, i - 100), min(len(df_all_new), i + 100)):
            if j != i:
                coords_1 = [df_all_new.Lat[i], df_all_new.Lon[i]]
                coords_2 = [df_all_new.Lat[j], df_all_new.Lon[j]]

                distance = geopy.distance.distance(coords_1, coords_2).km

                if distance < df_all_new.loc[i, "Nearest_station"]:
                    df_all_new.loc[i, "Nearest_station"] = distance

    return df_all_new
