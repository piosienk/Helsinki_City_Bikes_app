import datetime as dt
import json

import geopy.distance
import pandas as pd

from Functions.Data_ETL_functions.Places_ETL import get_distance_to_attractions, places_data_transformation

import Functions.Data_ETL_functions.Stations_ETL as pre_processing


def split_ww_data(path="./Data/Bikes/database.csv"):
    df_raw = pre_processing.load_and_filter_data(path)
    df, coord = pre_processing.prepare_time_series_data(df_raw)
    df_f = df[(df.cal_month >= 4) & (df.cal_month <= 10)].reset_index(drop=True)

    df_f.loc[:, "week"] = df_f.loc[:, ['cal_year', 'cal_month', 'cal_day', 'cal_hour']].apply(
        lambda x: dt.datetime(x.cal_year, x.cal_month, x.cal_day, x.cal_hour).isocalendar()[1], axis=1)
    df_f.loc[:, "weekday"] = df_f.loc[:, ['cal_year', 'cal_month', 'cal_day', 'cal_hour']].apply(
        lambda x: dt.datetime(x.cal_year, x.cal_month, x.cal_day, x.cal_hour).isocalendar()[2], axis=1)

    df_f.loc[:, "working_day"] = df_f.loc[:, ["weekday", "cal_hour"]].apply(
        lambda x: 1 if ((x.weekday < 6) and ((6 <= x.cal_hour <= 8) or (14 <= x.cal_hour <= 18))) else 0, axis=1)

    df_f.loc[:, "weekend"] = df_f.loc[:, ["weekday", "cal_hour"]].apply(
        lambda x: 1 if (((x.weekday == 5) and (x.cal_hour >= 20)) or (x.weekday == 6) or (
                (x.weekday == 7) and x.cal_hour <= 20)) else 0, axis=1)

    df_working = df_f[df_f.working_day == 1].drop(["weekday", 'working_day', 'weekend'], axis=1).reset_index(drop=True)
    df_weekend = df_f[df_f.weekend == 1].drop(["weekday", 'working_day', 'weekend'], axis=1).reset_index(drop=True)

    df_weekend_numbers = df_weekend.groupby(["departure", "cal_year", "week"])[["departure", "number"]].sum().groupby(
        level=0).mean()
    df_weekend_numbers = df_weekend_numbers.reset_index()
    df_weekend_numbers = df_weekend_numbers.merge(coord, how="left", left_on='departure', right_on='departure_name')

    df_working_numbers = df_working.groupby(["departure", "cal_year", "week"])[["departure", "number"]].sum().groupby(
        level=0).mean()
    df_working_numbers = df_working_numbers.reset_index()
    df_working_numbers = df_working_numbers.merge(coord, how="left", left_on='departure', right_on='departure_name')

    df_working_numbers.to_csv("./Data/Bikes/df_working_from_2018.csv")
    df_weekend_numbers.to_csv("./Data/Bikes/df_weekend_from_2018.csv")

    return df_weekend_numbers, df_working_numbers


def calculate_graph_variables(df_raw):
    df_metrics = pre_processing.get_graph_metrics_for_network(df_raw)
    df_neighbour_metrics = df_metrics.copy()
    df_neighbour_metrics.loc[:, "Neigh_1_dist"] = 100
    df_neighbour_metrics.loc[:, "Neigh_2_dist"] = 100
    df_neighbour_metrics.loc[:, "Neigh_3_dist"] = 100

    df_neighbour_metrics.loc[:, "Neigh_1_name"] = "Null"
    df_neighbour_metrics.loc[:, "Neigh_2_name"] = "Null"
    df_neighbour_metrics.loc[:, "Neigh_3_name"] = "Null"

    for i in range(len(df_neighbour_metrics)):
        for j in range(len(df_neighbour_metrics)):
            if i != j:
                coord1 = (df_neighbour_metrics.iloc[i, 1], df_neighbour_metrics.iloc[i, 2])
                coord2 = (df_neighbour_metrics.iloc[j, 1], df_neighbour_metrics.iloc[j, 2])
                distance = geopy.distance.distance(coord1, coord2).km

                if distance < df_neighbour_metrics.iloc[i, 10]:
                    df_neighbour_metrics.iloc[i, 10] = distance
                    df_neighbour_metrics.iloc[i, 13] = df_neighbour_metrics.iloc[j, 0]
                    continue

                elif distance < df_neighbour_metrics.iloc[i, 11]:
                    df_neighbour_metrics.iloc[i, 11] = distance
                    df_neighbour_metrics.iloc[i, 14] = df_neighbour_metrics.iloc[j, 0]
                    continue

                elif distance < df_neighbour_metrics.iloc[i, 12]:
                    df_neighbour_metrics.iloc[i, 12] = distance
                    df_neighbour_metrics.iloc[i, 15] = df_neighbour_metrics.iloc[j, 0]

    metric_names = df_metrics.columns[3:9]
    metric_names1 = [x + "1" for x in metric_names]
    metric_names2 = [x + "2" for x in metric_names]
    metric_names3 = [x + "3" for x in metric_names]

    metrics_neighbour1 = df_neighbour_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_1_name, metric_names].values[0], axis=1,
        result_type="expand")
    metrics_neighbour2 = df_neighbour_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_2_name, metric_names].values[0], axis=1,
        result_type="expand")
    metrics_neighbour3 = df_neighbour_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_3_name, metric_names].values[0], axis=1,
        result_type="expand")

    df_neighbour_metrics[metric_names1] = metrics_neighbour1
    df_neighbour_metrics[metric_names2] = metrics_neighbour2
    df_neighbour_metrics[metric_names3] = metrics_neighbour3

    df_neighbour_metrics.to_csv("./Data/Bikes/stations_metrics_10.csv")

    return df_neighbour_metrics


def calculate_main_attractions_variables(df, point_type="station"):
    df_distances = []

    for i in range(len(df)):
        if point_type == "station":
            distance = get_distance_to_attractions(df.iloc[i, 1:3])[["Name", "distance"]]
        else:
            distance = get_distance_to_attractions(df.iloc[i, 0:2])[["Name", "distance"]]

        distance.index = distance.Name
        distance = distance.drop(columns="Name").transpose().reset_index()
        if point_type == "station":
            distance[["Lat", "Lon"]] = df.iloc[i, 1:3]
        else:
            distance[["Lat", "Lon"]] = df.iloc[i, 0:2]

        df_distances.append(distance.iloc[0, :])

    df_distances = pd.DataFrame(df_distances, columns=distance.columns).iloc[:, 1:]

    if point_type == "station":
        df_distances.to_csv("./Data/Main_Attractions/df_distance.csv")
    else:
        df_distances.to_csv("./Data/Main_Attractions/df_location_distance.csv")

    return df_distances


def calculate_places_variables(places_data_path="./Data/Places_data/", radius=0.5):
    existing_coord_path_file = places_data_path + "merged_places_0" + str(int(radius * 10)) + ".txt"
    summary_coord_path_file = places_data_path + "summary_places_0" + str(int(radius * 10)) + ".csv"

    with open(existing_coord_path_file) as places_file:
        json_data = places_file.read()
        json_data = "[" + json_data + "]"

        exisiting_data = json.loads(json_data)
        places_file.close()

    df_all_stations = places_data_transformation(exisiting_data)

    df_all_stations.to_csv(summary_coord_path_file, index=False)

    return df_all_stations
