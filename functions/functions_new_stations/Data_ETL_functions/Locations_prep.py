import datetime as dt
import json
import geopy.distance
import pandas as pd
from Functions.Data_ETL_functions.Places_ETL import get_distance_to_attractions, places_data_transformation
import Functions.Data_ETL_functions.Stations_ETL as pre_processing


def calculate_graph_variables_for_new_locations(df_metrics, df_locations, threshold=10):
    df_locations_metrics = df_locations.copy()

    df_locations_metrics.loc[:, "Neigh_1_dist"] = 100
    df_locations_metrics.loc[:, "Neigh_2_dist"] = 100
    df_locations_metrics.loc[:, "Neigh_3_dist"] = 100

    df_locations_metrics.loc[:, "Neigh_1_name"] = "Null"
    df_locations_metrics.loc[:, "Neigh_2_name"] = "Null"
    df_locations_metrics.loc[:, "Neigh_3_name"] = "Null"

    for i in range(len(df_locations_metrics)):
        print("Iteration number: ", i)
        for j in range(len(df_metrics)):
            coord1 = (df_locations_metrics.iloc[i, 0], df_locations_metrics.iloc[i, 1])
            coord2 = (df_metrics.iloc[j, 1], df_metrics.iloc[j, 2])
            distance = geopy.distance.distance(coord1, coord2).km

            if distance < df_locations_metrics.iloc[i, 4]:
                df_locations_metrics.iloc[i, 4] = distance
                df_locations_metrics.iloc[i, 7] = df_metrics.iloc[j, 0]
                continue

            elif distance < df_locations_metrics.iloc[i, 5]:
                df_locations_metrics.iloc[i, 5] = distance
                df_locations_metrics.iloc[i, 8] = df_metrics.iloc[j, 0]
                continue

            elif distance < df_locations_metrics.iloc[i, 6]:
                df_locations_metrics.iloc[i, 6] = distance
                df_locations_metrics.iloc[i, 9] = df_metrics.iloc[j, 0]

    metric_names = df_metrics.columns[3:9]
    metric_names1 = [x + "1" for x in metric_names]
    metric_names2 = [x + "2" for x in metric_names]
    metric_names3 = [x + "3" for x in metric_names]

    metrics_neighbour1 = df_locations_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_1_name, metric_names].values[0], axis=1,
        result_type="expand")
    metrics_neighbour2 = df_locations_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_2_name, metric_names].values[0], axis=1,
        result_type="expand")
    metrics_neighbour3 = df_locations_metrics.apply(
        lambda x: df_metrics.loc[df_metrics["station"] == x.Neigh_3_name, metric_names].values[0], axis=1,
        result_type="expand")

    df_locations_metrics[metric_names1] = metrics_neighbour1
    df_locations_metrics[metric_names2] = metrics_neighbour2
    df_locations_metrics[metric_names3] = metrics_neighbour3

    path = "./Data/Bikes/locations_metrics_" + str(threshold) + ".csv"
    df_locations_metrics.to_csv(path)

    return df_locations_metrics