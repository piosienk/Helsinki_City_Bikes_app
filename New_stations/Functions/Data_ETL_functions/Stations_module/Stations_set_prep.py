import datetime as dt
import json
import os
import networkx as nx
import osmnx as ox
import geopy.distance
import pandas as pd

from New_stations.Functions.Data_ETL_functions.Auxillary_module.Aux_functions import not_used
from New_stations.Functions.Data_ETL_functions.Joint_ETL_module.Places_ETL import get_distance_to_attractions, \
    places_data_transformation

import New_stations.Functions.Data_ETL_functions.Stations_module.Stations_ETL as pre_processing


def split_ww_data(file="./Data/Bikes/database.csv", save_path="./Data/"):
    """
    :param save_path: path to save results
    :param file: path to .csv file with main bikes data table

    Functions filters and then aggregates bike rides information on the station level

    :return: 2 dataframes with average number of bike rides in weekends and the rest of week (on station level)
    """

    df_raw = pre_processing.load_and_filter_data(file)
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

    save_working_file = save_path + "Bikes/stations_working_data.csv"
    save_weekend_file = save_path + "Bikes/stations_weekend_data.csv"

    df_working_numbers.to_csv(save_working_file)
    df_weekend_numbers.to_csv(save_weekend_file)

    return df_weekend_numbers, df_working_numbers


def calculate_graph_variables(df_raw, path="./Data/", threshold=10):
    """
    :param path: save path
    :param threshold: minimal number of connections (bike rides between stations) needed to create edge between points
    :param df_raw: raw dataframe with bike rides data

    Function calculates graph metrics for each station and searches 3 nearest stations (absolute distance) in existing
    network. Then, each stations is assigned with graph metrics of these 3 neighbour stations.

    :return: dataframe with existing stations and graph metrics of 3 nearest stations
    """

    path += "Metrics/stations_metrics_" + str(threshold) + ".csv"
    # path = "./Data/Metrics/stations_metrics_" + str(threshold) + ".csv"

    df_metrics = pre_processing.get_graph_metrics_for_network(df_raw, threshold=threshold)
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

                    # 2 -> 3
                    df_neighbour_metrics.iloc[i, 12] = df_neighbour_metrics.iloc[i, 11]
                    df_neighbour_metrics.iloc[i, 15] = df_neighbour_metrics.iloc[i, 14]

                    # 1 -> 2
                    df_neighbour_metrics.iloc[i, 11] = df_neighbour_metrics.iloc[i, 10]
                    df_neighbour_metrics.iloc[i, 14] = df_neighbour_metrics.iloc[i, 13]

                    # new -> 1

                    df_neighbour_metrics.iloc[i, 10] = distance
                    df_neighbour_metrics.iloc[i, 13] = df_neighbour_metrics.iloc[j, 0]
                    continue

                elif distance < df_neighbour_metrics.iloc[i, 11]:

                    # 2 -> 3
                    df_neighbour_metrics.iloc[i, 12] = df_neighbour_metrics.iloc[i, 11]
                    df_neighbour_metrics.iloc[i, 15] = df_neighbour_metrics.iloc[i, 14]

                    # new -> 2

                    df_neighbour_metrics.iloc[i, 11] = distance
                    df_neighbour_metrics.iloc[i, 14] = df_neighbour_metrics.iloc[j, 0]
                    continue

                elif distance < df_neighbour_metrics.iloc[i, 12]:

                    # new -> 3

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

    df_neighbour_metrics.to_csv(path)

    return df_neighbour_metrics


@not_used
def calculate_graph_variables_real_distances(df_raw, threshold=50):
    """
    :param threshold: minimal number of connections (bike rides between stations) needed to create edge between points
    :param df_raw: raw dataframe with bike rides data

    Function calculates graph metrics for each station and searches 3 nearest stations (real distance) in existing
    network. Then, each stations is assigned with graph metrics of these 3 neighbour stations.

    :return: dataframe with existing stations and graph metrics of 3 nearest stations
    """

    df_metrics = pre_processing.get_graph_metrics_for_network(df_raw, threshold=threshold)
    df_neighbour_metrics = df_metrics.copy()
    df_neighbour_metrics.loc[:, "Neigh_1_dist"] = 100
    df_neighbour_metrics.loc[:, "Neigh_2_dist"] = 100
    df_neighbour_metrics.loc[:, "Neigh_3_dist"] = 100

    df_neighbour_metrics.loc[:, "Neigh_1_name"] = "Null"
    df_neighbour_metrics.loc[:, "Neigh_2_name"] = "Null"
    df_neighbour_metrics.loc[:, "Neigh_3_name"] = "Null"

    # Retrieve bike roads data from area of interest

    if not os.path.exists("./Data/Graph/Helsinki_roads_graph.graphml"):
        G = ox.graph_from_bbox(60.13441209253857, 60.26612675692856, 24.667035463003632, 25.160530279975347,
                               network_type='bike')
        ox.save_graphml(G, "./Data/Graph/Helsinki_roads_graph.graphml")
    else:
        G = ox.load_graphml("./Data/Graph/Helsinki_roads_graph.graphml")

    path = "./Data/Graph/Nearest_nodes_in_graph_existing_station.csv"

    if not os.path.exists(path):

        list_of_nearest_nodes_in_graph = []
        for i in range(len(df_neighbour_metrics)):
            print("Graph iteration number: ", i)
            coord1 = (df_neighbour_metrics.iloc[i, 2], df_neighbour_metrics.iloc[i, 1])  # First Lon, then Lat
            nearest_node_index = ox.distance.nearest_nodes(G, coord1[0], coord1[1], return_dist=False)
            list_of_nearest_nodes_in_graph.append(nearest_node_index)

        df_of_nearest_nodes_in_graph = pd.DataFrame(list_of_nearest_nodes_in_graph,
                                                    columns=["nearest_node_index"])
        df_of_nearest_nodes_in_graph.loc[:, "Lat"] = df_neighbour_metrics.iloc[:, 1]
        df_of_nearest_nodes_in_graph.loc[:, "Lon"] = df_neighbour_metrics.iloc[:, 2]
        df_of_nearest_nodes_in_graph.to_csv(path)

    df_of_nearest_nodes_in_graph_station = pd.read_csv("./Data/Locations/Nearest_nodes_in_graph_existing_station.csv",
                                                       index_col=0)
    list_of_nearest_nodes_in_graph_station = df_of_nearest_nodes_in_graph_station.loc[:, "nearest_node_index"].tolist()

    for i in range(len(df_neighbour_metrics)):
        print(i)
        distances_to = nx.shortest_path_length(G, source=list_of_nearest_nodes_in_graph_station[i], weight='length')
        distances_from = nx.shortest_path_length(G, target=list_of_nearest_nodes_in_graph_station[i], weight='length')

        distances_to_mod = {k: distances_to[k] for k in list(distances_to)[1:]}
        distances_from_mod = {k: distances_from[k] for k in list(distances_from)[1:]}

        distances_to_mod2 = {k: distances_to_mod[k] for k in distances_to_mod if
                             k in list_of_nearest_nodes_in_graph_station}
        distances_from_mod2 = {k: distances_from_mod[k] for k in distances_from_mod if
                               k in list_of_nearest_nodes_in_graph_station}

        distances_to_top = {k: distances_to_mod2[k] for k in list(distances_to_mod2)[0:8]}
        distances_from_top = {k: distances_from_mod2[k] for k in list(distances_from_mod2)[0:8]}

        df_to = pd.DataFrame.from_dict(distances_to_top, orient="index").reset_index()
        df_from = pd.DataFrame.from_dict(distances_from_top, orient="index").reset_index()

        df_to_from = pd.concat([df_to, df_from])
        df_to_from.columns = ["node_index", "dist"]

        df_top_values = df_to_from.sort_values(by="dist").drop_duplicates(subset="node_index").head(3)

        neigh_1_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[0, 0])
        neigh_2_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[1, 0])
        neigh_3_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[2, 0])

        neigh_1_distance = df_top_values.iloc[0, 1]
        neigh_2_distance = df_top_values.iloc[1, 1]
        neigh_3_distance = df_top_values.iloc[2, 1]

        # Assign 3
        df_neighbour_metrics.iloc[i, 12] = neigh_3_distance / 1000
        df_neighbour_metrics.iloc[i, 15] = df_neighbour_metrics.iloc[neigh_3_index, 0]

        # Assign 2
        df_neighbour_metrics.iloc[i, 11] = neigh_2_distance / 1000
        df_neighbour_metrics.iloc[i, 14] = df_neighbour_metrics.iloc[neigh_2_index, 0]

        # Assign 1
        df_neighbour_metrics.iloc[i, 10] = neigh_1_distance / 1000
        df_neighbour_metrics.iloc[i, 13] = df_neighbour_metrics.iloc[neigh_1_index, 0]

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

    df_neighbour_metrics.to_csv("./Data/Metrics/stations_metrics_50.csv")

    return df_neighbour_metrics


@not_used
def calculate_main_attractions_variables(df, point_type="station"):
    """
    :param df: Dataframe with all existing stations
    :param point_type: location - new point from grid or station - existing point from the network

    For each attraction, function measures the distance (absolute distance) from the station using graph from the osmnx
    library. Then, the result is appended to the predefined dataframe that keeps data for all points from df

    :return: dataframe with absolute distances to the main attractions (for all existing stations)
    """

    df_distances = []

    for i in range(len(df)):
        if point_type == "station":
            distance = get_distance_to_attractions(df.iloc[i, 1:3], )[["Name", "distance"]]
        else:
            distance = get_distance_to_attractions(df.iloc[i, 0:2], )[["Name", "distance"]]

        distance.index = distance.Name
        distance = distance.drop(columns="Name").transpose().reset_index()
        if point_type == "station":
            distance[["Lat", "Lon"]] = df.iloc[i, 1:3]
        else:
            distance[["Lat", "Lon"]] = df.iloc[i, 0:2]

        df_distances.append(distance.iloc[0, :])

    df_distances = pd.DataFrame(df_distances, columns=distance.columns).iloc[:, 1:]

    if point_type == "station":
        df_distances.to_csv("./Data/Main_Attractions/to_stations_distance.csv")
    else:
        df_distances.to_csv("./Data/Main_Attractions/to_location_distance.csv")

    return df_distances


def calculate_main_attractions_variables_real_distance(df, G=None, point_type="station"):
    """
    :param df: Dataframe with all existing stations
    :param G: Graph built on the existing stations network
    :param point_type: location - new point from grid or station - existing point from the network

    For each attraction, function measures the distance (for a bike ride) from the station using graph from the osmnx
    library. Then, the result is appended to the predefined dataframe that keeps data for all points from df

    :return: dataframe with real-life distances to the main attractions (for all stations)
    """

    if G is None:

        if not os.path.exists("./Data/Graph/Helsinki_roads_graph.graphml"):
            G = ox.graph_from_bbox(60.13441209253857, 60.26612675692856, 24.667035463003632, 25.160530279975347,
                                   network_type='bike')
            ox.save_graphml(G, "./Data/Graph/Helsinki_roads_graph.graphml")

        else:
            G = ox.load_graphml("./Data/Graph/Helsinki_roads_graph.graphml")

    if point_type == "station":
        nearest_nodes_list = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_existing_station.csv", index_col=0)
    else:
        nearest_nodes_list = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_new_locations.csv", index_col=0)

    nearest_nodes_list['Lat'] = nearest_nodes_list['Lat'].round(decimals=8)
    nearest_nodes_list['Lon'] = nearest_nodes_list['Lon'].round(decimals=8)

    nearest_nodes_list['Lat'] = nearest_nodes_list['Lat'].round(decimals=6)
    nearest_nodes_list['Lon'] = nearest_nodes_list['Lon'].round(decimals=6)

    df["latitude"] = df['latitude'].round(decimals=8)
    df["longitude"] = df['longitude'].round(decimals=8)

    df["latitude"] = df['latitude'].round(decimals=6)
    df["longitude"] = df['longitude'].round(decimals=6)

    df_merged = df.merge(nearest_nodes_list, left_on=["latitude", "longitude"], right_on=["Lat", "Lon"], how="left")

    df_distances = []

    df_attractions = pd.read_csv("./Data/Main_Attractions/Attractions_with_index.csv", sep=",")
    df_attractions.columns = ['Name', 'Lat', 'Lon', 'Type', ' Year', "Node_index"]

    for i in range(len(df_merged)):

        print("Iteration number: ", i)

        distance = get_distance_to_attractions(G, df_merged.loc[:, "nearest_node_index"].iloc[i], df=df_attractions)
        distance = distance[["Name", "distance"]]
        distance.index = distance.Name
        distance = distance.drop(columns="Name").transpose().reset_index()
        if point_type == "station":
            distance[["Lat", "Lon"]] = df.iloc[i, 1:3]
        else:
            distance[["Lat", "Lon"]] = df.iloc[i, 0:2]

        df_distances.append(distance.iloc[0, :])

    df_distances = pd.DataFrame(df_distances, columns=distance.columns).iloc[:, 1:]

    if point_type == "station":
        df_distances.to_csv("./Data/Main_Attractions/to_stations_distance.csv")
    else:
        df_distances.to_csv("./Data/Main_Attractions/to_location_distance.csv")

    return df_distances
