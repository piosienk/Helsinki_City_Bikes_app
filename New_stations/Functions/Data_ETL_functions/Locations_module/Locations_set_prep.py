import os
import geopy.distance
import networkx as nx
import osmnx as ox
import pandas as pd

from New_stations.Functions.Data_ETL_functions.Auxillary_module.Aux_functions import not_used


def calculate_graph_variables_for_new_locations(df_metrics, df_locations, threshold=10):
    """
    :param df_metrics: dataframe with graph metrics calculated for each station
    :param df_locations: dataframe with coordinates for each location
    :param threshold: minimal number of connections (bike rides between stations) needed to create edge between points

    Function takes coordinates from df_locations and searches 3 nearest stations (absolute distance) in existing
    network (for each point). Then, each locations is assigned with graph metrics of these 3 stations.

    :return: dataframe with locations and graph metrics of 3 nearest stations
    """

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

                # 2 -> 3
                df_locations_metrics.iloc[i, 6] = df_locations_metrics.iloc[i, 5]
                df_locations_metrics.iloc[i, 9] = df_locations_metrics.iloc[i, 8]

                # 1 -> 2
                df_locations_metrics.iloc[i, 5] = df_locations_metrics.iloc[i, 4]
                df_locations_metrics.iloc[i, 8] = df_locations_metrics.iloc[i, 7]

                # new -> 1
                df_locations_metrics.iloc[i, 4] = distance
                df_locations_metrics.iloc[i, 7] = df_metrics.iloc[j, 0]
                continue

            elif distance < df_locations_metrics.iloc[i, 5]:

                # 2 -> 3
                df_locations_metrics.iloc[i, 6] = df_locations_metrics.iloc[i, 5]
                df_locations_metrics.iloc[i, 9] = df_locations_metrics.iloc[i, 8]

                df_locations_metrics.iloc[i, 5] = distance
                df_locations_metrics.iloc[i, 8] = df_metrics.iloc[j, 0]
                continue

            elif distance < df_locations_metrics.iloc[i, 6]:

                # new -> 3
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

    path = "./Data/Metrics/locations_metrics_" + str(threshold) + ".csv"
    df_locations_metrics.to_csv(path)

    return df_locations_metrics


@not_used
def calculate_graph_variables_for_new_locations_real_distances(df_metrics, df_locations, threshold=10):
    """
    :param df_metrics: dataframe with graph metrics calculated for each station
    :param df_locations: dataframe with coordinates for each location
    :param threshold: minimal number of connections (bike rides between stations) needed to create edge between points

    Function takes coordinates from df_locations and searches 3 nearest stations (REAL LIFE distance) in existing
    network (for each point). Then, each locations is assigned with graph metrics of these 3 stations.

    :return: dataframe with locations and graph metrics of 3 nearest stations
    """

    # Retrieve bike roads data from area of interest

    if not os.path.exists("./Data/Graph/Helsinki_roads_graph.graphml"):
        G = ox.graph_from_bbox(60.13441209253857, 60.26612675692856, 24.667035463003632, 25.160530279975347,
                               network_type='bike')
        ox.save_graphml(G, "./Data/Graph/Helsinki_roads_graph.graphml")
    else:
        G = ox.load_graphml("./Data/Graph/Helsinki_roads_graph.graphml")

    df_locations_metrics = df_locations.copy()

    for path in ["./Data/Graph/Nearest_nodes_in_graph_new_locations.csv",
                 "./Data/Graph/Nearest_nodes_in_graph_existing_station.csv"]:

        if path == "./Data/Graph/Nearest_nodes_in_graph_new_locations.csv":
            df_point = df_locations_metrics
        else:
            df_point = df_metrics.iloc[:, 1:]

        if not os.path.exists(path):
            list_of_nearest_nodes_in_graph = []
            for i in range(len(df_point)):
                print("Graph iteration number: ", i)
                coord1 = (df_point.iloc[i, 1], df_point.iloc[i, 0])  # First Lon, then Lat
                nearest_node_index = ox.distance.nearest_nodes(G, coord1[0], coord1[1], return_dist=False)
                list_of_nearest_nodes_in_graph.append(nearest_node_index)

            df_of_nearest_nodes_in_graph = pd.DataFrame(list_of_nearest_nodes_in_graph,
                                                        columns=["nearest_node_index"])
            df_of_nearest_nodes_in_graph.loc[:, "Lat"] = df_point.iloc[:, 0]
            df_of_nearest_nodes_in_graph.loc[:, "Lon"] = df_point.iloc[:, 1]
            df_of_nearest_nodes_in_graph.to_csv(path)

    df_of_nearest_nodes_in_graph_location = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_new_locations.csv",
                                                        index_col=0)
    list_of_nearest_nodes_in_graph_location = df_of_nearest_nodes_in_graph_location.loc[:,
                                              "nearest_node_index"].tolist()

    df_of_nearest_nodes_in_graph_station = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_existing_station.csv",
                                                       index_col=0)
    list_of_nearest_nodes_in_graph_station = df_of_nearest_nodes_in_graph_station.loc[:, "nearest_node_index"].tolist()

    df_locations_metrics.loc[:, "Neigh_1_dist"] = 100
    df_locations_metrics.loc[:, "Neigh_2_dist"] = 100
    df_locations_metrics.loc[:, "Neigh_3_dist"] = 100

    df_locations_metrics.loc[:, "Neigh_1_name"] = "Null"
    df_locations_metrics.loc[:, "Neigh_2_name"] = "Null"
    df_locations_metrics.loc[:, "Neigh_3_name"] = "Null"

    if not os.path.exists("./Data/Location/locations_neigh.csv"):
        for i in range(12000, len(df_locations_metrics)):
            print("Iteration number: ", i)

            distances_to = nx.shortest_path_length(G, source=list_of_nearest_nodes_in_graph_location[i],
                                                   weight='length')
            # distances_from = nx.shortest_path_length(G, target=list_of_nearest_nodes_in_graph_location[i], weight='length')

            distances_to_mod = {k: distances_to[k] for k in list(distances_to)[1:]}
            # distances_from_mod = {k: distances_from[k] for k in list(distances_from)[1:]}

            distances_to_mod2 = {k: distances_to_mod[k] for k in distances_to_mod if
                                 k in list_of_nearest_nodes_in_graph_station}
            # distances_from_mod2 = {k: distances_from_mod[k] for k in distances_from_mod if
            #                       k in list_of_nearest_nodes_in_graph_station}

            distances_to_top = {k: distances_to_mod2[k] for k in list(distances_to_mod2)[0:8]}
            # distances_from_top = {k: distances_from_mod2[k] for k in list(distances_from_mod2)[0:8]}

            df_to = pd.DataFrame.from_dict(distances_to_top, orient="index").reset_index()
            # df_from = pd.DataFrame.from_dict(distances_from_top, orient="index").reset_index()

            # df_to_from = pd.concat([df_to, df_from])
            if len(df_to) < 3:
                print("Connections to less than 3 point - remove")
                continue
            # df_to_from.columns = ["node_index", "dist"]
            df_to.columns = ["node_index", "dist"]

            # df_top_values = df_to_from.sort_values(by="dist").drop_duplicates(subset="node_index").head(3)
            df_top_values = df_to.sort_values(by="dist").drop_duplicates(subset="node_index").head(3)

            neigh_1_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[0, 0])
            neigh_2_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[1, 0])
            neigh_3_index = list_of_nearest_nodes_in_graph_station.index(df_top_values.iloc[2, 0])

            neigh_1_distance = df_top_values.iloc[0, 1]
            neigh_2_distance = df_top_values.iloc[1, 1]
            neigh_3_distance = df_top_values.iloc[2, 1]

            # Assign 3
            df_locations_metrics.iloc[i, 6] = neigh_3_distance / 1000
            df_locations_metrics.iloc[i, 9] = df_metrics.iloc[neigh_3_index, 0]

            # Assign 2
            df_locations_metrics.iloc[i, 5] = neigh_2_distance / 1000
            df_locations_metrics.iloc[i, 8] = df_metrics.iloc[neigh_2_index, 0]

            # Assign 1
            df_locations_metrics.iloc[i, 4] = neigh_1_distance / 1000
            df_locations_metrics.iloc[i, 7] = df_metrics.iloc[neigh_1_index, 0]

            if i % 2000 == 0:
                df_locations_metrics.to_csv("./Data/Temp/df_locations_metrics_temp.csv")

        df_locations_metrics.to_csv("./Data/Locations/locations_neigh.csv")

    else:
        df_locations_metrics = pd.read_csv("./Data/Locations/locations_neigh.csv")

    df_locations_metrics = df_locations_metrics.loc[df_locations_metrics.loc[:, "Neigh_1_name"] != "Null", :]

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

    path = "./Data/Metrics/locations_metrics_" + str(threshold) + ".csv"
    df_locations_metrics.to_csv(path)

    return df_locations_metrics


def calculate_main_attractions_variables_real_distance_locations(df, G=None, point_type="location"):
    """
    :param df: Dataframe with all points (newly created from the grid)
    :param G: Graph built on the existing stations network
    :param point_type: location - new point from grid or station - existing point from the network

    For each attraction, function measures the distance (for a bike ride) from the station using graph from the osmnx
    library. Then, the result is appended to the predefined dataframe that keeps data for all points from df

    :return: dataframe with real-life distances to the main attractions (for all coordinates)
    """

    if G is None:
        G = ox.load_graphml("./Data/Locations/Helsinki_roads_graph.graphml")

    if point_type == "station":
        nearest_nodes_list = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_existing_station.csv", index_col=0)
    else:
        nearest_nodes_list = pd.read_csv("./Data/Graph/Nearest_nodes_in_graph_new_locations.csv", index_col=0)

    nearest_nodes_list['Lat'] = nearest_nodes_list['Lat'].round(decimals=8)
    nearest_nodes_list['Lon'] = nearest_nodes_list['Lon'].round(decimals=8)

    nearest_nodes_list['Lat'] = nearest_nodes_list['Lat'].round(decimals=6)
    nearest_nodes_list['Lon'] = nearest_nodes_list['Lon'].round(decimals=6)

    nearest_nodes_list = nearest_nodes_list.drop_duplicates()

    df['Lat'] = df['Lat'].round(decimals=8)
    df['Lon'] = df['Lon'].round(decimals=8)

    df['Lat'] = df['Lat'].round(decimals=6)
    df['Lon'] = df['Lon'].round(decimals=6)

    df_merged = df.merge(nearest_nodes_list, left_on=["Lat", "Lon"], right_on=["Lat", "Lon"], how="left")
    df_merged_nearest_node_index = df_merged.loc[:, "nearest_node_index"].tolist()

    df_distances = df_merged.copy()
    df_distances = df_distances.loc[:, ["Lat", "Lon", "nearest_node_index"]].drop_duplicates()

    df_attractions = pd.read_csv("./Data/Main_Attractions/Attractions_with_index.csv", sep=",")
    df_attractions.columns = ['Name', 'Lat', 'Lon', 'Type', ' Year', "Node_index"]

    for i in range(len(df_attractions)):
        print("Iteration number: ", i)

        distances_to = nx.shortest_path_length(G, source=df_attractions.loc[:, "Node_index"].iloc[i], weight='length')
        distances_from = nx.shortest_path_length(G, target=df_attractions.loc[:, "Node_index"].iloc[i], weight='length')

        distances_to_mod = {k: distances_to[k] for k in list(distances_to)}
        distances_from_mod = {k: distances_from[k] for k in list(distances_from)}

        distances_to_mod2 = {k: distances_to_mod[k] for k in distances_to_mod if k in df_merged_nearest_node_index}
        distances_from_mod2 = {k: distances_from_mod[k] for k in distances_from_mod if
                               k in df_merged_nearest_node_index}

        distances_to_mod2 = pd.DataFrame.from_dict(distances_to_mod2, orient="index").reset_index()
        distances_from_mod2 = pd.DataFrame.from_dict(distances_from_mod2, orient="index").reset_index()

        df_to_from = pd.concat([distances_to_mod2, distances_from_mod2])
        df_to_from.columns = ["node_index", df_attractions.loc[:, "Name"].iloc[i]]
        df_to_from.iloc[:, 1] = df_to_from.iloc[:, 1] / 1000
        df_to_from = df_to_from.sort_values(by=df_attractions.loc[:, "Name"].iloc[i]).drop_duplicates(
            subset="node_index")

        df_distances = df_distances.merge(df_to_from, left_on="nearest_node_index", right_on="node_index", how="left")
        df_distances = df_distances.drop(["node_index"], axis=1)

    df_distances = df_distances.dropna()
    df_distances = df_distances.drop(["nearest_node_index"], axis=1)

    if point_type == "station":
        df_distances.to_csv("./Data/Main_Attractions/to_stations_distance.csv")
    else:
        df_distances.to_csv("./Data/Main_Attractions/to_location_distance.csv")

    return df_distances
