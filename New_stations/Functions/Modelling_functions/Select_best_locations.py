import numpy as np
import geopy.distance
import pandas as pd
import os


def calculate_score(df_existing_stations, df_locations, model="weekend"):
    """

    :param df_existing_stations: dataframe with existing stations
    :param df_locations: dataframe with proposed locations (after model prediction step)
    :param model: type of the model

    Function calculates distance-to-the-nearest-stations weighted scores

    :return: dataframe with proposed locations and distance weighted scores
    """

    df_mean_dist = (
                           df_existing_stations.Neigh_1_dist + df_existing_stations.Neigh_2_dist + df_existing_stations.Neigh_3_dist
                   ) / 3

    sq = df_mean_dist.value_counts()
    df_empirical_distribution = pd.DataFrame(sq.sort_index().cumsum() * 1. / len(sq))

    df_empirical_distribution = df_empirical_distribution.reset_index()
    df_empirical_distribution.columns = ["Distance", "P"]

    if model == "weekend":
        label = "label_weekend"
        score = "score_weekend"
    elif model == "working":
        label = "label_working"
        score = "score_working"
    else:
        raise ValueError("select proper model type: weekend or working")

    df_locations.loc[:, label] = df_locations.Label
    weights = []

    for i in range(len(df_locations)):
        mean_location_distance = df_locations.iloc[i, 2:5].mean()
        distribution_index = max(np.searchsorted(df_empirical_distribution.Distance, mean_location_distance) - 1, 0)
        weight = 1 - abs(1 - df_empirical_distribution.loc[distribution_index, "P"] * 2)
        weights.append(weight)

    df_locations.loc[:, "weights"] = weights
    df_locations.loc[:, score] = df_locations.loc[:, label] * df_locations.loc[:, "weights"]

    return df_locations.iloc[:, [0, 1, 2, 3, 4, 72, 73, 74]].drop_duplicates(), df_empirical_distribution


def check_if_prohibited_and_filter(df_locations, nearest_stations_distance,
                                   path_to_prohibited="./Data/Modelling/prohibited_areas.csv"):
    """

    :param df_locations: dataframe with new locations
    :param nearest_stations_distance: minimal distance to the existing stations. If new locations is closer than this
    threshold, it is removed from the df_locations
    :param path_to_prohibited: path to .csv file with prohibited areas coordinates

    Function performs filtration based on prohibited areas coordinates and minimum distance to existing stations

    :return: Filtered df_locations
    """

    df_locations.loc[:, "prohibited"] = 0

    prohibited = pd.read_csv(path_to_prohibited)
    for index, row in prohibited.iterrows():
        series = (df_locations["Lat"] <= row['max_lat']) & (
                df_locations["Lat"] >= row['min_lat']) & (
                         df_locations["Lon"] <= row['max_lon']) & (
                         df_locations["Lon"] >= row['min_lon'])

        df_locations.loc[series, "prohibited"] = 1

    df_locations_filter = df_locations.loc[df_locations.prohibited == 0, :]
    df_locations_filter = df_locations_filter.loc[df_locations.Neigh_1_dist > nearest_stations_distance, :]

    return df_locations_filter


def select_top_locations(df_locations, nearest_stations_distance, n=25):
    """

    :param df_locations: dataframe with scored locations
    :param nearest_stations_distance:  minimal distance to the existing stations. If new locations is closer than this
    threshold, it is removed from the df_locations
    :param n: number of locations in final list

    Function performs the following steps:
        n times:
            select location with the highest scores
            append locations to existing stations list
            update nearest station feature based on appended existing stations list
            filter locations with

    :return: list of the best locations for the new station
    """

    df_locations_filter = df_locations.reset_index(drop=True).copy()

    top_locations_list = []

    for k in range(n):

        new_location = df_locations_filter.sort_values('sum_score', ascending=False).iloc[0, [0, 1, -7, -4, -2]]
        top_locations_list.append(new_location)
        print(new_location)

        for j in range(len(df_locations_filter)):
            distance_to_new = geopy.distance.distance(
                (df_locations_filter.iloc[j, 0], df_locations_filter.iloc[j, 1]),
                (new_location.Lat, new_location.Lon)).km

            if distance_to_new < df_locations_filter.iloc[j, 2]:
                df_locations_filter.iloc[j, 4] = df_locations_filter.iloc[j, 3]
                df_locations_filter.iloc[j, 3] = df_locations_filter.iloc[j, 2]
                df_locations_filter.iloc[j, 2] = distance_to_new

            elif distance_to_new < df_locations_filter.iloc[j, 3]:
                df_locations_filter.iloc[j, 4] = df_locations_filter.iloc[j, 3]
                df_locations_filter.iloc[j, 3] = distance_to_new

            elif distance_to_new < df_locations_filter.iloc[j, 4]:
                df_locations_filter.iloc[j, 4] = distance_to_new

        df_locations_filter = df_locations_filter.loc[
                              df_locations_filter.Neigh_1_dist > nearest_stations_distance, :].reset_index(drop=True)

    df_top_locations = pd.DataFrame(top_locations_list).reset_index(drop=True)

    if os.path.exists("./Data/Results/df_top_locations.csv"):

        os.replace("./Data/Results/df_top_locations.csv", "./Data/Temp/old_df_top_locations.csv")
        df_top_locations.to_csv("./Data/Results/df_top_locations.csv", index=False)

    else:
        df_top_locations.to_csv("./Data/Results/df_top_locations.csv", index=False)

    return top_locations_list


def perform_selection(df_weekend, df_working, df_predicted_weekend, df_predicted_working,
                      nearest_stations_distance=0.4, n=25):
    """

    :param df_weekend: dataframe with aggregated weekend ride bikes between existing stations
    :param df_working: dataframe with aggregated working-day ride bikes between existing stations
    :param df_predicted_weekend: dataframe with locations and predicted scores for weekend rides
    :param df_predicted_working: dataframe with locations and predicted scores for working-day rides
    :param nearest_stations_distance:  minimal distance to the existing stations. If new locations is closer than this
    threshold, it is removed from the df_locations
    :param n: number of locations in final list

    Functions execute scoring and selections process using the above functions

    :return: list with selected locations for the new stations
    """

    df_locations_weekend_with_score, df_empirical_dist = calculate_score(df_weekend, df_predicted_weekend,
                                                                         model="weekend")
    df_locations_working_with_score, df_empirical_dist = calculate_score(df_working, df_predicted_working,
                                                                         model="working")

    df_locations_merged = df_locations_weekend_with_score.merge(
        df_locations_working_with_score, on=["Lat", "Lon", 'Neigh_1_dist', "Neigh_2_dist", "Neigh_3_dist", "weights"],
        how="left")
    df_locations_merged.loc[:, "sum_score"] = df_locations_merged.loc[:, "score_weekend"] + df_locations_merged.loc[:,
                                                                                            "score_working"]

    df_locations_filter = check_if_prohibited_and_filter(df_locations_merged,
                                                         nearest_stations_distance=nearest_stations_distance)
    df_locations_filter = df_locations_filter.reset_index(drop=True)

    top_locations_list = select_top_locations(df_locations_filter, nearest_stations_distance, n)

    return top_locations_list
