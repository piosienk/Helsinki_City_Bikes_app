import os

import pandas as pd

from New_stations.Functions.Data_ETL_functions.Locations_module.Locations_set_prep import \
    calculate_graph_variables_for_new_locations, \
    calculate_main_attractions_variables_real_distance_locations
from New_stations.Functions.Data_ETL_functions.Joint_ETL_module.Places_ETL import places_data_download, \
    calculate_places_variables


def aggregate_locations_data(path="./Data/", update_flag=False):
    """

    :param path: Path to main Data directory

    Function aggregates all information needed to predict number of bike rides from the proposed, new localizations.
    Merged data consists of: graph metrics, weekend and working day bike rides statistics, main attraction distances,
    aggregated data of places nearby localizations.

    :return: Dataframe prepared for model prediction (new localizations data)
    """

    locations_path = path + "Locations/locations_final_list.csv"

    metrics_path_old_stations = path + "Metrics/stations_metrics_50.csv"
    metrics_path_new_locations = path + "Metrics/locations_metrics_50.csv"
    weekend_data_path = path + "Bikes/stations_weekend_data.csv"
    working_day_data_path = path + "Bikes/stations_working_data.csv"
    main_attractions_path = path + "Main_Attractions/to_location_distance.csv"
    summary_places_02_path = path + "Places_data/summary_places_02.csv"
    summary_places_05_path = path + "Places_data/summary_places_05.csv"
    merged_places_02_path = path + "Places_data/merged_places_02.txt"
    merged_places_05_path = path + "Places_data/merged_places_05.txt"

    df_locations = pd.read_csv(locations_path, index_col=0)
    df_weekend = pd.read_csv(weekend_data_path, index_col=0)
    df_working_day = pd.read_csv(working_day_data_path, index_col=0)

    # Metrics calculation
    if not os.path.exists(metrics_path_new_locations) or update_flag:
        df_station_metrics = pd.read_csv(metrics_path_old_stations, index_col=0)
        df_location_metrics = calculate_graph_variables_for_new_locations(
            df_station_metrics, df_locations, threshold=50
        )
        df_location_metrics.to_csv(metrics_path_new_locations)
    else:
        df_location_metrics = pd.read_csv(metrics_path_new_locations, index_col=0)

    # Main Attractions calculation
    if not os.path.exists(main_attractions_path):
        df_location_distance = calculate_main_attractions_variables_real_distance_locations(df_location_metrics,
                                                                                            point_type="location")
        df_location_distance.to_csv(main_attractions_path)
    else:
        df_location_distance = pd.read_csv(main_attractions_path, index_col=0)

    # Places data calculation
    if not os.path.exists(merged_places_02_path):
        df_places_02 = places_data_download(radius=0.2)
        df_summary_places_02 = calculate_places_variables(radius=0.2)
        df_places_02.to_csv(merged_places_02_path)
        df_summary_places_02.to_csv(summary_places_02_path)

    elif not os.path.exists(summary_places_02_path):
        df_summary_places_02 = calculate_places_variables(radius=0.2)
        df_summary_places_02.to_csv(summary_places_02_path)
    else:
        df_summary_places_02 = pd.read_csv(summary_places_02_path, index_col=0)

    if not os.path.exists(merged_places_05_path):
        df_places_05 = places_data_download(radius=0.5)
        df_summary_places_05 = calculate_places_variables(radius=0.5)
        df_places_05.to_csv(merged_places_05_path)
        df_summary_places_05.to_csv(summary_places_05_path)

    elif not os.path.exists(summary_places_05_path):
        df_summary_places_05 = calculate_places_variables(radius=0.5)
        df_summary_places_05.to_csv(summary_places_05_path)

    else:
        df_summary_places_05 = pd.read_csv(summary_places_05_path, index_col=0)

    df_location_metrics['Lat'] = df_location_metrics['Lat'].round(decimals=8)
    df_location_metrics['Lon'] = df_location_metrics['Lon'].round(decimals=8)

    df_location_metrics['Lat'] = df_location_metrics['Lat'].round(decimals=6)
    df_location_metrics['Lon'] = df_location_metrics['Lon'].round(decimals=6)

    df_summary_places_05['station_lat'] = df_summary_places_05['station_lat'].round(decimals=8)
    df_summary_places_05['station_lon'] = df_summary_places_05['station_lon'].round(decimals=8)

    df_summary_places_05['station_lat'] = df_summary_places_05['station_lat'].round(decimals=6)
    df_summary_places_05['station_lon'] = df_summary_places_05['station_lon'].round(decimals=6)

    df_summary_places_02['station_lat'] = df_summary_places_02['station_lat'].round(decimals=8)
    df_summary_places_02['station_lon'] = df_summary_places_02['station_lon'].round(decimals=8)

    df_summary_places_02['station_lat'] = df_summary_places_02['station_lat'].round(decimals=6)
    df_summary_places_02['station_lon'] = df_summary_places_02['station_lon'].round(decimals=6)

    df_location_distance['Lat'] = df_location_distance['Lat'].round(decimals=8)
    df_location_distance['Lon'] = df_location_distance['Lon'].round(decimals=8)

    df_location_distance['Lat'] = df_location_distance['Lat'].round(decimals=6)
    df_location_distance['Lon'] = df_location_distance['Lon'].round(decimals=6)

    df_new_locations = df_location_metrics.merge(df_location_distance, how="right", left_on=["Lat", "Lon"],
                                                 right_on=["Lat", "Lon"]).drop_duplicates(inplace=False)

    df_new_locations = df_new_locations.merge(df_summary_places_05, how="left",
                                              left_on=["Lat", "Lon"], right_on=["station_lat", "station_lon"],
                                              suffixes=(None, "_5")).drop_duplicates(inplace=False)
    df_new_locations = df_new_locations.merge(df_summary_places_02, how="left",
                                              left_on=["Lat", "Lon"], right_on=["station_lat", "station_lon"],
                                              suffixes=("_5", "_2")).drop_duplicates(inplace=False)

    df_new_locations.loc[:, "Neigh_1_number_weekend"] = df_new_locations[["Neigh_1_name"]].apply(
        lambda x: df_weekend.loc[df_weekend.departure == x.Neigh_1_name, "number"].values[0], axis=1)
    df_new_locations.loc[:, "Neigh_2_number_weekend"] = df_new_locations[["Neigh_2_name"]].apply(
        lambda x: df_weekend.loc[df_weekend.departure == x.Neigh_2_name, "number"].values[0], axis=1)
    df_new_locations.loc[:, "Neigh_3_number_weekend"] = df_new_locations[["Neigh_3_name"]].apply(
        lambda x: df_weekend.loc[df_weekend.departure == x.Neigh_3_name, "number"].values[0], axis=1)

    df_new_locations.loc[:, "Neigh_1_number_working"] = df_new_locations[["Neigh_1_name"]].apply(
        lambda x: df_working_day.loc[df_working_day.departure == x.Neigh_1_name, "number"].values[0], axis=1)
    df_new_locations.loc[:, "Neigh_2_number_working"] = df_new_locations[["Neigh_2_name"]].apply(
        lambda x: df_working_day.loc[df_working_day.departure == x.Neigh_2_name, "number"].values[0], axis=1)
    df_new_locations.loc[:, "Neigh_3_number_working"] = df_new_locations[["Neigh_3_name"]].apply(
        lambda x: df_working_day.loc[df_working_day.departure == x.Neigh_3_name, "number"].values[0], axis=1)

    df_new_locations.drop(['Neigh_1_name', 'Neigh_2_name', 'Neigh_3_name',
                           'station_lon_5', 'station_lat_5', 'station_lon_2', 'station_lat_2', 'is_water'], axis=1,
                          inplace=True)

    save_path = path + "Modelling/locations_modelling_dataset.csv"

    df_new_locations.to_csv(save_path)

    return df_new_locations
