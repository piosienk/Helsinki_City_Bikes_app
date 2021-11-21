import os

import pandas as pd

from Functions.Data_ETL_functions.Modelling_prep import calculate_graph_variables, split_ww_data, \
    calculate_main_attractions_variables, calculate_places_variables
from Functions.Data_ETL_functions.Places_ETL import places_data_download
from Functions.Data_ETL_functions.Stations_ETL import load_and_filter_data


def aggregate_stations_data(path="./Data/"):
    bikes_path = path + "Bikes/database.csv"
    metrics_path = path + "Bikes/stations_metrics_10.csv"
    weekend_data_path = path + "Bikes/df_weekend_from_2018.csv"
    working_day_data_path = path + "Bikes/df_working_from_2018.csv"
    main_attractions_path = path + "Main_Attractions/df_distance.csv"
    summary_places_02_path = path + "Places_data/summary_places_02.csv"
    summary_places_05_path = path + "Places_data/summary_places_05.csv"
    merged_places_02_path = path + "Places_data/merged_places_02.txt"
    merged_places_05_path = path + "Places_data/merged_places_05.txt"

    # Metrics calculation
    if not os.path.exists(metrics_path):
        df_raw = load_and_filter_data(bikes_path)
        df_station_metrics = calculate_graph_variables(df_raw)
        df_station_metrics.to_csv(metrics_path)
    else:
        df_station_metrics = pd.read_csv(metrics_path, index_col=0)

    # Bikes data preparation
    if not (os.path.exists(weekend_data_path) and os.path.exists(working_day_data_path)):
        df_weekend, df_working_day = split_ww_data()
        df_weekend.to_csv(weekend_data_path)
        df_working_day.to_csv(working_day_data_path)
    else:
        df_weekend = pd.read_csv(weekend_data_path, index_col=0)
        df_working_day = pd.read_csv(working_day_data_path, index_col=0)

    # Main Attractions calculation
    if not os.path.exists(main_attractions_path):
        df_distance = calculate_main_attractions_variables(df_station_metrics)
        df_distance.to_csv(main_attractions_path)
    else:
        df_distance = pd.read_csv(main_attractions_path, index_col=0)

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

    df_working_day['departure_latitude'] = df_working_day['departure_latitude'].round(decimals=8)
    df_working_day['departure_longitude'] = df_working_day['departure_longitude'].round(decimals=8)

    df_working_day['departure_latitude'] = df_working_day['departure_latitude'].round(decimals=6)
    df_working_day['departure_longitude'] = df_working_day['departure_longitude'].round(decimals=6)

    df_weekend['departure_latitude'] = df_weekend['departure_latitude'].round(decimals=8)
    df_weekend['departure_longitude'] = df_weekend['departure_longitude'].round(decimals=8)

    df_weekend['departure_latitude'] = df_weekend['departure_latitude'].round(decimals=6)
    df_weekend['departure_longitude'] = df_weekend['departure_longitude'].round(decimals=6)

    df_summary_places_05['station_lat'] = df_summary_places_05['station_lat'].round(decimals=8)
    df_summary_places_05['station_lon'] = df_summary_places_05['station_lon'].round(decimals=8)

    df_summary_places_05['station_lat'] = df_summary_places_05['station_lat'].round(decimals=6)
    df_summary_places_05['station_lon'] = df_summary_places_05['station_lon'].round(decimals=6)

    df_summary_places_02['station_lat'] = df_summary_places_02['station_lat'].round(decimals=8)
    df_summary_places_02['station_lon'] = df_summary_places_02['station_lon'].round(decimals=8)

    df_summary_places_02['station_lat'] = df_summary_places_02['station_lat'].round(decimals=6)
    df_summary_places_02['station_lon'] = df_summary_places_02['station_lon'].round(decimals=6)

    df_distance['Lat'] = df_distance['Lat'].round(decimals=8)
    df_distance['Lon'] = df_distance['Lon'].round(decimals=8)

    df_distance['Lat'] = df_distance['Lat'].round(decimals=6)
    df_distance['Lon'] = df_distance['Lon'].round(decimals=6)

    df_working_day = df_working_day.merge(df_station_metrics, how="left", left_on=["departure"],
                                          right_on=["station"], suffixes=(None, '_m'))
    df_working_day = df_working_day.merge(df_summary_places_05, how="left",
                                          left_on=["departure_latitude", "departure_longitude"],
                                          right_on=["station_lat", "station_lon"], suffixes=(None, "_5"))
    df_working_day = df_working_day.merge(df_summary_places_02, how="left",
                                          left_on=["departure_latitude", "departure_longitude"],
                                          right_on=["station_lat", "station_lon"], suffixes=("_5", "_2"))
    df_working_day = df_working_day.merge(df_distance, how="left",
                                          left_on=["departure_latitude", "departure_longitude"],
                                          right_on=["Lat", "Lon"])

    df_weekend = df_weekend.merge(df_station_metrics, how="left", left_on=["departure"],
                                  right_on=["station"], suffixes=(None, '_m'))
    df_weekend = df_weekend.merge(df_summary_places_05, how="left",
                                  left_on=["departure_latitude", "departure_longitude"],
                                  right_on=["station_lat", "station_lon"], suffixes=(None, '_5'))
    df_weekend = df_weekend.merge(df_summary_places_02, how="left",
                                  left_on=["departure_latitude", "departure_longitude"],
                                  right_on=["station_lat", "station_lon"], suffixes=('_5', '_2'))
    df_weekend = df_weekend.merge(df_distance, how="left", left_on=["departure_latitude", "departure_longitude"],
                                  right_on=["Lat", "Lon"])

    df_weekend.loc[:, "Neigh_1_number"] = df_weekend[["Neigh_1_name", "station"]].apply(
        lambda x: df_weekend.loc[df_weekend.station == x.Neigh_1_name,  "number"].values[0], axis=1)
    df_weekend.loc[:, "Neigh_2_number"] = df_weekend[["Neigh_2_name", "station"]].apply(
        lambda x: df_weekend.loc[df_weekend.station == x.Neigh_2_name,  "number"].values[0], axis=1)
    df_weekend.loc[:, "Neigh_3_number"] = df_weekend[["Neigh_3_name", "station"]].apply(
        lambda x: df_weekend.loc[df_weekend.station == x.Neigh_3_name,  "number"].values[0], axis=1)

    df_working_day.loc[:, "Neigh_1_number"] = df_working_day[["Neigh_1_name", "station"]].apply(
        lambda x: df_working_day.loc[df_working_day.station == x.Neigh_1_name,  "number"].values[0], axis=1)
    df_working_day.loc[:, "Neigh_2_number"] = df_working_day[["Neigh_2_name", "station"]].apply(
        lambda x: df_working_day.loc[df_working_day.station == x.Neigh_2_name,  "number"].values[0], axis=1)
    df_working_day.loc[:, "Neigh_3_number"] = df_working_day[["Neigh_3_name", "station"]].apply(
        lambda x: df_working_day.loc[df_working_day.station == x.Neigh_3_name,  "number"].values[0], axis=1)


    df_weekend.drop(['departure', 'departure_name', 'departure_latitude', 'departure_longitude', 'degree_centr', 'in_degree_centr',
                     'out_degree_centr', 'betwenneess_centr', 'eigenvector_centr', 'closeness_centr', 'pagerank_value',
                     'Neigh_1_name', 'Neigh_2_name', 'Neigh_3_name',
                     'station_lon_5', 'station_lat_5', 'station_lon_2', 'station_lat_2', 'Lat', 'Lon'], axis=1, inplace=True)
    df_working_day.drop(['departure', 'departure_name', 'departure_latitude', 'departure_longitude', 'degree_centr', 'in_degree_centr',
                         'out_degree_centr', 'betwenneess_centr', 'eigenvector_centr', 'closeness_centr',
                         'pagerank_value',
                          'Neigh_1_name', 'Neigh_2_name', 'Neigh_3_name',
                         'station_lon_5', 'station_lat_5', 'station_lon_2', 'station_lat_2', 'Lat', 'Lon'], axis=1,
                        inplace=True)

    df_weekend.to_csv('./Data/Modelling/weekend_modelling_dataset.csv')
    df_working_day.to_csv('./Data/Modelling/working_day_modelling_dataset.csv')

    return df_weekend, df_working_day
