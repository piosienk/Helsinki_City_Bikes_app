import pickle
from datetime import datetime, timedelta

import pandas as pd

from New_stations.Functions.Data_ETL_functions.Auxillary_module.Aux_functions import not_used
from New_stations.Functions.Data_ETL_functions.Stations_module.Stations_ETL import load_and_filter_data, \
    prepare_time_series_data


@not_used
def popular_stations_heatmap(df_working, df_weekend, path_to_save="./Data/Statistics/stations_merged.csv"):
    """
    :param df_weekend: weekend df
    :param df_working: working df
    :param path_to_save: Path to saved results
    :return: Heatmap with Helsinki map and bike network
    """

    df_merged = df_working.merge(
        df_weekend, on=["departure_latitude", "departure_longitude"], suffixes=("_working", "_weekend"))

    df_merged.loc[:, "number"] = df_merged.loc[:, "number_working"] + df_merged.loc[:, "number_weekend"]

    df_merged = df_merged.loc[:, ["departure_weekend", "departure_latitude", "departure_longitude", "number"]]
    df_merged.columns = ["station_name", "latitude", "longitude", "number"]
    df_merged.to_csv(path_to_save)

    return df_merged


def most_popular_in_periods(path_to_file="./Data/Bikes/database.csv",
                            path_to_save="./Data/Statistics/most_popular_in_periods.pickle"):
    """
    :param path_to_save: path_to_save: Path to saved results
    :param path_to_files: Path to main data directory
    :return: Aggregated statistics from last 7 days
    """

    df_bikes = load_and_filter_data(path_to_file)
    df_time_series, coord = prepare_time_series_data(df_bikes)

    most_recent_date = df_time_series.loc[:, "date"].max()
    last_recent_week = most_recent_date - timedelta(days=7)
    last_recent_month = most_recent_date - timedelta(days=30)
    last_recent_3months = most_recent_date - timedelta(days=90)
    last_recent_year = most_recent_date - timedelta(days=365)

    df_week_bikes = df_time_series.loc[df_time_series.loc[:, "date"] > last_recent_week, :]
    df_month_bikes = df_time_series.loc[df_time_series.loc[:, "date"] > last_recent_month, :]
    df_3months_bikes = df_time_series.loc[df_time_series.loc[:, "date"] > last_recent_3months, :]
    df_year_bikes = df_time_series.loc[df_time_series.loc[:, "date"] > last_recent_year, :]
    df_whole_sample_bikes = df_time_series

    list_df_bikes = [df_week_bikes, df_month_bikes, df_3months_bikes, df_year_bikes, df_whole_sample_bikes]
    list_df_agg_bikes = []

    for df in list_df_bikes:
        df_agg = df.groupby(["departure"])[["departure", "number"]].sum()
        df_agg = df_agg.sort_values(by=["number"], ascending=False)
        df_agg = df_agg.reset_index()
        df_agg = df_agg.merge(coord, left_on="departure", right_on="departure_name")
        df_agg = df_agg.drop(columns=["departure_name"])
        list_df_agg_bikes.append(df_agg)

    file_to_store = open(path_to_save, "wb")
    pickle.dump(list_df_agg_bikes, file_to_store)
    file_to_store.close()

    return list_df_agg_bikes


def most_popular_connections(path_to_file="./Data/Bikes/database.csv",
                             path_to_save="./Data/Statistics/most_popular_connections.pickle"):
    """
    :param path_to_save: path_to_save: Path to saved results
    :param path_to_files: Path to main data directory
    :return: Aggregated statistics from last 7 days
    """

    df_bikes = load_and_filter_data(path_to_file)

    most_recent_date = df_bikes.loc[:, "departure"].max()
    last_recent_week = most_recent_date - timedelta(days=7)
    last_recent_month = most_recent_date - timedelta(days=30)
    last_recent_3months = most_recent_date - timedelta(days=90)
    last_recent_year = most_recent_date - timedelta(days=365)

    df_week_bikes = df_bikes.loc[df_bikes.loc[:, "departure"] > last_recent_week, :]
    df_month_bikes = df_bikes.loc[df_bikes.loc[:, "departure"] > last_recent_month, :]
    df_3months_bikes = df_bikes.loc[df_bikes.loc[:, "departure"] > last_recent_3months, :]
    df_year_bikes = df_bikes.loc[df_bikes.loc[:, "departure"] > last_recent_year, :]
    df_whole_sample_bikes = df_bikes

    list_df_bikes = [df_week_bikes, df_month_bikes, df_3months_bikes, df_year_bikes, df_whole_sample_bikes]
    list_df_agg_bikes = []

    for df in list_df_bikes:
        df_agg = df.groupby(["departure_name", "return_name"]).size().reset_index()
        df_agg.columns = ["departure_name", "return_name", "number"]
        df_agg = df_agg.sort_values(by=["number"], ascending=False)
        df_agg = df_agg.reset_index()
        list_df_agg_bikes.append(df_agg)

    file_to_store = open(path_to_save, "wb")
    pickle.dump(list_df_agg_bikes, file_to_store)
    file_to_store.close()

    return list_df_agg_bikes
