import pandas as pd
import numpy as np
from datetime import date, timedelta
import os
import json
import datetime as dt
import pickle
from fmiopendata.wfs import download_stored_query
import time
from functools import reduce


def historical_weather_load(end_date, date_format="%Y-%m-%d %H:%M:%S", path="./data/"):
    """
    end_date - historical weather observations till this date will be loaded
    date_format - format of the end_date
    path - path to directory with weather data

    function loads data from local directory (if exists), downloads required historical weather observations
    and merges data from both sources into one dictionary
    returns:
    hist_metadata_dict - dictionary with coordinates of all weather stations in the observed period
    hist_weather_dict - dictionary with dataframe for each weather station.
    """

    def align_dict(obs):
        """
        obs - string to modify

        function prepares string before transformation to dictionary

        returns:
        dictonary with weather parameters
        """

        mod_obs = str(obs).replace("'", '"')
        mod_obs = mod_obs.replace('nan', '"nan"')

        return json.loads(mod_obs)

    path_obs_file = path + "hist_weather_dict.pickle"  # dictionary with current meteo observations
    path_metadata_file = path + "hist_metadata_dict.pickle"  # dictionary with coordinates of meteo stations

    start_date = dt.datetime.strptime("2016-01-01 00:00:00", date_format)
    end_date = dt.datetime.strptime(end_date, date_format)

    if end_date > dt.datetime.now():
        print("Requested time period exceeds historical data range. Downloading observations from start date to now")

        end_date = dt.datetime.now()

    if os.path.exists(path_obs_file):

        print("Loading existing historial weather data")
        hist_weather_file = open(path_obs_file, "rb")
        hist_weather_dict = pickle.load(hist_weather_file)
        hist_weather_file.close()

        hist_metadata_file = open(path_metadata_file, "rb")
        hist_metadata_dict = pickle.load(hist_metadata_file)
        hist_metadata_file.close()

        # To assess from which time window we need to download weather observations, we need to find latest date
        # in the current datasets
        max_date = hist_weather_dict[next(iter(hist_weather_dict))].index.max()

        for station in hist_weather_dict.keys():

            station_max_date = hist_weather_dict[station].index.max()

            if station_max_date > max_date:
                max_date = station_max_date

        max_date_str = str(max_date)
        current_end_date = dt.datetime.strptime(max_date_str, '%Y-%m-%d %H:%M:%S')

        # If we have data for requested period, return dictionaries from local datasets
        if current_end_date >= end_date:

            print("Historical data for the requested period is available")

            return hist_metadata_dict, hist_weather_dict

        # If not, then observations from missing period will be downloaded
        else:
            start_date = current_end_date + dt.timedelta(hours=1)


    else:

        print("Historical weather data not found")
        hist_weather_dict = {}
        hist_metadata_dict = {}

    first_time = start_date - dt.timedelta(minutes=10)
    last_time = first_time + dt.timedelta(days=5)

    temp_weather_dict = {}
    temp_metadata_dict = {}

    # Data will be downloaded in 5 days intervals
    while last_time < (end_date + dt.timedelta(days=5)):
        # Convert times to properly formatted strings
        start_time_iso = first_time.isoformat(timespec="seconds") + "Z"
        end_time_iso = last_time.isoformat(timespec="seconds") + "Z"
        timestep = str(60)

        obs = download_stored_query("fmi::observations::weather::multipointcoverage",
                                    args=["bbox=24.7,60.1,25.2,60.3",
                                          "starttime=" + start_time_iso,
                                          "endtime=" + end_time_iso,
                                          "Timestep=" + timestep])

        temp_weather_dict.update(obs.data)  # add downloaded obs
        temp_metadata_dict.update(obs.location_metadata)  # update dictionary with locations

        time.sleep(2)  # sleep to avoid breaching API limit

        print(last_time)

        first_time = last_time
        last_time += dt.timedelta(days=5)

    # Update metadata files without any transformations
    hist_metadata_dict.update(temp_metadata_dict)

    hist_metadata_file = open(path_metadata_file, "wb")
    pickle.dump(hist_metadata_dict, hist_metadata_file)
    hist_metadata_file.close()

    # Update weather observations
    df_new_weather = pd.DataFrame.from_dict(temp_weather_dict, orient='index')  # dataframe with downloaded obs
    df_old_weather = pd.DataFrame.from_dict  # dataframe with obs from local file
    n_stations = len(df_new_weather.columns)

    new_hist_weather_dict = {}

    for i in range(n_stations):
        station = df_new_weather.iloc[:, i].apply(align_dict)
        station_df = station.apply(pd.Series)

        # retrieve values from dictionaries
        station_df = station_df.applymap(lambda x: x["value"] if type(x) == dict else x)

        # unify nan coding
        station_df = station_df.applymap(lambda x: float("NaN") if x == "nan" else x)

        # remove temporary columns
        station_df = station_df.loc[:, ['Air temperature',
                                        'Cloud amount', 'Dew-point temperature',
                                        'Gust speed', 'Horizontal visibility',
                                        'Precipitation amount', 'Precipitation intensity',
                                        'Present weather (auto)', 'Pressure (msl)',
                                        'Relative humidity', 'Snow depth',
                                        'Wind direction', 'Wind speed']]

        station_key = df_new_weather.columns[i]
        new_hist_weather_dict.update({station_key: station_df})

    # joining local and downloaded observations

    for station in new_hist_weather_dict.keys():

        # if station doesn't exist in current local file, new key/value pair is assigned
        if station not in hist_weather_dict.keys():
            hist_weather_dict[station] = new_hist_weather_dict[station]

        # if exists, merge observations for the particular station
        else:
            df_old = hist_weather_dict[station]
            df_new = new_hist_weather_dict[station]
            df_merged = pd.concat([df_old, df_new], ignore_index=False)
            hist_weather_dict[station] = df_merged

    # Write dictonary to local file
    hist_weather_file = open(path_obs_file, "wb")
    pickle.dump(hist_weather_dict, hist_weather_file)
    hist_weather_file.close()

    return #hist_metadata_dict, hist_weather_dict


def historical_weather_transformations(weather_dict, window_width=2, date_format="%Y-%m-%d %H:%M:%S"):
    """
    weather_dict - dictionary to transform
    window_width - width of moving window (in hours)
    date_format - format of the date

    Function takes dictonary with weather observations and transforms it into required time series format.
    Each row consists of average values from the last n hours of observations. Additionally, year, month, day,
    hour columns are created.
    returns:
    mod_weather_dict - dictionary with modified dataframes
    """

    mod_weather_dict = {}

    for station in weather_dict:
        df_weather = weather_dict[station].copy()

        # Removing columns that are not needed
        df_weather.drop(columns=['Horizontal visibility', 'Present weather (auto)',
                                 'Wind direction', 'Dew-point temperature', 'Precipitation intensity', ],
                        inplace=True, errors='ignore')

        # unify nan coding
        df_weather = df_weather.applymap(lambda x: float("NaN") if x == "nan" else x)
        df_weather.loc[df_weather['Snow depth'] < 0, 'Snow depth'] = 0

        # calculate average from last n hours of observations
        df_weather_rolled = df_weather.rolling(window_width, min_periods=1).mean()
        df_weather_rolled = df_weather_rolled.reset_index().rename(columns={'index': 'date'})

        # select observations only from the past
        df_weather_rolled = df_weather_rolled.loc[df_weather_rolled.date < dt.datetime.now(), :]

        # create datetime columms
        df_weather_rolled.loc[:, 'cal_year'] = df_weather_rolled['date'].dt.year
        df_weather_rolled.loc[:, 'cal_month'] = df_weather_rolled['date'].dt.month
        df_weather_rolled.loc[:, 'cal_day'] = df_weather_rolled['date'].dt.day
        df_weather_rolled.loc[:, 'cal_hour'] = df_weather_rolled['date'].dt.hour
        df_weather_rolled.columns = ['date', 'Temperature', 'Cloud Cover', 'Wind Gust',
                                     'Precipitation', 'Sea Level Pressure', 'Relative Humidity',
                                     'Snow Depth', 'Wind Speed', 'cal_year',
                                     'cal_month', 'cal_day', 'cal_hour']

        # parameters values as floats
        df_weather_rolled.iloc[:, 1:9] = df_weather_rolled.iloc[:, 1:9].astype("float")

        mod_weather_dict[station] = df_weather_rolled

        mod_hist_weather_file = open('./data/hist_mod_weather_dict.pickle', "wb")
        pickle.dump(mod_weather_dict, mod_hist_weather_file)
        mod_hist_weather_file.close()

    return #mod_weather_dict


def create_weather_stats(weather_dict, metadata_dict, column_list_default=True,
                         column_list_default_non_default=None):
    """
    weather_dict - dictionary to use
    metadata_dict - dicitonary with metadata
    column_list - on which columns averages should be calculated

    Function calculates average monthly values of weather parameters across all stations.
    They will be used in data imputation
    returns:
    df_stats - dataframe with average values
    """
    if column_list_default:
        column_list = ['Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation',
                       'Sea Level Pressure', 'Relative Humidity', 'Snow Depth', 'Wind Speed']
    else:
        column_list = column_list_default_non_default

    df_stats = pd.DataFrame(columns=column_list)


    # list of all dataframes from the dictionary
    dfs = [weather_dict[x] for x in metadata_dict]

    # merge columns from all dataframes
    df_concat = reduce(
        lambda left, right:
        pd.merge(left, right, on=["cal_year", "cal_month", "cal_day", "cal_hour"], how="outer"), dfs)

    df_concat = df_concat.drop(columns=['date_x', 'date_y'])
    df_concat_2 = df_concat.loc[df_concat.cal_hour % 2 == 1, :]
    df_concat_2.loc[:, 'date'] = df_concat_2.date - timedelta(hours=1)
    df_concat = df_concat.merge(df_concat_2, how = 'right', on = 'date')
    df_concat.loc[:, 'cal_year'] = df_concat['date'].dt.year
    df_concat.loc[:, 'cal_month'] = df_concat['date'].dt.month
    df_concat.loc[:, 'cal_day'] = df_concat['date'].dt.day
    df_concat.loc[:, 'cal_hour'] = df_concat['date'].dt.hour



    # for each parameter and observation, calculate average value across all stations. Then group observations
    # by month and calculate average on the monthly level

    for column in column_list:
        mean_column = "mean" + column
        df_concat.loc[:, mean_column] = df_concat.loc[:, [column + "_y", column + "_x", column + "_y_y",
                                                          column + "_x_y", column + "_x_x", column + "_y_x"]].mean(axis=1)
        df_stats[column] = df_concat.loc[:, ["cal_year", "cal_month", "cal_day", "cal_hour", mean_column]].groupby(["cal_year", "cal_month", "cal_day", "cal_hour"]).agg({mean_column: ['mean']}).reset_index(drop=True)
        #print(df_stats)
    df_stats.loc[:, 'cal_year'] = df_concat['cal_year']
    df_stats.loc[:, 'cal_month'] = df_concat['cal_month']
    df_stats.loc[:, 'cal_day'] = df_concat['cal_day']
    df_stats.loc[:, 'cal_hour'] = df_concat['cal_hour']
    df_stats.loc[:,'date'] = df_concat['date']
    df_stats.to_csv('./data/weather_time_series.csv')
    return

