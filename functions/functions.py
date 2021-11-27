import pandas as pd
import numpy as np
from datetime import date, timedelta
import networkx as nx
import holidays
import requests
import pandas as pd
import datetime as dt
import os
import pickle
from datetime import timedelta
from pycaret.regression import *
from functions.ts_class import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from functions.metrics_class import mean_absolute_percentage_error, mean_absolute_scaled_error



# IMPORTANT!!!
# API limit is just 500 requests in MONTH
# And don't share API key ;)


def forecast_weather_download(path="./Data/Weather/Weather_data/"):
    """
    path - path to weather data directory

    Function downloads hourly weather forecast for Helsinki and saves it as pickle file.
    Additionally, standard df is returned.

    returns:
    df - dataframe with weather forecast

    """

    path_forecast_file = path + "forecast_weather_dict.pickle"  # dictionary with current meteo observations

    url = "https://visual-crossing-weather.p.rapidapi.com/forecast"

    querystring = {"location": "Helsinki", "aggregateHours": "1", "shortColumnNames": "false", "unitGroup": "metric",
                   "contentType": "csv"}

    headers = {
        'x-rapidapi-host': "visual-crossing-weather.p.rapidapi.com",
        'x-rapidapi-key': "e5ab711d2fmsh0f4bc5cae86a9d4p13bd4djsnfbdb24e92d16"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    data = response.text
    df = pd.DataFrame([x.split(',') for x in data.split('\n')])
    df = df.iloc[1:, :]
    df.columns = ["Address", "Date time", "Latitude", "Longitude", "Address", "Region", "Country", "Name",
                  "Wind Direction",
                  "Temperature", "Wind Speed", "Cloud Cover", "Heat Index", "Chance Precipitation (%)"
        , "Precipitation", "Sea Level Pressure", "Snow Depth", "Snow", "Relative Humidity",
                  "Wind Gust", "Wind Chill", "Conditions", "Note"]

    df.drop(columns=["Address", "Latitude", "Longitude",
                     "Address", "Region", "Country", "Name",
                     "Wind Direction", "Heat Index", "Chance Precipitation (%)",
                     "Snow", "Wind Chill", "Conditions", "Note"], inplace=True)

    forecast_weather_file = open(path_forecast_file, "wb")
    pickle.dump(df, forecast_weather_file)
    forecast_weather_file.close()

    return df


def forecast_weather_transformation(df, window_width=2):
    """
    df - dataframe with downloaded forecast
    window_width - width of moving window (in hours)

    Funtion takes forecast for the next 7 days (in 1-hour intervals) and aligns dataframe with historical weather table.
    Final dataframe consists of rolling averages (like in historical weather table)

    returns:
    df - transformed hourly forecast for Helsinki.

    """

    df_for = df.iloc[:-1, ].copy()  # remove last observation

    # Create calendar columns
    df_for.loc[:, 'date'] = df_for['Date time'].apply(lambda x: dt.datetime.strptime(x, '"%m/%d/%Y %H:%M:%S"'))

    # Take only weekly forecast
    df_for = df_for.iloc[:169, :]
    df_for.reset_index(inplace=True, drop=True)

    # Keep only necessary columns
    df_for.drop(columns=["Date time"], inplace=True)
    columns_list = ['date', 'Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation',
                    'Sea Level Pressure', 'Relative Humidity', 'Snow Depth', 'Wind Speed']

    # Align columns order like in the historical dataset
    df_for = df_for[columns_list]
    df_for.iloc[:, 1:9] = df_for.iloc[:, 1:9].astype("float")

    # Map from km/h to m/s
    df_for.loc[:, "Wind Gust"] = df_for.loc[:, "Wind Gust"] / 3.6
    df_for.loc[:, "Wind Speed"] = df_for.loc[:, "Wind Speed"] / 3.6

    # Map from cloud cover % to octane scale
    bins_list = [-1] + [(x * 0.125) * 100 for x in range(9)]
    labels = [x for x in range(9)]
    df_for.loc[:, "Cloud Cover"] = pd.cut(df_for["Cloud Cover"], bins_list, labels=labels, include_lowest=True)

    # Create moving average for the 2 hours intervals
    df_for_rolled = df_for.copy()
    df_for_rolled.iloc[:, 1:9] = df_for.iloc[:, 1:9].rolling(window_width, min_periods=1).mean()
    df_for_rolled.date = df_for_rolled.date + timedelta(hours=-1)
    df_for_rolled = df_for_rolled.iloc[2:, :]
    df_for_rolled.loc[:, 'cal_year'] = df_for_rolled['date'].dt.year
    df_for_rolled.loc[:, 'cal_month'] = df_for_rolled['date'].dt.month
    df_for_rolled.loc[:, 'cal_day'] = df_for_rolled['date'].dt.day
    df_for_rolled.loc[:, 'cal_hour'] = df_for_rolled['date'].dt.hour

    hl = pd.DataFrame()
    for date_day, name in sorted(holidays.Finland(years=[2020, 2021]).items()):
        hl_instance = pd.DataFrame({'date': [date_day], 'name': [name]})
        hl = pd.concat([hl, hl_instance])
    hl.date = hl.date.apply(pd.to_datetime, format='%Y-%m-%d')
    hl.loc[:, 'cal_year'] = hl['date'].dt.year
    hl.loc[:, 'cal_month'] = hl['date'].dt.month
    hl.loc[:, 'cal_day'] = hl['date'].dt.day
    hl = hl.reset_index(drop=True)
    hl.loc[:, 'holiday'] = 1
    for name in hl.name.unique():
        hl.loc[:, name] = 0
    for i in range(hl.shape[0]):
        cname = hl.loc[i, 'name']
        hl.loc[i, [cname]] = 1
    hls = ['holiday'] + hl.name.unique().tolist()
    hl = hl.loc[:, ['cal_year', 'cal_month', 'cal_day', 'holiday'] + hl.name.unique().tolist()]
    df_for_rolled = df_for_rolled.merge(hl, how='left',
                                              on=["cal_year", "cal_month", "cal_day"])
    for cname in hls:
        df_for_rolled.loc[df_for_rolled[cname].isna(), cname] = 0
        df_for_rolled[cname] = df_for_rolled[cname].astype(int)
    df_for_rolled = df_for_rolled.loc[df_for_rolled.cal_hour % 2 == 0, :].reset_index(drop=True)

    return df_for_rolled


def load_and_filter_data(path_to_file):
    """
    path_to_file - path to csv file with columns: departure_id, return_id, distance (m), duration (sec.),
                   avg_speed (km/h), departure, return, departure_name, return_name, departure_latitude,
                   departure_longitude, return_latitude, return_longitude
    function loads data and does some filtering based on set rules
    return: pandas dataframe with columns: departure, return, departure_name, return_name, departure_latitude,
            departure_longitude, return_latitude, return_longitude
    """
    df = pd.read_csv(path_to_file, dtype={'departure_id': str, 'return_id': str})
    df = df.loc[(df['distance (m)'] > 100) & (df['duration (sec.)'] < 18000) &
                (df['duration (sec.)'] > 60) & (df['avg_speed (km/h)'] > 0),
                ['departure', 'return', 'departure_name', 'return_name', 'departure_latitude', 'departure_longitude',
                 'return_latitude', 'return_longitude']].dropna().reset_index(drop=True)
    df[['departure', 'return']] = df[['departure', 'return']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
    return df


def prepare_time_series_data(input_dt, time_granularity_in_hours=2):
    df = input_dt.copy()
    df = df[['departure', 'departure_name']]
    df.loc[:, 'dep_year'] = df['departure'].dt.year
    df.loc[:,'dep_month'] = df['departure'].dt.month
    df.loc[:,'dep_day'] = df['departure'].dt.day
    df.loc[:,'dep_hour'] = df['departure'].dt.hour
    df.loc[:,'dep_hour'] = df.loc[:,'dep_hour'] - df.loc[:,'dep_hour'].mod(time_granularity_in_hours)
    df = df[['departure_name', 'dep_year', 'dep_month',
             'dep_day', 'dep_hour']].groupby(['departure_name', 'dep_year', 'dep_month',
                                              'dep_day', 'dep_hour']).size().reset_index().rename(columns =
                                                                                                  {0: 'number'})
    time_series_data = pd.DataFrame()
    for station in df.departure_name.unique():
        station_data = df.loc[df.departure_name == station,:]
        date_data = station_data[['dep_year', 'dep_month', 'dep_day']]
        date_data.columns = ['YEAR', 'MONTH','DAY']
        sdate = np.min(pd.to_datetime(date_data))
        edate = np.max(pd.to_datetime(date_data))
        calendar = pd.DataFrame({'date': pd.date_range(sdate, edate + timedelta(days=1),freq='2H')})
        calendar[['date']] = calendar[['date']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
        calendar.loc[:,'cal_year'] = calendar['date'].dt.year
        calendar.loc[:,'cal_month'] = calendar['date'].dt.month
        calendar.loc[:,'cal_day'] = calendar['date'].dt.day
        calendar.loc[:,'cal_hour'] = calendar['date'].dt.hour
        calendar = calendar.loc[calendar.cal_year > 2016,:]
        calendar = calendar.merge(station_data, how = 'left', left_on = ['cal_year', 'cal_month',
                                                                         'cal_day', 'cal_hour'],
                                  right_on = ['dep_year', 'dep_month', 'dep_day', 'dep_hour'])
        calendar = calendar[['date', 'cal_year', 'cal_month', 'cal_day', 'cal_hour', 'departure_name', 'number']]
        calendar.loc[:,'departure_name'] = station
        calendar = calendar.rename(columns = {'departure_name': 'departure'})
        calendar.loc[calendar.number.isna(),'number'] = 0
        calendar[['number']] = calendar[['number']].astype(int)
        time_series_data = pd.concat([time_series_data, calendar])
    return time_series_data.reset_index(drop = True)


def get_graph_metrics_for_network(df):
    """
    df - pandas dataframe with columns: departure_name, return_name, departure_latitude, departure_longitude
    function calculates values of multiple graph metrics
    return: pandas dataframe with station, lat, lon & value of graph metrics
    """
    G = nx.from_pandas_edgelist(df, source = 'departure_name', target = 'return_name', create_using = nx.DiGraph())
    degree = nx.degree_centrality(G)
    node_degree = [degree[i] for i in degree.keys()]
    in_degree = nx.in_degree_centrality(G)
    node_in_degree = [in_degree[i] for i in in_degree.keys()]
    out_degree = nx.out_degree_centrality(G)
    node_out_degree = [out_degree[i] for i in out_degree.keys()]
    betweenness = nx.betweenness_centrality(G)
    node_betweenness = [betweenness[i] for i in betweenness.keys()]
    eigenvector = nx.eigenvector_centrality(G)
    node_eigenvector = [eigenvector[i] for i in eigenvector.keys()]
    closeness = nx.closeness_centrality(G)
    node_closeness = [closeness[i] for i in closeness.keys()]
    pagerank = nx.pagerank(G)
    node_pagerank = [pagerank[i] for i in pagerank.keys()]
    centrality_df = pd.DataFrame({'station': [i for i in degree.keys()],
                                  'degree_centr' : node_degree,
                                  'in_degree_centr': node_in_degree,
                                  'out_degree_centr': node_out_degree,
                                  'betwenneess_centr': node_betweenness,
                                  'eigenvector_centr': node_eigenvector,
                                  'closeness_centr': node_closeness,
                                  'pagerank_value': node_pagerank})
    centrality_df = centrality_df.merge(df[['departure_name', 'departure_latitude',
                                        'departure_longitude']].drop_duplicates(),
                                    left_on=['station'], right_on = ['departure_name'])
    centrality_df = centrality_df[['station', 'departure_latitude', 'departure_longitude', 'degree_centr',
                               'in_degree_centr', 'out_degree_centr', 'betwenneess_centr', 'eigenvector_centr',
                              'closeness_centr', 'pagerank_value']]
    centrality_df = centrality_df.rename(columns = {'departure_latitude': 'latitude',
                                                    'departure_longitude': 'longitude'})
    return centrality_df


def transform_time_series(path_to_time_series, path_to_weather):
    """
    :param path_to_time_series:
    :param path_to_weather:
    :return:
    """
    time_series_data = pd.read_csv(path_to_time_series, index_col=0)
    weather_data = pd.read_csv(path_to_weather, index_col=0)
    time_series_data = time_series_data.merge(weather_data, how='left',
                                              on=["cal_year", "cal_month", "cal_day", "cal_hour"])
    hl = pd.DataFrame()
    for date_day, name in sorted(holidays.Finland(years=[2017, 2018, 2019, 2020]).items()):
        hl_instance = pd.DataFrame({'date': [date_day], 'name': [name]})
        hl = pd.concat([hl, hl_instance])
    hl.date = hl.date.apply(pd.to_datetime, format='%Y-%m-%d')
    hl.loc[:, 'cal_year'] = hl['date'].dt.year
    hl.loc[:, 'cal_month'] = hl['date'].dt.month
    hl.loc[:, 'cal_day'] = hl['date'].dt.day
    hl = hl.reset_index(drop=True)
    hl.loc[:, 'holiday'] = 1
    for name in hl.name.unique():
        hl.loc[:, name] = 0
    for i in range(hl.shape[0]):
        cname = hl.loc[i, 'name']
        hl.loc[i, [cname]] = 1
    hls = ['holiday'] + hl.name.unique().tolist()
    hl = hl.loc[:, ['cal_year', 'cal_month', 'cal_day', 'holiday'] + hl.name.unique().tolist()]
    time_series_data = time_series_data.merge(hl, how='left',
                           on=["cal_year", "cal_month", "cal_day"])
    for cname in hls:
        time_series_data.loc[time_series_data[cname].isna(), cname] = 0
        time_series_data[cname] = time_series_data[cname].astype(int)
    time_series_data.to_csv("./data/time_series_data_transformed.csv")
    return


def load_and_transform_forecast_data():
    forecast_data = forecast_weather_download('./data')
    forecast_data = forecast_weather_transformation(forecast_data)
    now_year = date.today().year
    hl = pd.DataFrame()
    for date_day, name in sorted(holidays.Finland(years=[now_year, now_year + 1]).items()):
        hl_instance = pd.DataFrame({'date': [date_day], 'name': [name]})
        hl = pd.concat([hl, hl_instance])
    hl.date = hl.date.apply(pd.to_datetime, format='%Y-%m-%d')
    hl.loc[:, 'cal_year'] = hl['date'].dt.year
    hl.loc[:, 'cal_month'] = hl['date'].dt.month
    hl.loc[:, 'cal_day'] = hl['date'].dt.day
    hl = hl.reset_index(drop=True)
    hl.loc[:, 'holiday'] = 1
    for name in hl.name.unique():
        hl.loc[:, name] = 0
    for i in range(hl.shape[0]):
        cname = hl.loc[i, 'name']
        hl.loc[i, [cname]] = 1
    hls = ['holiday'] + hl.name.unique().tolist()
    hl = hl.loc[:, ['cal_year', 'cal_month', 'cal_day', 'holiday'] + hl.name.unique().tolist()]
    forecast_data.loc[:, 'cal_year'] = forecast_data['date'].dt.year
    forecast_data.loc[:, 'cal_month'] = forecast_data['date'].dt.month
    forecast_data.loc[:, 'cal_day'] = forecast_data['date'].dt.day
    forecast_data.loc[:, 'cal_hour'] = forecast_data['date'].dt.hour
    forecast_data = forecast_data.merge(hl, how='left',
                                        on=["cal_year", "cal_month", "cal_day"])
    for cname in hls:
        forecast_data.loc[forecast_data[cname].isna(), cname] = 0
        forecast_data[cname] = forecast_data[cname].astype(int)
    forecast_data.loc[:, 'station'] = None
    forecast_data = forecast_data.loc[forecast_data.cal_hour % 2 == 0, :].reset_index(drop=True)
    return forecast_data


def train_models_and_save_predictions(path_to_data = './data/time_series_data_transformed.csv',
                                      path_to_stats = './data/stats.csv',
                                      path_to_predictions='./data/predictions.csv'):
    """
    :param path_to_predictions:
    :param path_to_stats:
    :param path_to_data:
    :return:
    """
    results_all = pd.DataFrame()
    df_predictions_all = pd.DataFrame()
    dt_all = pd.read_csv(path_to_data, index_col=0)
    for station in dt_all['departure'].unique():
        dt = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
        dt = dt.loc[~dt.cal_month.isin([1, 2, 3, 11, 12]), :]
        dt.number = dt.number + 0.1
        train = dt[:-84]
        test = dt[-84::]
        tscv = TimeSeriesSplit(n_splits=5, test_size=84)
        s = setup(data=train,
                  test_data=test,
                  target='number',
                  fold_strategy=tscv,
                  data_split_shuffle=False,
                  normalize=True,
                  ignore_features=['departure', 'date', 'cal_year', 'Snow Depth'],
                  categorical_features=['cal_month', 'cal_day', 'cal_hour',
                                        'holiday', 'Uudenvuodenpäivä', 'Loppiainen',
                                        'Pitkäperjantai', 'Pääsiäispäivä', '2. pääsiäispäivä',
                                        'Vappu',
                                        'Helatorstai', 'Helluntaipäivä', 'Juhannusaatto',
                                        'Juhannuspäivä',
                                        'Pyhäinpäivä', 'Itsenäisyyspäivä', 'Jouluaatto',
                                        'Joulupäivä',
                                        'Tapaninpäivä'],
                  numeric_features=['Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation', 'Sea Level Pressure',
                                    'Relative Humidity', 'Wind Speed'
                                    ],
                  silent=True,
                  verbose=False,
                  session_id=1,
                  preprocess=True, html=False)
        best = compare_models(sort='MAE', verbose=True, include=['lightgbm', 'gbr', 'br', 'ridge', 'omp'])
        final_best = finalize_model(best)
        predictions = predict_model(final_best, data=test)
        predictions.loc[predictions.Label < 0, 'Label'] = 0
        predictions.Label = predictions.Label.astype('int')
        results = pd.DataFrame({'station': [station],
                                'model MAE': [mean_absolute_error(predictions.number, predictions.Label)],
                                'baseline mean MAE': [mean_absolute_error(predictions.number,
                                                                          np.repeat(train.number.mean(),
                                                                                    len(predictions.number)))],
                                'baseline median MAE': [mean_absolute_error(predictions.number,
                                                                            np.repeat(train.number.median(),
                                                                                      len(predictions.number)))],
                                'model MSE': [mean_squared_error(predictions.number, predictions.Label)],
                                'baseline mean MSE': [mean_squared_error(predictions.number,
                                                                         np.repeat(train.number.mean(),
                                                                                   len(predictions.number)))],
                                'baseline median MSE': [mean_squared_error(predictions.number,
                                                                           np.repeat(train.number.median(),
                                                                                     len(predictions.number)))],
                                'model MAPE': [mean_absolute_percentage_error(predictions.number, predictions.Label)],
                                'baseline mean MAPE': [mean_absolute_percentage_error(predictions.number,np.repeat(train.number.mean(), len(predictions.number)))],
                                'baseline median MAPE': [mean_absolute_percentage_error(predictions.number, np.repeat(train.number.median(), len(predictions.number)))],
                                'model MASE': [mean_absolute_scaled_error(train, predictions.number, predictions.Label)],
                                'baseline mean MASE': [mean_absolute_scaled_error(train, predictions.number, np.repeat(train.number.mean(), len(predictions.number)))],
                                'baseline median MASE': [mean_absolute_scaled_error(train, predictions.number, np.repeat(train.number.median(), len(predictions.number)))],
                                'model type': [pull().iloc[0:1].Model[0]]})
        results_all = pd.concat([results, results_all]).reset_index(drop=True)
        results_all.to_csv(path_to_stats)
        all_preds = predict_model(final_best, data=dt)
        df = dt_all.loc[dt_all.departure == station, :].reset_index(drop=True)
        df = df.merge(all_preds, how = 'left', on = ['date'])
        df.loc[df.Label.isna(),'Label'] = 0
        df = df[['cal_year_x', 'cal_month_x', 'cal_day_x', "number_x", "Label"]]
        df['date'] = pd.to_datetime(
            df['cal_year_x'].astype(str) + df['cal_month_x'].astype(str) + df['cal_day_x'].astype(str),
            format='%Y%m%d')
        df = df[['date', 'number_x', 'Label']]
        df = df.groupby(['date']).agg(['mean']).reset_index()
        df.columns = ['date', 'Value', 'Prediction']
        df.loc[:, 'station'] = station
        df_predictions_all = pd.concat([df_predictions_all, df]).reset_index(drop=True)
        df_predictions_all.to_csv(path_to_predictions)
        path_to_model = './data/models/model_for_' + station.replace("/", "-")
        save_model(final_best, path_to_model)
    return
