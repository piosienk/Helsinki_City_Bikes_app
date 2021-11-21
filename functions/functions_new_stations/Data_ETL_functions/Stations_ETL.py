import pandas as pd
import numpy as np
from datetime import date, timedelta
import networkx as nx

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
    coord = df[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates().reset_index(drop=True)
    df = df[['departure', 'departure_name']]
    df.loc[:,'dep_year'] = df['departure'].dt.year
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
        calendar = pd.DataFrame({'date': pd.date_range(sdate,edate + timedelta(days=1),freq='2H')})
        calendar[['date']] = calendar[['date']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
        calendar.loc[:,'cal_year'] = calendar['date'].dt.year
        calendar.loc[:,'cal_month'] = calendar['date'].dt.month
        calendar.loc[:,'cal_day'] = calendar['date'].dt.day
        calendar.loc[:,'cal_hour'] = calendar['date'].dt.hour
        calendar = calendar.loc[calendar.cal_year > 2017,:]
        calendar = calendar.merge(station_data, how = 'left', left_on = ['cal_year', 'cal_month',
                                                                         'cal_day', 'cal_hour'],
                                  right_on = ['dep_year', 'dep_month', 'dep_day', 'dep_hour'])
        calendar = calendar[['date', 'cal_year', 'cal_month', 'cal_day', 'cal_hour', 'departure_name', 'number']]
        calendar.loc[:,'departure_name'] = station
        calendar = calendar.rename(columns = {'departure_name': 'departure'})
        calendar.loc[calendar.number.isna(),'number'] = 0
        calendar[['number']] = calendar[['number']].astype(int)
        time_series_data = pd.concat([time_series_data, calendar])
    return time_series_data.reset_index(drop = True), coord

def get_graph_metrics_for_network(df):
    """
    df - pandas dataframe with columns: departure_name, return_name, departure_latitude, departure_longitude
    function calculates values of multiple graph metrics
    return: pandas dataframe with station, lat, lon & value of graph metrics
    """
    df_grouped = df.groupby(["departure_name", "return_name"]).count()['departure'].reset_index()
    df_grouped = df_grouped.loc[df_grouped.departure >= 50, :]
    G = nx.from_pandas_edgelist(df_grouped, source = 'departure_name', target = 'return_name', create_using = nx.DiGraph())
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