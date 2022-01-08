from datetime import timedelta
import holidays
from pycaret.regression import *


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
    #df[['departure', 'return']] = df[['departure', 'return']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
    df.to_csv('../Existing_stations/Data/Time_series/filtered_data.csv', index=False)
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
    station_nr = 1
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
        calendar.loc[:, 'departure_name'] = station
        calendar = calendar.rename(columns={'departure_name': 'departure'})
        calendar.loc[calendar.number.isna(), 'number'] = 0
        calendar[['number']] = calendar[['number']].astype(int)
        time_series_data = pd.concat([time_series_data, calendar])
        time_series_data = time_series_data.reset_index(drop=True)
        time_series_data.reset_index(drop=True).to_csv("../Existing_stations/Data/Time_series/time_series_data.csv")
        print(station_nr)
        station_nr += 1
    return


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
    time_series_data = time_series_data.drop(columns=['date_y'])
    time_series_data = time_series_data.rename(columns = {'date_x': 'date'})
    time_series_data.to_csv("../Existing_stations/Data/Time_series/time_series_data_transformed.csv")
    return
