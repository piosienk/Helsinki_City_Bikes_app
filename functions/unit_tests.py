import datetime
from datetime import timedelta
import pandas as pd
import numpy as np


def test_hist_data(metadata_dict, weather_dict):
    date = '2019-12-25 14:00:00'
    results = pd.DataFrame()
    names = [x for x in metadata_dict]
    date_1 = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    date_2 = date_1 + timedelta(hours=1)
    feature_names = ['Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation',
                       'Sea Level Pressure', 'Relative Humidity', 'Snow Depth', 'Wind Speed']
    for feature in feature_names:
        names_temp = [float(weather_dict[x].loc[weather_dict[x].date == date_1, feature]) for x in metadata_dict] + \
                 [float(weather_dict[x].loc[weather_dict[x].date == date_2, feature]) for x in metadata_dict]
        names_temp = np.array(names_temp)
        names_temp = names_temp[~np.isnan(names_temp)]
        results[feature] = [names_temp.mean()]
    results[['Uudenvuodenpäivä', 'Loppiainen', 'Pitkäperjantai',
       'Pääsiäispäivä', '2. pääsiäispäivä', 'Vappu', 'Helatorstai',
       'Helluntaipäivä', 'Juhannusaatto', 'Juhannuspäivä', 'Pyhäinpäivä',
       'Itsenäisyyspäivä', 'Jouluaatto', 'Tapaninpäivä']] = 0
    results[['holiday', 'Joulupäivä']] = 1
    ts = pd.read_csv('./data/time_series_data_transformed.csv', index_col = 0)
    ts1 =ts.loc[ts.date == str(date_1), :].reset_index(drop=True).head(1)
    combined_result = pd.DataFrame()
    combined_result.index = results.columns
    combined_result['by hand'] = results.loc[0, :].astype(float)
    combined_result['by algorithm'] = ts1.loc[0, feature_names + ['Uudenvuodenpäivä', 'Loppiainen', 'Pitkäperjantai',
                                                                  'Pääsiäispäivä', '2. pääsiäispäivä', 'Vappu', 'Helatorstai',
       'Helluntaipäivä', 'Juhannusaatto', 'Juhannuspäivä', 'Pyhäinpäivä',
       'Itsenäisyyspäivä', 'Jouluaatto', 'Tapaninpäivä', 'holiday', 'Joulupäivä']].to_numpy()
    combined_result['diff'] = combined_result['by hand'] - combined_result['by algorithm']
    combined_result.to_csv('./data/unit_test.csv')
    return

def forcast_data():
    dt = datetime.datetime.now()

    return
