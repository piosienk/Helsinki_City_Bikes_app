import requests
import pandas as pd
import datetime as dt
import os
import pickle


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
    df_for.loc[:, 'cal_year'] = df_for['date'].dt.year
    df_for.loc[:, 'cal_month'] = df_for['date'].dt.month
    df_for.loc[:, 'cal_day'] = df_for['date'].dt.day
    df_for.loc[:, 'cal_hour'] = df_for['date'].dt.hour

    # Take only weekly forecast
    df_for = df_for.iloc[:169, :]
    df_for.reset_index(inplace=True, drop=True)

    # Keep only necessary columns
    df_for.drop(columns=["Date time"], inplace=True)
    columns_list = ['date', 'Temperature', 'Cloud Cover', 'Wind Gust', 'Precipitation',
                    'Sea Level Pressure', 'Relative Humidity', 'Snow Depth', 'Wind Speed',
                    'cal_year', 'cal_month', 'cal_day', 'cal_hour']

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

    return df_for_rolled
