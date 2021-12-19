import dash_uploader as du
import pandas as pd
import numpy as np
from dash import html, dcc, Output, Input, State
from app import app
from functions.functions import *
from functions.Historical_weather_load_transform import *

forecast_data = pd.read_csv('../data/forecast_time_series.csv')

layout = html.Div([
    html.Div(id='forecast_value', style={'whiteSpace': 'pre-line', 'width': '70%', 'height': '70vh',
                                         'margin': "auto", "display": "block", "align": "left",
                           'z-index': '1', 'margin-top': '150px'},
             children=f'You have downloaded weather forecast since {np.min(forecast_data.date)} '
                   f'up to {np.max(forecast_data.date)}.'),
    du.Upload(id='dash-uploader',
              text_completed='New bike data added: ',
              max_file_size=10000,
              chunk_size=2048,
              filetypes=['csv']),
    dcc.Loading(type="default", children=html.Div(id='clickdata2',
                                                  className="clickdata2-loading"),
                color='orange'),
    html.Div(id='clickdata3'),
dcc.Loading(type="default", children=html.Div(id='clickdata4',
                                                  className="clickdata4-loading"), color='orange'),
dcc.Loading(type="default", children=html.Div(id='clickdata5',
                                                  className="clickdata5-loading"), color='orange'),
dcc.Loading(type="default", children=html.Div(id='clickdata6',
                                                  className="clickdata6-loading"), color='orange'),
dcc.Loading(type="default", children=html.Div(id='clickdata7',
                                                  className="clickdata7-loading"), color='orange')
])


@app.callback([Output("clickdata2", "children")],
              [Input('dash-uploader', 'isCompleted')],
              [State('dash-uploader', 'fileNames')]
)
def callback_on_completion(iscompleted, fileNames):
    if iscompleted:
        print('filtering')
        path_str = '.\data\\' + fileNames[0]
        df = load_and_filter_data(path_str)
        print(df.shape)
        return ['uploaded']
    return ['notuploaded']


@app.callback([Output("clickdata4", "children")],
              [Input('clickdata2', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':
        print('tak')
        df = pd.read_csv('./data/filtered_data.csv')
        df[['departure', 'return']] = df[['departure', 'return']].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S.%f')
        ts_data = prepare_time_series_data(df)
        #print(ts_data.shape)
        return ['uploaded']
    return ['notuploaded']


@app.callback([Output("clickdata5", "children")],
              [Input('clickdata4', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':
        ts_data = pd.read_csv('./data/time_series_data.csv')
        hist_metadata_dict, hist_weather_dict = historical_weather_load(end_date=np.max(ts_data.date))
        print(3)
        mod_weather_dict = historical_weather_transformations(hist_weather_dict)
        print(4)
        create_weather_stats(mod_weather_dict, hist_metadata_dict)
        return ['uploaded']
    return ['notuploaded']


@app.callback([Output("clickdata6", "children")],
              [Input('clickdata5', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':
        transform_time_series('./data/time_series_data.csv', './data/weather_time_series.csv')
        return ['uploaded']
    return ['notuploaded']


@app.callback([Output("clickdata7", "children")],
              [Input('clickdata6', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':
        train_models_and_save_predictions()
        return ['uploaded']
    return ['notuploaded']


