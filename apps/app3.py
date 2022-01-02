import math

import branca.colormap as cm
import dash_uploader as du
import folium
from dash import html, dcc, Output, Input, State

from New_stations.Functions.Data_ETL_functions.Locations_module.Locations_set_creation import aggregate_locations_data
from New_stations.Functions.Data_ETL_functions.Stations_module.Stations_set_creation import aggregate_stations_data
from New_stations.Functions.Modelling_functions.Select_best_locations import perform_selection
from New_stations.Functions.Modelling_functions.Train_Predict_stations_model import predict_stations
from New_stations.Functions.Stats_functions.Stations_statistics import most_popular_in_periods, most_popular_connections
from app import app
from functions.Historical_weather_load_transform import *
from functions.functions import *

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
              filetypes=['csv']
              ),
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
                                                  className="clickdata7-loading"), color='orange'),
    dcc.Loading(type="default", children=html.Div(id='clickdata8',
                                                  className="clickdata8-loading"), color='orange')
])


@app.callback([Output("clickdata2", "children")],
              [Input('dash-uploader', 'isCompleted')],
              [State('dash-uploader', 'fileNames')]
)
def callback_on_completion(iscompleted, fileNames):
    if iscompleted:
        print('filtering')
        path_str = '.\data\\' + fileNames[0]
        path_str = "./data/database.csv"  # For Unix
        df = load_and_filter_data(path_str)
        print(df.shape)
        return ['uploaded']
    return ['notuploaded']


@app.callback([Output("clickdata4", "children")],
              [Input('clickdata2', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':

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


@app.callback([Output("clickdata8", "children")],
              [Input('clickdata7', 'children')])
def callback(value):
    print(value)
    if value == 'uploaded':

        owd = os.getcwd()
        os.chdir("../New_stations/")
        bikes_path = owd + "/data/database.csv"
        path = "./Data/"

        print(bikes_path)
        df_weekend, df_working = aggregate_stations_data(path=path, bikes_path=bikes_path,
                                                         update_flag=False)
        df_locations = aggregate_locations_data(path=path, update_flag=False)
        model_working = load_model("./Data/Modelling/working_tuned_random_forest_regressor")
        model_weekend = load_model("./Data/Modelling/weekend_tuned_random_forest_regressor")

        predict_stations(df_locations=df_locations, weekend_model=model_weekend, working_model=model_working,
                         min_distance_in_grid=0.2)

        df_working_predicted = pd.read_csv(
            "./Data/Results/working_locations_predicted.csv", index_col=0)
        df_weekend_predicted = pd.read_csv(
            "./Data/Results/weekend_locations_predicted.csv", index_col=0)

        perform_selection(df_weekend, df_working, df_weekend_predicted, df_working_predicted,
                          nearest_stations_distance=0.357950, n=25)
        print("updating statistics")
        most_popular_in_periods(path_to_file=bikes_path)
        most_popular_connections(path_to_file=bikes_path)

        file_to_read = open("../New_stations/Data/Statistics/most_popular_in_periods.pickle", "rb")
        list_df = pickle.load(file_to_read)
        file_to_read.close()

        radius_scaler = [200, 30, 7, 2, 1]
        color_scaler = [10, 100, 500, 1500, 2000]

        for i in range(len(list_df)):
            df = list_df[i]
            df.columns = ["station_name", "number", "lat", "lon"]
            max_number = df.number.max()
            min_number = df.number.min()

            hel_map = folium.Map([60.1975594, 24.9320720], zoom_start=12)
            folium.TileLayer('openstreetmap').add_to(hel_map)
            colormap = cm.LinearColormap(colors=['#feb24c', '#800026'], index=[color_scaler[i], max_number],
                                         vmin=color_scaler[i], vmax=max_number)

            for index, row in df.iterrows():
                folium.CircleMarker([row['lat'], row['lon']],
                                    radius=math.sqrt(row["number"] * radius_scaler[i] / (math.pi * 50)),
                                    popup=row['station_name'] + " -  " + str(round(row['number'])),
                                    color="grey",
                                    fill_color=colormap(row['number']),
                                    fill_opacity=0.9,
                                    ).add_to(hel_map)

            file_name = "../assets/Helsinki_heat_" + str(i) + ".html"

            hel_map.save(file_name)

        os.chdir(owd)

        print('updating statistics finished')

        return ['uploaded']
    return ['notuploaded']
