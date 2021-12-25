from app import app
import dash
import dash_leaflet as dl
from os.path import exists
import pandas as pd
from dash import html, dcc, Output, Input
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime, timedelta
import numpy as np
from pycaret.regression import *
from functions.functions import forecast_weather_transformation, forecast_weather_download
import dash_bootstrap_components as dbc

points = pd.read_csv('points.csv')
points = points[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates()
stations = []
predictions = pd.read_csv('./data/predictions.csv', index_col=0)

if exists("./data/forecast_weather_dict.pickle"):
    df_forecast = forecast_weather_transformation(df=pd.read_pickle("./data/forecast_weather_dict.pickle"))
    f_min = np.min(df_forecast.date)
    f_max = np.max(df_forecast.date)
else:
    df_forecast = None
    f_min = None
    f_max = None

mks = []
for i in range(points.shape[0]):
    mk = dl.Marker(dl.Tooltip(points.iloc[i].departure_name),
                   position=[points.iloc[i].departure_latitude,
                             points.iloc[i].departure_longitude],
                   id="stat{}".format(i))
    mks.append(mk)
markers = mks
cluster = dl.MarkerClusterGroup(id="markers", children=markers, options={"polygonOptions": {"color": "red"}})


layout = html.Div(id='all',
                  children=[
                      dl.Map(children=[dl.TileLayer(), cluster], center=(60.19, 24.95), zoom=11, id="map",
                    style={'width': '70%', 'height': '60%',  'z-index': '1',
                           'margin-top': '0', 'position': 'absolute', 'top': "0%", 'left': '30%'}),
    #dcc.Dropdown(id='time_series_stations', value = None,
    #             options = [{'label': v, 'value': k} for k, v in points[['departure_name']].squeeze().to_dict().items()],
    #             clearable=False, multi=False),
    html.Div(children=[
dbc.Card(
        dbc.CardBody([
dbc.Row([
                dbc.Col([html.H4(id='clickdata',
                                 style={'width': '20vw', 'z-index': '1',
                                        'margin-top': '2.5vh', 'position': 'absolute', 'top': "60vh", 'left': '2.25vw'}
                                 ),
                         ], width=1),
                dbc.Col([
html.H4('You have forecast data since {}, to {}'.format(f_min, f_max),
        id='forecast_info' ,
        style={'width': '20vw', 'z-index': '1',
                           'margin-top': '2.5vh', 'position': 'absolute', 'top': "70vh", 'left': '2.25vw'})
                ], width=2)], align='center', style={'width': '80%', 'align-items': 'center', 'marigin-left': '15%'})]))]

        # html.Div(children=[html.H4(id='clickdata')],
        #          style={'display': 'inline-block', 'vertical-align': 'top', 'margin-left': '15%',
        #                 'left': '0', 'horizontal-align':'left'}),
        # html.Div(children=html.H4('You have forecast data since {}, to {}'.format(np.min(df_forecast.date),
        #                                                                           np.max(df_forecast.date))),
        #          style={'display': 'inline-block', 'vertical-align': 'top', 'margin-right': '15%', 'right': '0',
        #                 'horizontal-align': 'right'}
        #          )], className='row', style={'height': '150px', 'width': '100%', 'display': 'block', 'align-items': 'center', 'justify-content': 'center'}
             #html.Button('Upload current forecast', id='upload', n_clicks=0)], className="row"
             ),
html.Button('Upload current forecast', id='upload', n_clicks=0,
            style={'width': '10vw', 'z-index': '1', 'font-size':"smaller",
                           'margin-top': '2.5vh', 'position': 'absolute', 'top': "80vh", 'left': '2.25vw'}),
    html.Div(children=[
        html.Div(children=[
            dcc.Graph(id="graph-with-slider")

            ],style={'display': 'inline-block', 'vertical-align': 'top', 'width': '35%', 'z-index': '1',
                           'margin': '0', 'position': 'absolute', 'top': "60%", 'left': '30%'}
        ),
        html.Div(children=[
            dcc.Graph(id="graph2")],
            style={'display': 'inline-block', 'vertical-align': 'top', 'width': '35%',  'z-index': '1',
                           'margin': '0', 'position': 'absolute', 'top': "60%", 'left': '65%'}
        )
    ], className="row", style={'height': '150px', 'width': '100%', 'display': 'block', 'align-items': 'center', 'justify-content': 'center'}),
    html.Div(id='app-1-display-value')
])
# Link drop down to geojson hideout prop (could be done with a normal callback, but clientside is more performant).
app.clientside_callback("function(x){return x;}", Output("geojson", "hideout"), Input("dd", "value"))


@app.callback([Output("clickdata", "children"),
               Output('graph-with-slider', 'figure'),
               Output('graph2', 'figure')],
              [Input(marker.id, "n_clicks") for marker in markers])
def marker_click(*args):
    marker_id = dash.callback_context.triggered[0]["prop_id"].split(".")[0]
    if marker_id !='':
        idx = int(marker_id[4:])
        df_plot = predictions.loc[predictions.station == points.iloc[idx].departure_name, :]
        # fig = go.Figure()
        # fig.add_trace(go.Scatter(x=df_plot.date, y=df_plot.Value, name='Actual Value', showlegend=True,
        #                          line=dict(color='green', width=1)))
        # fig.add_trace(go.Scatter(x=df_plot.date, y=df_plot.Prediction, name='Predicted Value', showlegend=True,
        #                          line=dict(color='orange', width=1)))
        # fig.update_layout(
        #     legend=dict(
        #         x=1,
        #         y=1,
        #         traceorder="reversed",
        #         title_font_family="Times New Roman",
        #         font=dict(
        #             family="Courier",
        #             size=12,
        #             color="black"
        #         ),
        #         bgcolor="LightSteelBlue",
        #         bordercolor="Black",
        #         borderwidth=2
        #     )
        # )
        # fig.update_layout(paper_bgcolor="darkblue")
        fig = px.line(df_plot, x='date', y=["Value", "Prediction"], template='plotly_dark')
        fig.update_layout(transition_duration=500)
        #### TODO - downloading forecast
        if exists("./data/forecast_weather_dict.pickle"):
            df_forecast = forecast_weather_transformation(df=pd.read_pickle("./data/forecast_weather_dict.pickle"))
            f_min = np.min(df_forecast.date)
            f_max = np.max(df_forecast.date)
        else:
            df_forecast = None
            f_min = None
            f_max = None
        path_to_model = '../data/models/model_for_' + points.iloc[idx].departure_name.replace('/', '-')
        print(path_to_model)
        mdl = load_model(path_to_model)
        preds = predict_model(mdl, data = df_forecast)
        preds.loc[preds.cal_month.isin([1, 2, 3, 11, 12]), 'Label'] = 0
        fig2 = px.line(preds, x='date', y=["Label"], template='plotly_dark')
        fig2.update_layout(transition_duration=500)
        vl = points.iloc[idx].departure_name
        vl = "You have selected {} Bike Station.".format(vl), html.Br(), \
             "Historical data for this station is gathered since {}.".format(np.min(df_plot.date))
    else:
        fig = go.Figure()
        fig2 = go.Figure()
        vl = 'You have not selected any Bike Station.'
    return vl, fig, fig2


@app.callback(
    Output('forecast_info', 'children'),
    Input('upload', 'n_clicks')
)
def update_output(n_clicks):
    forecast_weather_transformation(forecast_weather_download())
    df_forecast = forecast_weather_transformation(df=pd.read_pickle("./data/forecast_weather_dict.pickle"))
    return 'You have forecast data since {}, to {}'.format(np.min(df_forecast.date), np.max(df_forecast.date))


