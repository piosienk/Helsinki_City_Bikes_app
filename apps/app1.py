from app import app
import dash
import dash_leaflet as dl
import pandas as pd
from dash import html, dcc, Output, Input
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime, timedelta
import numpy as np

points = pd.read_csv('points.csv')
points = points[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates()
stations = []
predictions = pd.read_csv('../data/predictions.csv', index_col=0)

mks = []
for i in range(points.shape[0]):
    mk = dl.Marker(dl.Tooltip(points.iloc[i].departure_name), position = [points.iloc[i].departure_latitude,
                                                                    points.iloc[i].departure_longitude],
             id = "stat{}".format(i))
    mks.append(mk)
markers = mks
cluster = dl.MarkerClusterGroup(id="markers", children=markers, options={"polygonOptions": {"color": "red"}})

layout = html.Div([
    html.H1('An application for management of Helsinki City Bikes'),
    dl.Map([dl.TileLayer(), cluster], center=(60.19, 24.95), zoom=11, id="map",
                    style={'width': '80%', 'height': '70vh', 'margin': "auto", "display": "block", "align":"left"}),
    #dcc.Dropdown(id='time_series_stations', value = None,
    #             options = [{'label': v, 'value': k} for k, v in points[['departure_name']].squeeze().to_dict().items()],
    #             clearable=False, multi=False),
    html.Div(id='clickdata'),
    html.Div([
        html.Div(
            dcc.Graph(id="graph-with-slider", style={'display': 'inline-block'}),
            className="two columns",
            style={"width": '70%', "margin": 'auto', 'display': 'inline-block'}
        ),
        html.Div(
            dcc.Graph(id="graph2", style={'display': 'inline-block'}),
            className="two columns",
            style={"width": '10%', "margin": 'auto', 'display': 'inline-block'}
        ),
    ], className="row"),
    html.Div(id='app-1-display-value'),
    dcc.Link('Go to App 2', href='/apps/app2'),
    dcc.Link('Go to App 3', href='/apps/app3')
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
        #file_name = '../data/time_series_data/' + points.iloc[idx].departure_name.replace('/', '-') + '.csv'
        df_plot = predictions.loc[predictions.station == points.iloc[idx].departure_name, :]
        #df_plot = pd.read_csv(file_name, index_col=0)
        fig = px.line(df_plot, x='date', y=["Value", "Prediction"], template='plotly_dark')
        fig.update_layout(transition_duration=500)
        #### TODO
        now = datetime.today().strftime('%Y-%m-%d')
        df_forecast = pd.DataFrame()
        df_forecast['date'] = np.repeat(now, 7)
        df_forecast['date'] = pd.to_datetime(df_forecast['date'])
        df_forecast.loc[:, 'timedelta'] = [timedelta(days=dy) for dy in range(7)]
        df_forecast.date = df_forecast.date + df_forecast.timedelta
        df_forecast.loc[:, 'Prediction'] = 0
        df_forecast = df_forecast[['date', 'Prediction']]
        fig2 = px.line(df_forecast, x='date', y=["Prediction"], template='plotly_dark')
        fig2.update_layout(transition_duration=500)
        ####
    else:
        fig = go.Figure()
        fig2 = go.Figure()

    return "You have selected {}".format(points.iloc[idx].departure_name), fig, fig2


