from app import app
import dash
import dash_leaflet as dl
import pandas as pd
from dash import html, dcc, Output, Input
import plotly.express as px
import plotly.graph_objs as go
from dash_extensions.javascript import assign
from dash import dash_table

points = pd.read_csv('points.csv')
points = points[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates()
stations = []

new_stations = pd.read_csv('df_top_locations.csv')

icons_red = {
    "iconUrl": 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png',
    "shadowUrl": 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    "iconSize": [25, 41],  # size of the icon
    "shadowSize": [41, 41],  # size of the shadow
    "iconAnchor": [12, 41],  # point of the icon which will correspond to marker's location
    "shadowAnchor": [12, 41],  # the same for the shadow
    "popupAnchor": [-3, -34]  # point from which the popup should open relative to the iconAnchor
}
icons_blue = {
    "iconUrl": 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png',
    "shadowUrl": 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
    "iconSize": [25, 41],  # size of the icon
    "shadowSize": [41, 41],  # size of the shadow
    "iconAnchor": [12, 41],  # point of the icon which will correspond to marker's location
    "shadowAnchor": [12, 41],  # the same for the shadow
    "popupAnchor": [-3, -34]  # point from which the popup should open relative to the iconAnchor
}


mks = []
for i in range(points.shape[0]):
    mk = dl.Marker(dl.Tooltip(points.iloc[i].departure_name), position = [points.iloc[i].departure_latitude,
                                                                    points.iloc[i].departure_longitude],
             id = "stat{}".format(i), icon=icons_blue)
    mks.append(mk)
markers = mks
cluster = dl.MarkerClusterGroup(id="markers", children=markers, options={"polygonOptions": {"color": "red"}})

new_mks = []
for j in range(new_stations.shape[0]):

    new_mk = dl.Marker(dl.Tooltip("Station Ranking " + str(j+1)), position=[new_stations.iloc[j].Lat,
                                                                         new_stations.iloc[j].Lon],
                       id = "stat{}".format(i), icon= icons_red)
    new_mks.append(new_mk)
new_markers = new_mks
cluster_new = dl.MarkerClusterGroup(id="markers_new", children=new_markers, options={"polygonOptions": {"color": "green"},
                                                                                     "zoomToBoundsOnClick" : "True"})


layout = html.Div([
    dl.Map([
        dl.LayersControl(
            [dl.BaseLayer(dl.TileLayer(), name="World Map", checked=True)] +
            [dl.Overlay(dl.LayerGroup(new_markers), name="Proposed Stations", checked=True),
             dl.Overlay(dl.LayerGroup(markers), name="Current Stations", checked=False)]
        )],
        center=(60,20), zoom=3, id="map",
                    style={'width': '80%', 'height': '70vh', 'margin': "auto", "display": "block", "align":"left"}),
    dash_table.DataTable(id='table',
                         columns=[{"name": i, "id": i} for i in new_stations.columns],
                         data=new_stations.to_dict('records')),
    html.Div(id='app-2-display-value'),
    dcc.Link('Go to App 1', href='/apps/app1')
])


@app.callback(
    Output('app-2-display-value', 'children'),
    Input('app-2-dropdown', 'value'))
def display_value(value):
    return 'You have selected "{}"'.format(value)