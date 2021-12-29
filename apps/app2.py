from app import app
import dash
import dash_leaflet as dl
import pandas as pd
from dash import html, dcc, Output, Input
from dash import dash_table

# Initial data transformation
points = pd.read_csv('points.csv')
points = points[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates()
stations = []

df_stations = pd.read_csv('../data/updated_df_top_locations.csv')
df_stations = df_stations.reset_index()
df_stations.columns = ["Ranking", "Latitude", "Longitude", "Predicted weekend departures",
                       "Predicted working days departures", "Weighted Score"]

df_stations.loc[:, ['Predicted weekend departures',
                    "Predicted working days departures",
                    "Weighted Score"]] = df_stations.loc[:, ['Predicted weekend departures',
                                                             "Predicted working days departures",
                                                             "Weighted Score"]].round(2)

df_stations.loc[:, "Ranking"] += 1

# JS styles alignment

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

# Create list of current stations
mks = []
for i in range(points.shape[0]):
    mk = dl.Marker(dl.Tooltip(points.iloc[i].departure_name), position=[points.iloc[i].departure_latitude,
                                                                        points.iloc[i].departure_longitude],
                   id="stat{}".format(i), icon=icons_blue)
    mks.append(mk)
markers = mks

# Create list of proposed stations
new_mks = []
for j in range(df_stations.shape[0]):
    new_mk = dl.Marker(children=dl.Tooltip("Station Ranking " + str(j + 1)), position=[df_stations.iloc[j].Latitude,
                                                                                       df_stations.iloc[j].Longitude],
                       id="new_stat{}".format(j + 1), icon=icons_red)
    new_mks.append(new_mk)
new_markers = new_mks

# Create layout
layout = html.Div([
    # App selection
    # html.Div([
    #     dcc.RadioItems(
    #         id="app_select",
    #         options=[
    #             {'label': 'Existing Stations', 'value': 'app1'},
    #             {'label': 'New Stations', 'value': "app2"}
    #         ],
    #         value='app2',
    #         labelStyle={'display': 'flex'}
    #     )
    # ], id='app_selection', style={}),
    # Map of Helsinki
    dl.Map([
        html.Br(),
        dl.LayersControl(
            [dl.BaseLayer(dl.TileLayer(), name="World Map", checked=True),
            dl.Overlay(dl.LayerGroup(new_markers), name="Proposed Stations", checked=True),
             dl.Overlay(dl.LayerGroup(markers), name="Current Stations", checked=False)]
        )],
        center=(60.17, 24.95), zoom=12, id="map",
        style={'width': '70%', 'height': '60%', 'z-index': '1',
               'margin-top': '0', 'position': 'absolute', 'top': "0%", 'left': '30%'}),

    html.Br(),

    # Table with best locations
    html.Div([dash_table.DataTable(id='table',
                         columns=[{"name": i, "id": i} for i in df_stations.columns],
                         data=df_stations.to_dict('records'),
                         style_table={"minWidth": "100%",
                                      "height": "40vh",
                                      "overflowY": 'scroll'},
                         style_as_list_view=False,
                         style_header={
                             'backgroundColor': "lightgreen",
                             'fontWeight': 'bold',
                             'fontSize': '12pt',
                             'color': 'white'
                         },
                         style_data={
                             'backgroundColor': 'white',
                             'color': 'black',
                             'fontSize': '10pt',
                         },
                         style_cell_conditional=[
                             {
                                 'if': {"column_id": c},
                                 'fontWeight': 'bold'
                             } for c in ["Weighted Score", "Ranking"]
                         ]),
    # hidden div to address callback
    html.Div(id="hidden_div")],
             style={'width': '70%', 'height': '40%', 'z-index': '1',
               'margin': '0', 'position': 'absolute', 'top': "60%", 'left': '30%'})])

# highlight row related to marker
@app.callback(
    Output("table", "style_data_conditional"),
    [Input(marker.id, "n_clicks") for marker in new_markers])
def marker_click(*args):
    marker_id = dash.callback_context.triggered[0]["prop_id"].split(".")[0]
    if marker_id != '':
        idx = int(marker_id[8:])
    else:
        return dash.no_update
    val = [
        {"if": {"filter_query": "{{Ranking}} ={}".format(idx)}, "backgroundColor": "#fc9272", }]
    return val

# # change app
# @app.callback(
#     Output('hidden_div', 'children'),
#     Input('app_select', 'value')
# )
# def app_select(value):
#     if value == 'app1':
#         return dcc.Location(pathname='/apps/app1', id='app1')
#     else:
#         return dcc.Location(pathname='/apps/app2', id='app1')
