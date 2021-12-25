import math
import pickle

import dash_leaflet as dl
import dash_leaflet.express as dlx
from dash import dash_table
import pandas as pd
import plotly.express as px
from dash_extensions.javascript import assign
from dash import Dash, html, dcc, html, dcc, Output, Input
import plotly.graph_objects as go
import folium
import branca.colormap as cm
from folium import plugins

# Initial data transformation
# MAP data
from app import app

colorscale = ['#4575b4', '#abd9e9', '#fee090', '#fdae61', '#f46d43', '#d73027']  # rainbow
color_prop = 'number'

file_to_read = open("data/time_periods/most_popular_in_periods.pickle", "rb")
list_df= pickle.load(file_to_read)
df = list_df[4]
df.columns = ["station_name", "number", "lat", "lon"]
max_number = df.number.max()
min_number = df.number.min()



hel_map = folium.Map([60.1975594, 24.9320720], zoom_start=12)
folium.TileLayer('openstreetmap').add_to(hel_map)
colormap = cm.LinearColormap(colors=['#feb24c','#800026'], index=[min_number+1000, max_number],
                             vmin=min_number+1000,vmax=max_number)

for index, row in df.iterrows():
    folium.CircleMarker([row['lat'], row['lon']],
                        radius=math.sqrt(row["number"]/(math.pi*50)),
                        popup=row['station_name'] + " -  " + str(round(row['number'])),
                        color="grey",
                        fill_color=colormap(row['number']),
                        fill_opacity=0.9,
                        ).add_to(hel_map)

hel_map.save("Helsinki_heat.html")

# Barplot data
options_list = ["last week", "last month", "last 3 months", "last year", "full time period"]
file_to_read = open("data/time_periods/most_popular_in_periods.pickle", "rb")
list_df = pickle.load(file_to_read)



layout = html.Div([html.Iframe(id='map', srcDoc=open("Helsinki_heat.html", "r").read(),
                               style={'width': '70%', 'height': '60%',  'z-index': '1',
                           'margin-top': '0', 'position': 'absolute', 'top': "0%", 'left': '30%'}),
                   dcc.Dropdown(
                       id="dropdown_period",
                       options=[{"label": x, "value": options_list.index(x)} for x in options_list],
                       value=0,
                       style={'width': '25vw', 'position': 'absolute', 'top': "50%", 'left': '1vw',
                              'background': 'lightgreen'}),
                   dcc.Graph(id="barplot",
                             style={'width': '35%', 'height': '40%',  'z-index': '1',
                           'margin-top': '0', 'position': 'absolute', 'top': "60%", 'left': '30%'}),
                   ])



# # change app
# @app.callback(
#     Output('hidden_div', 'children'),
#     Input('app_select', 'value')
# )
# def app_select(value):
#     if value == 'app1':
#         return dcc.Location(pathname='/apps/app1', id='app1')
#     else:
#         return dcc.Location(pathname='/apps/app2_copy', id='app1')

@app.callback(
    [Output("barplot", "figure")],
    [Input("dropdown_period", "value")])
def update_barplot(period_index):
    df_barplot = list_df[period_index]
    df_barplot = df_barplot.head(8).sort_values(by=["number"], ascending=True)
    df_barplot.columns = ["Station", "Departures", "Lat", "Lon"]
    df_barplot.loc[:, "Station"] = df_barplot.loc[:, "Station"].str.split(",").str[0]
    title = "Most popular stations in " + options_list[period_index]
    fig = px.bar(df_barplot, y="Station", x="Departures", title=title, orientation='h',
                 color="Departures", color_continuous_scale='Emrld')
    fig.update_xaxes(visible=True, showticklabels=True)
    fig.update(layout_coloraxis_showscale=False)
    fig.update_layout(
        margin=dict(l=5, r=5, t=40, b=5),
    )


    return [fig]
