import math
import pickle

import branca.colormap as cm
import folium
import plotly.express as px
from dash import dash_table
from dash import html, dcc, Output, Input

# Initial data transformation
# MAP data
from app import app

colorscale = ['#4575b4', '#abd9e9', '#fee090', '#fdae61', '#f46d43', '#d73027']  # rainbow
color_prop = 'number'

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
                            radius=math.sqrt(row["number"]*radius_scaler[i] / (math.pi * 50)),
                            popup=row['station_name'] + " -  " + str(round(row['number'])),
                            color="grey",
                            fill_color=colormap(row['number']),
                            fill_opacity=0.9,
                            ).add_to(hel_map)

    file_name = "../assets/Helsinki_heat_" + str(i) + ".html"

    hel_map.save(file_name)

# Barplot data
options_list = ["last week", "last month", "last 3 months", "last year", "full time period"]

# Table data
table_file_to_read = open("../New_stations/Data/Statistics/most_popular_connections.pickle", "rb")
list_table_df = pickle.load(table_file_to_read)
table_file_to_read.close()
df_table = list_table_df[4].iloc[:,1:]
df_table.loc[:, "departure_name"] = df_table.loc[:, "departure_name"].str.split(",").str[0]
df_table.loc[:, "return_name"] = df_table.loc[:, "return_name"].str.split(",").str[0]
df_table.columns = ["Departure station", "Return Station", "Rides"]
df_table = df_table.head(25)

layout = html.Div([html.Iframe(id='map', srcDoc=open("../assets/Helsinki_heat_4.html", "r").read(),
                               style={'width': '69.8%', 'height': '60%', 'z-index': '1',
                                      'margin-top': '0', 'position': 'absolute', 'top': "0%", 'left': '30%'}),
                   dcc.Dropdown(
                       id="dropdown_period",
                       options=[{"label": x, "value": options_list.index(x)} for x in options_list],
                       value=0,
                       style={'width': '25vw', 'position': 'absolute', 'top': "50%", 'left': '1vw',
                              'background': 'lightgreen'}),
                   dcc.Graph(id="barplot",
                             style={'width': '70%', 'height': '40%', 'z-index': '1',
                                    'margin-top': '0', 'position': 'absolute', 'top': "60%", 'left': '30.05%'}),
                   html.Div([dash_table.DataTable(id='table_connections',
                                                  columns=[{"name": i, "id": i} for i in df_table.columns],
                                                  data=df_table.to_dict('records'),
                                                  style_table={'width': '35%', 'height': '30vh', 'z-index': '1',
                                                               'margin_top': '0', 'position': 'absolute', 'top': "65vh",
                                                               'left': '65%', "overflowY": 'scroll'},
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
                                                      } for c in ["Rides"]
                                                  ])])
                   ])


@app.callback(
    [Output("table_connections", "data")],
    [Input("dropdown_period", "value")])
def update_table(period_index):
    table_file_to_read_updated = open("../New_stations/Data/Statistics/most_popular_connections.pickle", "rb")
    list_table_df_updated = pickle.load(table_file_to_read_updated)
    table_file_to_read_updated.close()
    df_table_updated = list_table_df_updated[period_index].iloc[:, 1:]
    df_table_updated.loc[:, "departure_name"] = df_table_updated.loc[:, "departure_name"].str.split(",").str[0]
    df_table_updated.loc[:, "return_name"] = df_table_updated.loc[:, "return_name"].str.split(",").str[0]
    df_table_updated.columns = ["Departure station", "Return Station", "Rides"]
    df_table_updated = df_table_updated.head(25)

    return [df_table_updated.to_dict("records")]

@app.callback(
    [Output("barplot", "figure")],
    [Input("dropdown_period", "value")])
def update_barplot(period_index):
    df_barplot = list_df[period_index]
    df_barplot = df_barplot.head(8).sort_values(by=["number"], ascending=True)
    df_barplot.columns = ["Station", "Departures", "Lat", "Lon"]
    df_barplot.loc[:, "Station"] = df_barplot.loc[:, "Station"].str.split(",").str[0]
    title = "Most popular stations / bike rides in " + options_list[period_index]
    fig = px.bar(df_barplot, y="Station", x="Departures", title=title, orientation='h',
                 color="Departures", color_continuous_scale='Emrld')
    fig.update_xaxes(visible=True, showticklabels=True)
    fig.update(layout_coloraxis_showscale=False)
    fig.update_layout(
        margin=dict(l=5, r=570, t=40, b=5),
    )
    return [fig]

@app.callback(
    [Output("map", "srcDoc")],
    [Input("dropdown_period", "value")])
def update_barplot(period_index):
    file_name = "../assets/Helsinki_heat_" + str(period_index) + ".html"
    srcDoc = open(file_name, "r").read()
    return [srcDoc]
