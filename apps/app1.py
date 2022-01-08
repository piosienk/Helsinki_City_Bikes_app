from app import app
import dash
import dash_leaflet as dl
from os.path import exists
from dash import html, dcc, Output, Input
import plotly.express as px
import plotly.graph_objs as go
from pycaret.regression import *
from Existing_stations.Functions.Forecast_weather_functions import forecast_weather_transformation,\
    forecast_weather_download
import dash_bootstrap_components as dbc

points = pd.read_csv('../Existing_stations/Data/Results/points.csv')
points = points[['departure_name', 'departure_latitude', 'departure_longitude']].drop_duplicates()
stations = []
predictions = pd.read_csv('../Existing_stations/Data/Results/predictions.csv', index_col=0)

if exists("../Existing_stations/Data/Weather/forecast_weather_dict.pickle"):
    df_forecast = forecast_weather_transformation(df=pd.read_pickle("../Existing_stations/Data/Weather/forecast_weather_dict.pickle"))
    f_min = np.min(df_forecast.date)
    f_max = np.max(df_forecast.date)
    value_forecast = 'You have forecast data since {}, to {}'.format(f_min, f_max),
else:
    df_forecast = None
    value_forecast = 'You do not have any forecast data.'

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
                             style={'width': '70%', 'height': '60%', 'z-index': '1',
                                    'margin-top': '0', 'position': 'absolute', 'top': "0%", 'left': '30%'}),
                      html.Div(children=[
                          dbc.Card(
                              dbc.CardBody([
                                  dbc.Row([
                                      dbc.Col([html.H4(id='clickdata',
                                                       style={'width': '20vw', 'z-index': '1',
                                                              'margin-top': '2.5vh', 'position': 'absolute',
                                                              'top': "60vh", 'left': '2.25vw',
                                                              'background-color': 'lightgreen',
                                                              'border': '2px solid black'}),
                                               ], width=1),
                                      dbc.Col([
                                          html.H4(value_forecast,
                                                  id='forecast_info',
                                                  style={'width': '20vw', 'z-index': '1', 'margin-top': '2.5vh',
                                                         'position': 'absolute', 'top': "70vh",
                                                         'left': '2.25vw', 'background-color': 'lightgreen',
                                                         'border': '2px solid black'})
                                      ], width=2)], align='center', style={'width': '80%', 'align-items': 'center',
                                                                           'marigin-left': '15%'})]))
                      ]),
                      html.Button('Upload current forecast', id='forecast_upload', n_clicks=0,
                                  style={'width': '10vw', 'z-index': '1', 'font-size': "smaller",
                                         'margin-top': '2.5vh', 'position': 'absolute', 'top': "80vh", 'left': '2.25vw',
                                         'background-color': 'lightgreen', 'border': '2px solid black',
                                         'border-radius': '5px'}),
                      html.Div(children=[
                          html.Div(children=[
                              dcc.Graph(id="graph-with-slider")

                          ], style={'display': 'inline-block', 'vertical-align': 'top', 'width': '35%', 'z-index': '1',
                                    'margin': '0', 'position': 'absolute', 'top': "60%", 'left': '30%'}
                          ),
                          html.Div(children=[
                              dcc.Graph(id="graph2")],
                              style={'display': 'inline-block', 'vertical-align': 'top', 'width': '35%', 'z-index': '1',
                                     'margin': '0', 'position': 'absolute', 'top': "60%", 'left': '65%'}
                          )
                      ], className="row",
                          style={'height': '150px', 'width': '100%', 'display': 'block', 'align-items': 'center',
                                 'justify-content': 'center'}),
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
        fig = px.line(df_plot, x='date', y=["Value", "Prediction"], template='plotly_dark', color_discrete_sequence=
        ["blue", "green"])
        fig.update_layout(transition_duration=500,
                          title="Average number per 2-hours slots",
                          yaxis_title="Number of bikes rented")
        if exists("../Existing_stations/Data/Weather/forecast_weather_dict.pickle"):
            df_forecast = forecast_weather_transformation(df=pd.read_pickle("../Existing_stations/Data/Weather/forecast_weather_dict.pickle"))
            f_min = np.min(df_forecast.date)
            f_max = np.max(df_forecast.date)
        else:
            df_forecast = None
            f_min = None
            f_max = None
        path_to_model = '../Existing_stations/Data/Models/model_for_' + points.iloc[idx].departure_name.replace('/', '-')
        print(path_to_model)
        mdl = load_model(path_to_model)
        preds = predict_model(mdl, data = df_forecast)
        preds.loc[preds.cal_month.isin([1, 2, 3, 11, 12]), 'Label'] = 0
        preds = preds[["date", "Label"]]
        preds.columns = ["date", "Prediction"]
        fig2 = px.line(preds, x='date', y=["Prediction"], template='plotly_dark', color_discrete_sequence=["green"])
        fig2.update_layout(transition_duration=500,
                           title="Predicted number per 2-hour slots",
                           yaxis_title="Number of bikes rented")
        vl = points.iloc[idx].departure_name
        vl = "You have selected {} Bike Station.".format(vl), html.Br(), \
             "Historical data for this station is gathered since {}.".format(np.min(df_plot.date))
    else:
        fig = go.Figure()
        fig.update_layout(title_text='Place for <br> historical <br> time series', title_x=0.5, title_y=0.5,
                          font=dict(
                              family="Arial",
                              size=25,
                              color="White"
                          ))
        fig.update_layout(plot_bgcolor="black")
        fig.update_layout(paper_bgcolor="black")
        fig.update_xaxes(showline=True, linewidth=2, linecolor='darkblue', mirror=True)
        fig.update_yaxes(showline=True, linewidth=2, linecolor='darkblue', mirror=True)
        fig.update_yaxes(visible=False, showticklabels=False)
        fig.update_xaxes(visible=False, showticklabels=False)
        fig2 = go.Figure()
        fig2.update_layout(title_text='Place for <br> forecasted <br> time series', title_x=0.5, title_y=0.5,
                           font=dict(
                               family="Arial",
                               size=25,
                               color="White"
                           ))
        fig2.update_layout(plot_bgcolor="black")
        fig2.update_layout(paper_bgcolor="black")
        fig2.update_xaxes(showgrid=False, zeroline=False)
        fig2.update_xaxes(showline=True, linewidth=2, linecolor='darkblue', mirror=True)
        fig2.update_yaxes(showline=True, linewidth=2, linecolor='darkblue', mirror=True)
        fig2.update_yaxes(visible=False, showticklabels=False)
        fig2.update_xaxes(visible=False, showticklabels=False)
        vl = 'You have not selected any Bike Station.'
    fig.update_layout(legend={'title_text': ''})
    fig2.update_layout(legend={'title_text': ''})
    return vl, fig, fig2


@app.callback(
    Output('forecast_info', 'children'),
    Input('forecast_upload', 'n_clicks')
)
def update_output(n_clicks):
    print(n_clicks)
    if n_clicks > 0:
        forecast_weather_transformation(forecast_weather_download())
    df_forecast = forecast_weather_transformation(
        df=pd.read_pickle("../Existing_stations/Data/Weather/forecast_weather_dict.pickle"))
    return 'You have forecast data since {}, to {}'.format(np.min(df_forecast.date), np.max(df_forecast.date))


