import os

from dash import dcc
from dash import html
from dash.dependencies import Input, Output


from app import app
from apps import app1, app2, app3, app4

app.layout = html.Div([
    html.H1(id='header-title', children='An application for management of Helsinki City Bikes',
            style={'width': '25vw', 'z-index': '1',
                           'margin-top': '2.5vh', 'position': 'absolute', 'top': "0vh", 'left': '27vw'}),

        dcc.RadioItems(
            id="app_select",
            options=[
                {'label': 'Existing Stations', 'value': 'app1'},
                {'label': 'New Stations', 'value': "app2"},
                {'label': 'File Upload', 'value': 'app3'},
                {'label': 'General Statistics', 'value': 'app4'}
            ],
            labelStyle={'display': 'flex'},
            style = {'width': '25vw', 'z-index': '1',
                           'margin-top': '2.5vh', 'position': 'absolute', 'top': "30vh", 'left': '2.25vw'}
        ),
    #dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


# @app.callback(Output('page-content', 'children'),
#               Input('url', 'pathname'))
# def display_page(pathname):
#     if pathname == '/apps/app1':
#         return app1.layout
#     elif pathname == '/apps/app2':
#         return app2.layout
#     elif pathname == '/apps/app3':
#         return app3.layout
#     else:
#         return app1.layout

# change app
@app.callback(
    Output('page-content', 'children'),
    Input('app_select', 'value')
)
def app_select(value):
    if value == 'app1':
        return app1.layout
    elif value == 'app2':
        return app2.layout
    elif value == 'app3':
        return app3.layout
    elif value == 'app4':
        return app4.layout


app.run_server(debug=False)

