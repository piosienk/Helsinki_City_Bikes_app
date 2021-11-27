from dash import dcc
from dash import html
from dash.dependencies import Input, Output

from app import app
from apps import app1, app2, app3


app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/apps/app1':
        return app1.layout
    elif pathname == '/apps/app2':
        return app2.layout
    elif pathname == '/apps/app3':
        return app3.layout
    else:
        return app1.layout


app.run_server(debug=False)