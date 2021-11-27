import dash_uploader as du
from dash import html, dcc, Output, Input, State
from app import app
from functions.functions import *


layout = html.Div([
    html.H1('An application for management of Helsinki City Bikes'),
    du.Upload(id='dash-uploader',
              text_completed='New bike data added: ',
              max_file_size=10000,
              chunk_size=2048,
              filetypes=['csv']),
html.Div(id='clickdata2')
])


@app.callback(Output("clickdata2", "children"),
    [Input('dash-uploader', 'isCompleted')],
              [State('dash-uploader', 'fileNames')]
)
def callback_on_completion(iscompleted, fileNames):
    print(iscompleted)
    print(fileNames)
    path_str = '.\data\\' + fileNames[0]
    print(path_str)
    if iscompleted:
        df = load_and_filter_data(path_str)
        print(df.shape)
    return 'uploaded'