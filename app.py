import dash
import dash_uploader as du

app = dash.Dash(__name__, suppress_callback_exceptions=True)
server = app.server

UPLOAD_FOLDER_ROOT = r".\data"
du.configure_upload(app, UPLOAD_FOLDER_ROOT, use_upload_id=False)