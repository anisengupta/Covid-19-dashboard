# Initial Config
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from apps import app_layout

# Initiate the app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.UNITED],
)

server = app.server

# Set the title
title = app_layout.make_navbar_title()

# Set the layout
app_layout_style = {
    "position": "fixed",
    "max-width": "100vw",
    "min-width": "100vw",
    "max-height": "100vh",
    "min-height": "100vh",
    "background-image": "radial-gradient(#697582, #383F49,#383F49)",
}

# Set the layout
app.layout = html.Div(
    children=[
        title
    ]
)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)