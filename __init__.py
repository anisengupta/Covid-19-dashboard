# Initial Config
import dash
import dash_bootstrap_components as dbc
from flask_caching import Cache

# Initiate the app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.UNITED],
)

app.title = 'Covid 19 data dashboard'

# Set the cache config
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
})

# Initialise the cache
cache.init_app(app.server)