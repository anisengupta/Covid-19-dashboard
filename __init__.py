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

# Set the app title
app.title = 'Covid 19 data dashboard'

# Set the cache config
config = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300
}


# Set the cache config
cache = Cache(app.server,
              config=config
              )

# Initialise the cache
cache.init_app(app.server)