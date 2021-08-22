# Initial Config
import dash
import dash_bootstrap_components as dbc
from flask_caching import Cache
from utils import config

# Initiate the app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.UNITED],
)

# Set the app title
app.title = 'Covid 19 data dashboard'

# Set the cache config
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 7200,
    'CACHE_REDIS_HOST': config.REDISHOST,
    'CACHE_REDIS_PORT':config.REDISPORT
    }


# Set the cache config
cache = Cache(app.server,
              config=config
              )

# Initialise the cache
cache.init_app(app.server)