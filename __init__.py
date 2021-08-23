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


# Set the cache config - if set to do so in the config
if config.use_cache:
    print('Setting the cache config')
    if config.env == 'gcp_prod':
        print('Using the Redis cache for the gcp prod env')
        cache_config = {
            'CACHE_TYPE': 'redis',
            'CACHE_REDIS_HOST': '10.56.192.187',
            'CACHE_REDIS_PORT': '6379'
        }
    else:
        print('Using a simple cache for the local env')
        cache_config = {
            "DEBUG": True,          # some Flask specific configs
            "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
            "CACHE_DEFAULT_TIMEOUT": 300
        }

    # Set the cache config
    cache = Cache(app.server,
                  config=cache_config
                  )

    # Initialise the cache
    cache.init_app(app.server)
else:
    print('Instructed not to use the cache config')
    pass

if config.use_saved_data:
    print('Using the saved data')
else:
    pass
