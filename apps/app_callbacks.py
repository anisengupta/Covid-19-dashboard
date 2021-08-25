# Initial Config
import dash_core_components as dcc
from dash.dependencies import Input, Output
from utils import dashboard_connector, config
from apps import app_layout

# Use the cache only if instructed in the config
if config.use_cache:
    from __init__ import cache
else:
    pass


# Callback functions
def register_app_callbacks(app):
    """
    Registers the callbacks for app_layout.py
    Parameters
    ----------
    app: the Dash App initialized.
    Returns
    -------
    The callbacks functions for the Dash App.
    """

    @app.callback(
        Output("time-series-chart-area", "children"),
        [Input("time-series-dropdown", "value")],
    )
    def update_time_series_chart(country_value: str):
        """
        Updates the time series chart on the Dash app based on the country
        dropdown value.

        Note that to reset the graph, if the country value is 'All', the
        original time series data is retrieved from the cache and used
        accordingly.

        Parameters
        ----------
        country_value: str, the name of the country to filter the dataframe by.

        Returns
        -------
        A chart to be displayed.

        """
        # Retrieve the dataframe
        if config.use_cache:
            # Retrieve the dataframe from the cache
            df = cache.get("covid-19-data")
        else:
            df = app_layout.get_covid_19_data()

        # Make a time series dataframe based on the country value
        if country_value == "All":
            if config.use_cache:
                df_time_series = cache.get("original-time-series-data")
            else:
                df_time_series = (
                    dashboard_connector.DashboardGraphs.create_time_series_data(df=df)
                )
        else:
            df_time_series = (
                dashboard_connector.DashboardGraphs.create_time_series_data(
                    df=df, country=country_value
                )
            )

        # Make a time series chart
        time_series_chart = app_layout.make_time_series_chart(
            data=df_time_series,
            x_col="date",
            y_cols=["new_cases", "new_deaths"],
            title=f"Cases and deaths by country for {country_value}",
            desc="Time series of Covid-19 cases",
        )

        return dcc.Graph(figure=time_series_chart)
