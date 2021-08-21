# Initial Config
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from apps import app_layout, app_callbacks
from utils import dashboard_connector, config
from __init__ import app, cache

# Initiate the app
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

# Make the cases by country h bar chart
query = f"""
SELECT * FROM {config.table_name}
"""

# Construct the engine url
engine_url = dashboard_connector.Postgres(
    username=config.username, password=config.password
).construct_engine_url(database=config.database)

# Initiate the connection
engine = dashboard_connector.Postgres(
    username=config.username, password=config.password
).create_engine(engine_url=engine_url)

df = dashboard_connector.Postgres(
    username=config.username, password=config.password
).get_data_from_postgres(query=query, engine=engine)
cache.set("covid-19-data", df)

# Perform a groupby
df_groupby = dashboard_connector.DashboardGraphs.group_by_data(
    df=df, group_by_col="location"
)

# Remove the continents
df_groupby = df_groupby[~df_groupby["location"].isin(config.continents)]

# Make death rate data
df_group_by_death_rate = dashboard_connector.DashboardGraphs.create_death_rate_data(
    group_by_df=df_groupby
)

# Make a cases by country chart
cases_by_country_chart = app_layout.make_horizontal_bar_chart(
    data=df_groupby.sort_values(by=["total_cases"], ascending=False).head(10),
    x_col="total_cases",
    y_col="location",
    title="Cases by country",
    desc="Which countries have recorded the most cases so far",
)

# Make a deaths by country chart
deaths_by_country_chart = app_layout.make_horizontal_bar_chart(
    data=df_groupby.sort_values(by=["total_deaths"], ascending=False).head(10),
    x_col="total_deaths",
    y_col="location",
    title="Deaths by country",
    desc="Which countries have recorded the most deaths",
)

# Make a deaths by country chart
death_rate_by_country_chart = app_layout.make_horizontal_bar_chart(
    data=df_group_by_death_rate.sort_values(by=["death_rate"], ascending=False).head(
        10
    ),
    x_col="death_rate",
    y_col="location",
    title="Death rate by country",
    desc="Which countries have the greatest death rates (%)",
)

# Create a horizontal charts row
horizontal_chart_row = html.Div(
    dbc.Row(
        children=[
            dbc.Col(
                [dcc.Graph(id="cases-by-country-graph", figure=cases_by_country_chart)],
                width=4,
            ),
            dbc.Col(
                dcc.Graph(id="deaths-by-country-graph", figure=deaths_by_country_chart),
                width=4,
            ),
            dbc.Col(
                [
                    dcc.Graph(
                        id="death-rate-by-country-graph",
                        figure=death_rate_by_country_chart,
                    ),
                    html.Div("Death Rate = Number of deaths/number of cases"),
                ],
                width=4,
            ),
        ]
    )
)

# Make a time series dataframe
df_time_series = dashboard_connector.DashboardGraphs.create_time_series_data(df=df)
cache.set("original-time-series-data", df_time_series)

# Make a time series chart
time_series_chart = app_layout.make_time_series_chart(
    data=df_time_series,
    x_col="date",
    y_cols=["new_cases", "new_deaths"],
    title="Cases and deaths by country",
    desc="Time series of Covid-19 cases",
)

# Make time series dropdown options
options = dashboard_connector.DashboardGraphs.create_dropdown_options(df=df)

# Make the time series dropdown
dropdown = app_layout.make_time_series_dropdown(options=options)

time_series_row = html.Div(
    dbc.Row(
        children=[
            dbc.Col(
                html.Div(dcc.Graph(id="time-series-chart", figure=time_series_chart)),
                width=10,
                id="time-series-chart-area",
            ),
            dbc.Col([app_layout.make_break(), app_layout.make_break(), dropdown]),
        ]
    )
)

# Set the layout
app.layout = html.Div(
    children=[title, horizontal_chart_row, app_layout.make_break(), time_series_row]
)

# Callbacks
app_callbacks.register_app_callbacks(app=app)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
