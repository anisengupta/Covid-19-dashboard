# Initial Config
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from utils import config, dashboard_connector

# Use the cache only if instructed in the config
if config.use_cache:
    from __init__ import cache
else:
    pass


# Functions
def get_covid_19_data():
    if config.use_saved_data:
        filepath = '/Users/aniruddha.sengupta/PycharmProjects/Covid-19-dashboard/data/df.pq'
        df = pd.read_parquet(filepath)
    else:
        # Make the cases by country h bar chart
        query = f"""
            SELECT * FROM {config.table_name}
            """

        # Construct the engine url
        print('Constructing the engine url')
        engine_url = dashboard_connector.Postgres(
            username=config.username, password=config.password
        ).construct_engine_url(database=config.database)

        # Initiate the connection
        print('Initiating the connection')
        engine = dashboard_connector.Postgres(
            username=config.username, password=config.password
        ).create_engine(engine_url=engine_url)

        print('Retrieving the dataframe')
        df = dashboard_connector.Postgres(
            username=config.username, password=config.password
        ).get_data_from_postgres(query=query, engine=engine)

    return df


def make_navbar_title():
    """
    Sets the navigation bar on the top of the page with the title.

    Returns
    -------
    A navbar title.

    """
    navbar = dbc.Navbar(
        [
            html.A(
                # Use row and col to control vertical alignment of logo / brand
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.NavbarBrand(
                                "Latest COVID-19 data from Our World in Data",
                                className="ml-2",
                            )
                        ),
                        dbc.Col(html.Img(src=config.navbar_logo_url, height="30px")),
                    ],
                    align="center",
                    no_gutters=True,
                ),
                href=config.navbar_logo_url,
            ),
        ],
        color="primary",
        dark=True,
    )

    return navbar


def make_break():
    """
    Makes the break in the layout.
    Returns
    -------
    A break in the page.
    """
    return html.Br()


def make_bar_chart_title_explanation(title: str, desc: str) -> str:
    """
    Creates the title and description for the chart.

    Parameters
    ----------
    title: str, the title of the graph.
    desc: str, the description of the graph.

    Returns
    -------
    A string with like so:
    Title of graph

    Description of graph

    """
    html_str = f"""
    <b>{title}</b>
    <br>
    <br>
    {desc}
    <br>
    <br>
    """

    return html_str


def make_horizontal_bar_chart(
    data: pd.DataFrame, x_col: str, y_col: str, title: str, desc: str
):
    """
    Makes a horizontal bar chart based on the data inputs.

    Parameters
    ----------
    data: the pandas dataframe input.
    x_col: str, the name of the x column.
    y_col: str, the name of the y column.
    title: str, the title of the graph.
    desc: str, the description of the chart.

    Returns
    -------
    A Plotly horizontal bar chart.

    """
    # Construct the x and y values
    x_values = data[x_col]
    y_values = data[y_col]

    fig = go.Figure(go.Bar(x=x_values, y=y_values, orientation="h"))

    # Update the layout so the bar chart is in descending order
    fig.update_layout(
        title=make_bar_chart_title_explanation(title, desc),
        font_family=config.font,
        title_font_family=config.font,
        title_font_color="orange",
        yaxis=dict(autorange="reversed"),
    )

    return fig


def make_time_series_chart(
    data: pd.DataFrame, x_col: str, y_cols: list, title: str, desc: str
):
    """
    Makes a time series chart based on the input params. Note that it is
    assumed that the param x_col will be a datetime column.

    Parameters
    ----------
    data: the pandas dataframe input.
    x_col: str, the x column.
    y_cols: list, the y columns.
    title: str, the title of the graph.
    desc: str, the description of the graph.

    Returns
    -------

    """
    fig = px.line(data, x=x_col, y=y_cols)

    # Update the layout so the bar chart is in descending order
    fig.update_layout(
        title=make_bar_chart_title_explanation(title, desc),
        font_family=config.font,
        title_font_family=config.font,
        title_font_color="black",
    )

    return fig


def make_time_series_dropdown(options: list):
    """
    Makes a Dash dropdown based on the options provided.

    Parameters
    ----------
    options: list, a list of dictionaries to be provided.

    Returns
    -------
    A Dash dropdown.

    """
    dropdown = dcc.Dropdown(id="time-series-dropdown", options=options)

    return dropdown


def make_choropleth_map(data: pd.DataFrame,
                        country_code_col: str,
                        country_col: str,
                        display_col: str,
                        title: str,
                        desc: str,
                        colour: str
                        ):
    """
    Makes a choropleth world map figure.

    Parameters
    ----------
    data: the pandas dataframe input.
    country_code_col: str, the country code column.
    country_col: str, the country name column.
    display_col: str, the column to be displayed in the figure.
    title: str, the title of the graph.
    desc: str, the description of the graph.
    colour: str, the colour of the graph.

    Returns
    -------
    A choropleth world map figure.

    """
    # Handle colour input param
    if colour == 'Blue':
        color_continuous_scale = px.colors.sequential.Sunset
    else:
        color_continuous_scale = px.colors.sequential.Magenta

    # Set the figure
    fig = px.choropleth(data_frame=data,
                        locations=country_code_col,
                        color=display_col,
                        hover_name=country_col,
                        color_continuous_scale=color_continuous_scale)

    # Update the layout of the figure
    fig.update_layout(
        title_text=make_bar_chart_title_explanation(title, desc),
        font_family=config.font,
        title_font_family=config.font,
        width=900,
        height=600
    )

    return fig


def initiate_app_layout():
    """
    Initiates the entire app layout.

    Returns
    -------
    The app layout to be served.

    """
    # Set the title
    print('Setting the title')
    title = make_navbar_title()

    # Set the layout
    print('Setting the layout')
    app_layout_style = {
        "position": "fixed",
        "max-width": "100vw",
        "min-width": "100vw",
        "max-height": "100vh",
        "min-height": "100vh",
        "background-image": "radial-gradient(#697582, #383F49,#383F49)",
    }

    # Get the data
    df = get_covid_19_data()

    if config.use_cache:
        print('Setting to cache')
        cache.set("covid-19-data", df)
    else:
        pass

    # Perform a groupby
    print('Performing a groupby')
    df_groupby = dashboard_connector.DashboardGraphs.group_by_data(
        df=df, group_by_col="location"
    )

    # Remove the continents
    print('Removing the continents from the groupby dataframe')
    df_groupby = df_groupby[~df_groupby["location"].isin(config.continents)]

    # Make death rate data
    print('Making death rate data')
    df_group_by_death_rate = dashboard_connector.DashboardGraphs.create_death_rate_data(
        group_by_df=df_groupby
    )

    # Make a cases by country chart
    print('Making a cases by country chart')
    cases_by_country_chart = make_horizontal_bar_chart(
        data=df_groupby.sort_values(by=["total_cases"], ascending=False).head(10),
        x_col="total_cases",
        y_col="location",
        title="Cases by country",
        desc="Which countries have recorded the most cases so far",
    )

    # Make a deaths by country chart
    print('Making a deaths by country chart')
    deaths_by_country_chart = make_horizontal_bar_chart(
        data=df_groupby.sort_values(by=["total_deaths"], ascending=False).head(10),
        x_col="total_deaths",
        y_col="location",
        title="Deaths by country",
        desc="Which countries have recorded the most deaths",
    )

    # Make a death rate by country chart
    print('Making a death rate by country chart')
    death_rate_by_country_chart = make_horizontal_bar_chart(
        data=df_group_by_death_rate.sort_values(by=["death_rate"], ascending=False).head(
            10
        ),
        x_col="death_rate",
        y_col="location",
        title="Death rate by country",
        desc="Which countries have the greatest death rates (%)",
    )

    # Create a horizontal charts row
    print('Creating a horizontal charts row')
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
    print('Making a time series dataframe')
    df_time_series = dashboard_connector.DashboardGraphs.create_time_series_data(df=df)

    if config.use_cache:
        print('Setting the time series dataframe to cache')
        cache.set("original-time-series-data", df_time_series)
    else:
        pass

    # Make a time series chart
    print('Making a time series chart')
    time_series_chart = make_time_series_chart(
        data=df_time_series,
        x_col="date",
        y_cols=["new_cases", "new_deaths"],
        title="Cases and deaths by country",
        desc="Time series of Covid-19 cases",
    )

    # Make time series dropdown options
    print('Making the time series dropdown options')
    options = dashboard_connector.DashboardGraphs.create_dropdown_options(df=df)

    # Make the time series dropdown
    print('Creating the time series dropdown')
    dropdown = make_time_series_dropdown(options=options)

    print('Making a time series row')
    time_series_row = html.Div(
        dbc.Row(
            children=[
                dbc.Col(
                    html.Div(dcc.Graph(id="time-series-chart", figure=time_series_chart)),
                    width=10,
                    id="time-series-chart-area",
                ),
                dbc.Col([make_break(), make_break(), dropdown]),
            ]
        )
    )

    # Make a world map of cases & deaths
    print('Making a world map of cases')
    df_groupby_choropleth_cases = dashboard_connector.DashboardGraphs.create_choropleth_data(
        df=df, col='total_cases'
    )

    print('Making a world map of deaths')
    df_groupby_choropleth_deaths = dashboard_connector.DashboardGraphs.create_choropleth_data(
        df=df, col='total_deaths'
    )

    cases_world_map = make_choropleth_map(
        data=df_groupby_choropleth_cases,
        country_code_col='country_code',
        country_col='location',
        display_col='total_cases',
        title='Map of cases',
        desc='',
        colour='Blue'
    )

    deaths_world_map = make_choropleth_map(
        data=df_groupby_choropleth_deaths,
        country_code_col='country_code',
        country_col='location',
        display_col='total_deaths',
        title='Map of deaths',
        desc='',
        colour='Red'
    )

    # Make a world map row
    print('Making a world map row')
    world_map_row = html.Div(
        dbc.Row(
            children=[
                dbc.Col([
                    html.Div(dcc.Graph(id='map-of-cases', figure=cases_world_map))
                ]),
                dbc.Col([
                    html.Div(dcc.Graph(id='map-of-deaths', figure=deaths_world_map))
                ])

            ]
        )
    )

    # Set the layout
    print('Setting the app layout')
    app_layout = html.Div(
        children=[
            title,
            horizontal_chart_row,
            make_break(),
            time_series_row,
            world_map_row
        ]
    )

    return app_layout
