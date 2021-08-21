# Initial Config
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from utils import config


# Functions
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


def make_horizontal_bar_chart(data: pd.DataFrame,
                              x_col: str,
                              y_col: str,
                              title: str,
                              desc: str):
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
        yaxis=dict(autorange="reversed")
    )

    return fig


def make_time_series_chart(data: pd.DataFrame,
                           x_col: str,
                           y_cols: list,
                           title: str,
                           desc: str):
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
    dropdown = dcc.Dropdown(
        id='time-series-dropdown',
        options=options
    )

    return dropdown

