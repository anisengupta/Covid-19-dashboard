# Initial Config
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from utils import config
import plotly.graph_objects as go
import pandas as pd


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


def make_horizontal_bar_chart(data: pd.DataFrame, x_col: str, y_col: str, title: str):
    """
    Makes a horizontal bar chart based on the data inputs.

    Parameters
    ----------
    data: the pandas dataframe input.
    x_col: str, the name of the x column.
    y_col: str, the name of the y column.
    title: str, the title of the graph.

    Returns
    -------
    A Plotly horizontal bar chart.

    """
    # Construct the x and y values
    x_values = data[x_col]
    y_values = data[y_col]

    fig = go.Figure(go.Bar(x=x_values, y=y_values, orientation="h"))

    # Update the layout so the bar chart is in descending order
    fig.update_layout(title=title, yaxis=dict(autorange="reversed"))

    return fig
