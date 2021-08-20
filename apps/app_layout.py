# Initial Config
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
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
                        dbc.Col(dbc.NavbarBrand("Latest COVID-19 data from Our World in Data", className="ml-2")),
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
