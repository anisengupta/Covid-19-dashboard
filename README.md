# Covid-19-dashboard
A Dash app Dashboard created using Dash (https://dash.plotly.com/introduction) which is a productive Python framework built for writing web applications.

Covid-19 data is accessed from the Our World in Data website and parsed into a dataframe format accordingly (https://covid.ourworldindata.org/data/owid-covid-data.json).

Data is retrieved every day and this scheduling happens everyday locally using Airflow (https://airflow.apache.org/). Data is retrieved in a json format, it is then cleaned and pushed through to a Postgres database. When the Dash app is intialized; this data is then retrieved from Postgres and visualised.

This is a screenshot of the dashboard:

![COVID-19 GDS Dashboard](https://github.com/anisengupta/Covid-19-dashboard/blob/ani/images/covid_19_dash_app.png?raw=true)

The dashboard can be accessed here: https://covid-19-dashboard-dot-covid-19-dashboard-323716.nw.r.appspot.com/
