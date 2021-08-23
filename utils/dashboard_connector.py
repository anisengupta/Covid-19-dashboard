# Initial Config
import os
import pandas as pd
import numpy as np
import urllib.request
import json
import sqlalchemy as sqa
import sys
import importlib
import logging
from datetime import date
from google.cloud import storage
import google.auth
from google.oauth2 import service_account

try:
    from utils import config
except:
    code_root = '/Users/aniruddha.sengupta/PycharmProjects/Covid-19-dashboard/utils'
    if code_root not in sys.path:
        sys.path.insert(0, code_root)

    module = 'config'
    config = importlib.import_module(module)


# Classes
class GetData:
    """
    Class gets Covid-19 data from a json format and parses into a pandas dataframe.

    """

    def __init__(self):
        pass

    @staticmethod
    def get_json_data(url_path: str) -> dict:
        """
        Gets json data from a url specified and parses into a dictionary.

        Parameters
        ----------
        url_path: str, the path of the url.

        Returns
        -------
        A dictionary with Covid-19 data.

        """
        with urllib.request.urlopen(url=url_path) as url:
            data = json.loads(url.read().decode())

            return data

    @staticmethod
    def make_dict_keys(data: dict) -> list:
        """
        Stores the keys of the dictionary created in a list.
        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.

        Returns
        -------
        A list of keys: these should be country codes.

        """
        return list(data.keys())

    @staticmethod
    def make_country_codes_dataframe(url: str) -> pd.DataFrame:
        """
        Creates a table of countries and their respective ISO codes.

        Parameters
        ----------
        url: str, the url to parse through.

        Returns
        -------
        A pandas dataframe.

        """
        return pd.read_html(url)[1]

    @staticmethod
    def make_geo_ids_dict(geo_id_df: pd.DataFrame) -> dict:
        """
        Makes a dictionary of country codes with their respective geo ids.

        Parameters
        ----------
        geo_id_df: pd.DataFrame, the input dataframe with the country codes and their geo ids.

        Returns
        -------
        A dict.

        """
        geo_ids = geo_id_df["ISO-3166alpha2"].tolist()
        country_codes = geo_id_df["ISO-3166alpha3"].tolist()

        return dict(zip(country_codes, geo_ids))

    @staticmethod
    def filter_dataframe(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Filters the dataframe to the columns param specified.

        Parameters
        ----------
        df: a pandas dataframe input.
        columns: list, the columns to be filtered.

        Returns
        -------
        A filtered dataframe.

        """
        df_columns = df.columns.tolist()
        cols_to_add = list(set(columns) - set(df_columns))

        if len(cols_to_add) == 0:
            return df[columns]
        else:
            df[cols_to_add] = np.nan
            return df[columns]

    @staticmethod
    def make_dataframe(
        data: dict, country_code: str, _index: int, columns: list, geo_ids_dict: dict
    ) -> pd.DataFrame:
        """
        Makes a pandas dataframe based on the country_code, _index and columns params.

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        country_code: str, the country code to get data for, eg 'AFG'.
        _index: int, the index to be specified to get data for.
        columns: list, the columns to be filtered.
        geo_ids_dict: dict, a dict of country codes and their respective geo ids.

        Returns
        -------
        A pandas dataframe.

        """
        df = pd.DataFrame.from_dict(
            data[country_code]["data"][_index], orient="index"
        ).T

        # Make additional columns
        df["country_code"] = country_code
        df["location"] = data[country_code]["location"]
        df["geo_id"] = geo_ids_dict.get(country_code)

        return GetData.filter_dataframe(df=df, columns=columns)

    @staticmethod
    def dataframe_per_country(
        data: dict, country_code: str, columns: list, geo_ids_dict: dict
    ) -> pd.DataFrame:
        """
        Creates an overall dataframe for an overall country based on a specific country_code.

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        country_code: str, the country code to get data for, eg 'AFG'.
        columns: list, the columns to be filtered.
        geo_ids_dict: dict, a dict of country codes and their respective geo ids.

        Returns
        -------
        A pandas dataframe.

        """
        country_data = data[country_code]["data"]
        df_list = []

        for _index in range(0, len(country_data)):
            df = GetData.make_dataframe(
                data=data,
                country_code=country_code,
                _index=_index,
                columns=columns,
                geo_ids_dict=geo_ids_dict,
            )
            df_list.append(df)

        return pd.concat(df_list, axis=0)

    @staticmethod
    def dataframe_all_countries(
        data: dict, dict_keys: list, columns: list, geo_ids_dict: dict
    ) -> pd.DataFrame:
        """
        Gets data for all the countries based on a list of country codes (the dict_keys param).

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        dict_keys: list, the keys of the dictionary created using the get_json_data func, this should
        contain the list of country codes.
        columns: list, the columns to be filtered.
        geo_ids_dict: dict, a dict of country codes and their respective geo ids.

        Returns
        -------
        A pandas dataframe.

        """
        df_list = []

        for country_code in dict_keys:
            print(f"\rEvaluating data for {country_code}", end="")
            df = GetData.dataframe_per_country(
                data=data,
                country_code=country_code,
                columns=columns,
                geo_ids_dict=geo_ids_dict,
            )
            df_list.append(df)

        return pd.concat(df_list, axis=0)


class Postgres:
    """
    Class contains functions to push data to a Postgres SQL database.

    """

    def __init__(self, username: str, password: str):
        """
        Initialisation occurs with a username and password.

        Parameters
        ----------
        username: str, the username to connect to the Postgres SQL database.
        password: str, the password to connect to the Postgres SQL database.
        """
        self.username = username
        self.password = password

    def construct_engine_url(self, database: str) -> str:
        """
        Constructs the engine url required to secure a connection to Postgres.

        Parameters
        ----------
        database: str, the name of the database to connect to.

        Returns
        -------
        A string in the following format: postgresql+psycopg2://{username}:{password}@172.17.0.1:5432/{database}

        """
        if config.env == 'gcp_prod':
            return f"postgresql+psycopg2://{self.username}:{self.password}@172.17.0.1:5432/{database}"
        else:
            return f"postgresql+psycopg2://{self.username}:{self.password}@localhost:5432/{database}"

    @staticmethod
    def create_engine(engine_url: str) -> sqa.engine:
        """
        Constructs a SQL Alchemy Postgres engine to push data to.

        Parameters
        ----------
        engine_url: str, the engine url to connect to Postgres, constructed using the func construct_engine_url.

        Returns
        -------
        A SQL Alchemy engine to perform SQL operations.

        """
        return sqa.create_engine(engine_url)

    @staticmethod
    def push_to_postgres(
        df: pd.DataFrame, table_name: str, engine: sqa.engine, job_type: str
    ):
        """
        Pushes a dataframe to the relevant Postgres database; based on the engine param created.

        Parameters
        ----------
        df: pd.Dataframe, the dataframe to be pushed.
        table_name: str, the name of the table inside the database to be pushed to.
        engine: sqa.engine, the SQL Alchemy engine created.
        job_type: str, the job to do if data in the dataframe already exists, eg 'append', 'replace'.

        Returns
        -------
        The dataframe being pushed to the relevant database and table.

        """
        return df.to_sql(table_name, engine, if_exists=job_type)

    @staticmethod
    def get_data_from_postgres(query: str, engine: sqa.engine) -> pd.DataFrame:
        """
        Retrieves data from Postgres based on the relevant query and
        engine params.

        Parameters
        ----------
        query: str, the query to execute.
        engine: sqa.engine, the engine needed for the connection.

        Returns
        -------
        A pandas dataframe

        """
        return pd.read_sql(query, engine)


class DashboardGraphs:
    """
    Class contains functions to create and manipulate data to show graphs
    on the Dash app.

    """

    def __init__(self):
        pass

    @staticmethod
    def group_by_data(df: pd.DataFrame, group_by_col: str) -> pd.DataFrame:
        """
        Performs a groupby on the initial dataframe.

        Parameters
        ----------
        df: a pandas dataframe input.
        group_by_col: str, the column by which to perform the groupby.

        Returns
        -------
        A pandas dataframe.

        """
        return (
            df.groupby([group_by_col])["total_cases", "total_deaths"]
            .max()
            .reset_index()
        )

    @staticmethod
    def create_death_rate_data(group_by_df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a dataframe that calculates death rate of Covid-19 for each
        country.

        Parameters
        ----------
        group_by_df: a grouped dataframe.

        Returns
        -------
        A pandas dataframe.

        """
        group_by_df["death_rate"] = (
            group_by_df["total_deaths"] / group_by_df["total_cases"]
        ) * 100

        return group_by_df

    @staticmethod
    def create_time_series_data(df: pd.DataFrame, country: str = None) -> pd.DataFrame:
        """
        Creates a dataframe required to make a timeseries chart in the Dash app.

        Parameters
        ----------
        df: the pandas dataframe input.
        country: str, the country to filter the dataframe by.

        Returns
        -------
        A pandas dataframe.

        """

        # If the country_code param is passed in the filter the df accordingly
        if country:
            df = df[df["location"] == country]
        else:
            pass

        # Remove the continents
        df = df[~df["location"].isin(config.continents)]

        # Group by new cases & deaths
        df_grouped = df.groupby("date")["new_cases", "new_deaths"].sum().reset_index()

        # Remove null values in new_cases & new_deaths cols
        df_grouped["new_cases"] = df_grouped["new_cases"].fillna(0)
        df_grouped["new_deaths"] = df_grouped["new_deaths"].fillna(0)

        return df_grouped

    @staticmethod
    def create_dropdown_options(df: pd.DataFrame) -> list:
        """
        Creates a list of locations to be used as the Dash dropdown values.

        Parameters
        ----------
        df: a pandas dataframe input.

        Returns
        -------

        """
        # Create a list of locations
        locations = df["location"].unique().tolist()

        # Add an 'All'
        locations = ["All"] + locations

        # Filter out the continents
        locations = [i for i in locations if i not in config.continents]

        # Construct the required list of dictionaries
        sub_dicts = []
        for location in locations:
            sub_dict = {"label": location, "value": location}
            sub_dicts.append(sub_dict)

        return sub_dicts

    @staticmethod
    def create_choropleth_data(df: pd.DataFrame, col: str) -> pd.DataFrame:
        """
        Makes a dataframe to be used in the choropleth graph creation.

        Parameters
        ----------
        df: the pandas dataframe input.
        col: str, the name of the column to be displayed.

        Returns
        -------
        A pandas dataframe.

        """
        # Perform the groupby
        df_groupby = df.groupby(['country_code', 'location'])[col].max().reset_index()

        # Remove null values
        df_groupby[col] = df_groupby[col].fillna(0)

        return df_groupby


class Logging:
    """
    Logs all messages and outputs from the bot.
    """

    def __init__(self):
        pass

    @staticmethod
    def create_filename(filepath: str) -> str:
        """
        Creates a filename with the latest date.
        Parameters
        ----------
        filepath: str, the filepath of the logger file.
        Returns
        -------
        A filename with the latest date.
        """
        today = date.today()
        d1 = today.strftime("%d_%m_%Y")

        return filepath + "/covid_19_update_log_" + d1 + ".txt"

    @staticmethod
    def create_logging_config(filepath: str):
        """
        Creates a configuration to be used for logging purposes.
        Parameters
        ----------
        filepath: str, the filepath of the logger file.
        Returns
        -------
        """
        filename = Logging().create_filename(filepath=filepath)

        return logging.basicConfig(
            filename=filename,
            filemode="a",
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            level=logging.INFO,
        )


def get_covid_19_data_from_source() -> pd.DataFrame:
    """
    Retrieves and cleans the latest up-to-date Covid-19 data directly from
    the source.

    Returns
    -------
    A pandas dataframe.

    """
    url_path = "https://covid.ourworldindata.org/data/owid-covid-data.json"
    data = GetData.get_json_data(url_path=url_path)
    dict_keys = GetData.make_dict_keys(data=data)

    # Making the geo ids dict
    geo_id_df = GetData.make_country_codes_dataframe(
        url=config.geo_ids_url
    )
    geo_ids_dict = GetData.make_geo_ids_dict(geo_id_df=geo_id_df)

    # Make the pandas dataframe
    df = GetData.dataframe_all_countries(
        data=data, dict_keys=dict_keys, columns=config.columns, geo_ids_dict=geo_ids_dict
    )

    # Making the date column into datetime
    df["date"] = pd.to_datetime(df["date"])

    # Sorting the dataframe by date and country code
    df = df.sort_values(by=["location", "date"])

    return df

