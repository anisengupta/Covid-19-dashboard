# Initial Config
import pandas as pd
import numpy as np
import urllib.request
import json
import sqlalchemy as sqa


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
        data: dict, country_code: str, _index: int, columns: list
    ) -> pd.DataFrame:
        """
        Makes a pandas dataframe based on the country_code, _index and columns params.

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        country_code: str, the country code to get data for, eg 'AFG'.
        _index: int, the index to be specified to get data for.
        columns: list, the columns to be filtered.

        Returns
        -------
        A pandas dataframe.

        """
        df = pd.DataFrame.from_dict(
            data[country_code]["data"][_index], orient="index"
        ).T
        df["location"] = data[country_code]["location"]

        return GetData.filter_dataframe(df=df, columns=columns)

    @staticmethod
    def dataframe_per_country(
        data: dict, country_code: str, columns: list
    ) -> pd.DataFrame:
        """
        Creates an overall dataframe for an overall country based on a specific country_code.

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        country_code: str, the country code to get data for, eg 'AFG'.
        columns: list, the columns to be filtered.

        Returns
        -------
        A pandas dataframe.

        """
        country_data = data[country_code]["data"]
        df_list = []

        for _index in range(0, len(country_data)):
            df = GetData.make_dataframe(
                data=data, country_code=country_code, _index=_index, columns=columns
            )
            df_list.append(df)

        return pd.concat(df_list, axis=0)

    @staticmethod
    def dataframe_all_countries(
        data: dict, dict_keys: list, columns: list
    ) -> pd.DataFrame:
        """
        Gets data for all the countries based on a list of country codes (the dict_keys param).

        Parameters
        ----------
        data: dict, the data dictionary created using the func get_json_data.
        dict_keys: list, the keys of the dictionary created using the get_json_data func, this should
        contain the list of country codes.
        columns: list, the columns to be filtered.

        Returns
        -------
        A pandas dataframe.

        """
        df_list = []

        for country_code in dict_keys:
            print(f"Evaluating data for {country_code}")
            df = GetData.dataframe_per_country(
                data=data, country_code=country_code, columns=columns
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
        A string in the following format: postgresql+psycopg2://{username}:{password}@localhost:5432/{database}

        """
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
    def push_to_postgres(df: pd.DataFrame, table_name: str, engine: sqa.engine):
        """
        Pushes a dataframe to the relevant Postgres database; based on the engine param created.

        Parameters
        ----------
        df: pd.Dataframe, the dataframe to be pushed.
        table_name: str, the name of the table inside the database to be pushed to.
        engine: sqa.engine, the SQL Alchemy engine created.

        Returns
        -------
        The dataframe being pushed to the relevant database and table.

        """
        return df.to_sql(table_name, engine)
