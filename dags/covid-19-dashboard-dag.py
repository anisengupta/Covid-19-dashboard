# Initial Config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
from datetime import datetime, timedelta
import pandas as pd
import sys
import importlib
import json

# Import the dashboard connector code
try:
    from utils import dashboard_connector
except:
    code_root = "/Users/aniruddha.sengupta/PycharmProjects/Covid-19-dashboard/utils"
    if code_root not in sys.path:
        sys.path.insert(0, code_root)

    module = "dashboard_connector"
    dashboard_connector = importlib.import_module(module)

# Import the config
try:
    from utils import config
except:
    module = "config"
    config = importlib.import_module(module)

# Import the passwords.json file
f = open(config.passwords_file_path)
passwords_dict = json.load(f)


def covid_19_dashboard_update(
    username: str,
    password: str,
    database: str,
    table_name: str,
):
    """
    Process for updating the data to get the latest Covid-19 data
    and push to a Postgres table. Google Data Studio is then automatically
    configured to refresh to get the latest data.

    Parameters
    ----------
    username: str, the username.
    password: str, the password.
    database: str, the database name in Postgres.
    table_name: str, the name of the table in Postgres.

    Returns
    -------
    A refreshed Postgres database.

    """
    # Initiate the logger
    logger_filepath = "/Users/aniruddha.sengupta/Desktop/Covid-19 dashboard/logs"
    dashboard_connector.Logging.create_logging_config(filepath=logger_filepath)

    # Starting the logging
    logging.info("The Covid-19 Google Data Studio dashboard process has now started.")

    # Get the data
    df = dashboard_connector.get_covid_19_data_from_source()

    # Construct the engine url
    logging.info("Constructing the local engine url")
    engine_url = dashboard_connector.Postgres(
        username=username, password=password
    ).construct_engine_url(database=database)
    logging.info(engine_url)

    # Initiate the connection
    logging.info("Initiating the connection with the local Postgres")
    engine = dashboard_connector.Postgres(
        username=username, password=password
    ).create_engine(engine_url=engine_url)

    # Push the dataframe to Postgres
    logging.info("Pushing the dataframe to Postgres")
    dashboard_connector.Postgres(username=username, password=password).push_to_postgres(
        df=df, table_name=table_name, engine=engine, job_type="replace"
    )

    # Logging the end
    logging.info("The process has now ended")


def local_to_heroku_postgres(
    username: str, password: str, local_database: str, local_table_name: str
):
    """
    Pushed the Covid-19 data from the local Postgres data to the Heroku one.

    Parameters
    ----------
    username: str, the username.
    password: str, the password.
    local_database: str, the name of the local database.
    local_table_name: str, the name of the local table.

    Returns
    -------
    A refreshed Heroku Postgres database.

    """
    # Get data from the local Postgres database
    query = f"""SELECT * FROM {local_table_name}"""

    # Construct the engine url
    logging.info("Constructing the local engine url")
    engine_url = dashboard_connector.Postgres(
        username=username, password=password
    ).construct_engine_url(database=local_database)
    logging.info(engine_url)

    # Initiate the connection
    logging.info("Initiating the connection with the local Postgres")
    engine = dashboard_connector.Postgres(
        username=config.username, password=passwords_dict.get("postgres_password")
    ).create_engine(engine_url=engine_url)

    logging.info("Getting data from local Postgres")
    df = dashboard_connector.Postgres(
        username=config.username, password=passwords_dict.get("postgres_password")
    ).get_data_from_postgres(query=query, engine=engine)

    # Sub funcs
    def push_groupby_dataframe(
        df: pd.DataFrame,
        username: str,
        password: str,
    ):
        """
        Pushes the groupby dataframe to Heroku Postgres.

        Parameters
        ----------
        df: pd.DataFrame, the pandas dataframe input.
        username: str, the username.
        password: str, the password.

        Returns
        -------
        A refreshed Heroku Postgres.

        """
        logging.info("Grouping by the data")
        df_groupby = dashboard_connector.DashboardGraphs.group_by_data(
            df=df, group_by_col="location"
        )

        # Remove the continents
        logging.info("Removing the continents")
        df_groupby = df_groupby[~df_groupby["location"].isin(config.continents)]

        # Make a Heroku Postgres engine
        logging.info("Making a Heroku Postgres engine")
        heroku_engine = dashboard_connector.Postgres(
            username=config.username, password=passwords_dict.get("postgres_password")
        ).create_engine(engine_url=passwords_dict.get("heroku_postgres_uri"))

        # Push to the Heroku table
        logging.info(
            f"Pushing the groupby dataframe to Heroku Postgres : {config.heroku_table_name}"
        )
        dashboard_connector.Postgres(
            username=username, password=password
        ).push_to_postgres(
            df=df_groupby,
            table_name=config.heroku_table_name,
            engine=heroku_engine,
            job_type="replace",
        )

    def push_time_series_dataframe(
        df: pd.DataFrame,
        username: str,
        password: str,
        heroku_table_name: str,
        value_col: str,
    ):
        """
        Pushes the time series dataframe to Heroku Postgres.

        Parameters
        ----------
        df" pd.DataFrame, the pandas dataframe input.
        username: str, the username.
        password: str, the password.
        heroku_table_name: str, the name of the table in Heroku Postgres.
        value_col: str, the name of the value col.

        Returns
        -------
        A refreshed Postgres Heroku database.

        """
        logging.info(f"Pivoting the dataframe by {value_col}")
        df_pivot = dashboard_connector.Postgres.pivot_dataframe(
            df=df, value_col=value_col, index_col="date", column="location"
        )

        logging.info("Removing unwanted characters in columns")
        cols = df_pivot.columns.tolist()
        cols = [i.replace("(", "").replace(")", "") for i in cols]
        df_pivot.columns = cols

        # Make a Heroku Postgres engine
        logging.info("Making a Heroku Postgres engine")
        heroku_engine = dashboard_connector.Postgres(
            username=config.username, password=passwords_dict.get("postgres_password")
        ).create_engine(engine_url=passwords_dict.get("heroku_postgres_uri"))

        # Push to the Heroku table
        logging.info(f"Pushing to a Heroku Postgres table: {heroku_table_name}")
        dashboard_connector.Postgres(
            username=username, password=password
        ).push_to_postgres(
            df=df_pivot,
            table_name=heroku_table_name,
            engine=heroku_engine,
            job_type="replace",
        )

    # Initiate sub funcs
    push_groupby_dataframe(df, username, password)
    push_time_series_dataframe(
        df, username, password, config.heroku_table_new_cases, value_col="new_cases"
    )
    push_time_series_dataframe(
        df, username, password, config.heroku_table_new_deaths, value_col="new_deaths"
    )


# Creating the DAG
# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="covid_data_dag",
    default_args=default_args,
    description="DAG to update Covid 19 data daily to push to a Postgres database.",
    schedule_interval="30 9 * * *",
    start_date=datetime(2021, 8, 24),
) as dag:

    # Initiate tasks
    task_1 = DummyOperator(task_id="Initiate_DAG")

    task_2 = PythonOperator(
        task_id="dashboard_update",
        python_callable=covid_19_dashboard_update,
        op_kwargs={
            "username": config.username,
            "password": passwords_dict.get("postgres_password"),
            "database": config.database,
            "table_name": config.table_name
        },
    )

    task_3 = PythonOperator(
        task_id="push_local_Postgres_to_Heroku",
        python_callable=local_to_heroku_postgres,
        op_kwargs={
            "username": config.username,
            "password": passwords_dict.get("postgres_password"),
            "local_database": config.database,
            "local_table_name": config.table_name,
        },
    )

    task_1 >> [task_2, task_3]
