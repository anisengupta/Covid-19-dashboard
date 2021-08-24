# Initial Config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
from datetime import datetime, timedelta
import pandas as pd
import sys
import importlib

# Import the dashboard connector code
try:
    from utils import dashboard_connector
except:
    code_root = '/Users/aniruddha.sengupta/PycharmProjects/Covid-19-dashboard/utils'
    if code_root not in sys.path:
        sys.path.insert(0, code_root)

    module = 'dashboard_connector'
    dashboard_connector = importlib.import_module(module)

# Import the config
try:
    from utils import config
except:
    module = 'config'
    config = importlib.import_module(module)


def covid_19_dashboard_update(
    username: str,
    password: str,
    database: str,
    table_name: str,
    columns: list,
    geo_ids_url: str,
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
    columns: list, the list of columns to be used in the filtering.
    geo_ids_url: str, the geo ids url to parse through.

    Returns
    -------
    A refreshed Postgres database.

    """
    # Initiate the logger
    logger_filepath = '/Users/aniruddha.sengupta/Desktop/Covid-19 dashboard/logs'
    dashboard_connector.Logging.create_logging_config(filepath=logger_filepath)

    # Starting the logging
    logging.info("The Covid-19 Google Data Studio dashboard process has now started.")

    # Specify the url path to get data from
    logging.info("Specifying the url path to get data from")
    url_path = "https://covid.ourworldindata.org/data/owid-covid-data.json"
    logging.info(url_path)

    # Retrieve the data and make dictionary keys
    logging.info("Retrieving the data and make dictionary keys")
    data = dashboard_connector.GetData.get_json_data(url_path=url_path)
    dict_keys = dashboard_connector.GetData.make_dict_keys(data=data)
    logging.info(dict_keys)

    # Specify the columns to keep
    logging.info("Specifying the columns to keep")
    logging.info(columns)

    # Making the geo ids dict
    geo_id_df = dashboard_connector.GetData.make_country_codes_dataframe(
        url=geo_ids_url
    )
    geo_ids_dict = dashboard_connector.GetData.make_geo_ids_dict(geo_id_df=geo_id_df)

    # Make the pandas dataframe
    logging.info("Making the pandas dataframe")
    df = dashboard_connector.GetData.dataframe_all_countries(
        data=data, dict_keys=dict_keys, columns=columns, geo_ids_dict=geo_ids_dict
    )

    # Making the date column into datetime
    logging.info("Making the date column into datetime")
    df["date"] = pd.to_datetime(df["date"])

    # Sorting the dataframe by date and country code
    logging.info("Sorting the dataframe by date and country location")
    df = df.sort_values(by=["location", "date"])

    # Specify the config to connect to Postgres
    logging.info("Specifying the config to connect to Postgres")
    logging.info(username, password, database, table_name)

    # Construct the engine url
    logging.info("Constructing the engine url")
    engine_url = dashboard_connector.Postgres(
        username=username, password=password
    ).construct_engine_url(database=database)
    logging.info(engine_url)

    # Initiate the connection
    logging.info("Initiating the connection")
    engine = dashboard_connector.Postgres(
        username=username, password=password
    ).create_engine(engine_url=engine_url)

    # Push the dataframe to Postgres
    logging.info("Pushing the dataframe to Postgres")
    dashboard_connector.Postgres(username=username, password=password).push_to_postgres(
        df=df, table_name=table_name, engine=engine, job_type="replace"
    )

    # Saving the dataframe to a GCP bucket
    bucket_name = config.bucket
    dashboard_connector.GCP.set_credentials(credentials_path=config.credentials_path)
    logging.info(f'Saving the dataframe to a GCP bucket - {bucket_name}')
    dashboard_connector.GCP.upload_dataframe_to_gcp_bucket(
        df=df, bucket_name=bucket_name, file_name='covid_19_df.pq', file_type='pq'
    )

    # Logging the end
    logging.info("The process has now ended")


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
    dag_id="main_dag",
    default_args=default_args,
    description="DAG to update Covid 19 data daily to push to a Postgres database.",
    schedule_interval='30 9 * * *',
    start_date=datetime(2021, 8, 24),
) as dag:

    # Initiate tasks
    task_1 = DummyOperator(task_id="Initiate_DAG")

    task_2 = PythonOperator(
        task_id="dashboard_update",
        python_callable=covid_19_dashboard_update,
        op_kwargs={
            "username": config.username,
            "password": config.password,
            "database": config.database,
            "table_name": config.table_name,
            "columns": config.columns,
            "geo_ids_url": config.geo_ids_url,
        },
    )

    task_1 >> task_2
