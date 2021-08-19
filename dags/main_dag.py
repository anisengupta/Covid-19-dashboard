# Initial Config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from utils import dashboard_connector
import logging
from datetime import datetime, timedelta


def covid_19_dashboard_update(
    username: str, password: str, database: str, table_name: str, columns: list
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

    Returns
    -------
    A refreshed Google Data Studio dashboard.

    """
    # Starting the logger
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

    # Make the pandas dataframe
    logging.info("Making the pandas dataframe")
    df = dashboard_connector.GetData.dataframe_all_countries(
        data=data, dict_keys=dict_keys, columns=columns
    )

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
        df=df, table_name=table_name, engine=engine
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
    description="DAG to update Covid 19 data daily to push to a GDS dashboard.",
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
) as dag:

    # Initiate tasks
    task_1 = DummyOperator(task_id="Initiate DAG")

    task_2 = PythonOperator(
        task_id="dashboard_update",
        python_callable=covid_19_dashboard_update,
        op_kwargs={
            "username": "postgres",
            "password": "Uchihazenzo1994",
            "database": "covid_19",
            "table_name": "covid_19_data",
            "columns": ["date", "location", "total_cases", "total_deaths"],
        },
    )

    task_1 >> task_2
