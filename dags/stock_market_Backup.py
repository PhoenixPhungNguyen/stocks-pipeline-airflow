#THIS PIPELINE:
#1.Check if the API is available
#2.Call the API to get stock prices (e.g., AAPL)
#3.Store the raw data into a storage system
#4.Process and format the data using Spark (inside a Docker container)
#5.Retrieve the processed CSV file
#6.Load the data into a Data Warehouse

from datetime import datetime
import requests

#define dag and task
from airflow.decorators import dag, task
#get connection information from Airflow Connections
from airflow.hooks.base import BaseHook
#run Python function
from airflow.operators.python import PythonOperator
#run job in Docket container(run Spark)
from airflow.providers.docker.operators.docker import DockerOperator
#send notification to Slack when executes successfully/fail
from airflow.providers.slack.notifications.slack import SlackNotifier
#return result of 1 sensor task
from airflow.sensors.base import PokeReturnValue

from include.scripts.stock_market.tasks import (
    # BUCKET_NAME,
    #get CSV file proccessed from Spark
    _get_formatted_csv,
    #call API to get Stock data
    _get_stock_prices,
    #load data into data warehouse
    _load_to_dw,
    #save data after getting
    _store_prices,
)

SYMBOL = "AAPL"
# MSFT, GOOGL, AMZN, TSLA, META, NFLX, NVDA, AMD, INTC

# default_args = {
#     "owner": "hai",
# }

#Define DAG by @dag
@dag(
    start_date=datetime(2024, 1, 1), #start date to run Dag
    schedule="@daily",              #run daily
    catchup=False,                  #not to run task of missed date
    tags=["stock_market"],          #notify after running
    on_success_callback=SlackNotifier(
        slack_conn_id="slack",
        text="Stock market DAG completed successfully",
        channel="#stocks-pipeline",
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id="slack",
        text="Stock market DAG failed",
        channel="#stocks-pipeline",
    ),
    # default_args=default_args,
)
def stock_market():
    #check whether the data API is working or not.
    #If available, it returns the URL (stored in XCom for other tasks to use).
    #poke_interval=30: checks every 30 seconds.
    #timeout=300: if there's no result after 5 minutes, the task stops.
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    #call get_stock_prices function to get data from API
    #use XCom to pass the URL from the sensor task
    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={
            "url": "{{ ti.xcom_pull(task_ids='is_api_available') }}",
            "symbol": SYMBOL,
        },
    )
    #save data into system (file/blob storage)
    #data getting from result of previous task (get_stock_prices)
    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={"stock": "{{ ti.xcom_pull(task_ids='get_stock_prices') }}"},
    )
    #Docker task
    #run Spark app in Docker to process data
    #Retrieve the data path from the store_prices task via XCom.
    #Use a Docker container named spark-app.
    format_prices = DockerOperator(
        task_id="format_prices",
        image="spark-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": "{{ ti.xcom_pull(task_ids='store_prices') }}"
        },
    )
    #get result CSV file from Spark
    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={
            "stock_folder_path": "{{ ti.xcom_pull(task_ids='store_prices') }}",
        },
    )
    #load processed CSV file into Datawarehouse
    load_to_dw = PythonOperator(
        task_id="load_to_dw",
        python_callable=_load_to_dw,
        op_kwargs={
            "csv_path": "{{ ti.xcom_pull(task_ids='get_formatted_csv') }}",
        },
    )
    #Flow between tasks in order
    #check API --> call API --> save Data --> process -->get CSV -->load into datawarehouse
    (
        is_api_available()
        >> get_stock_prices
        >> store_prices
        >> format_prices
        >> get_formatted_csv
        >> load_to_dw
    )


stock_market()
