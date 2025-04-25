#call stock API --> Save data into MinIO(object storage)
#look up processed file --> get csv
#load data into PosgreSQL (Data Warehouse)
import json
#process file data in memory
from io import BytesIO, StringIO
#connect PosgreSQL
import psycopg2
#send HHTP request to API
import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
#library to MinIO Object storage
from minio import Minio

#name bucket to save data on MinIO
BUCKET_NAME = "stock-market"

#connect to PosgreSQL(run on Local/Docker)
#host.docker.internal: access local host from Docker container
def _get_postgres_connection():
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="postgres",
        password="postgres",
    )
    return conn

#create client MinIO
def _get_minio_client():
    #get info to connect Minio from Airflow Connection
    minio = BaseHook.get_connection("minio")
    #create client to manipulate with MinIO(upload/download file)
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,   #use HTTP not HTTPS
    )
    return client

#get stock data from API
#concat URL and call API to get stock data of 1 symbol(Example: AAPL)
#return JSON string of stock price in range 1 year
def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?interval=1d&range=1y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson["headers"])
    return json.dumps(response.json()["chart"]["result"][0])

#save raw data into MinIO
def _store_prices(stock):
    client = _get_minio_client()
    #check bucket exists , if it doesn't exist, create bucket
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    #get symbol( example: AAPL) from API data
    symbol = stock["meta"]["symbol"]
    #save JSON data into AAPL/prices.json file in MinIO
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    #return path to use in next step
    return f"{objw.bucket_name}/{symbol}"

#Find CSV file which is processed by Spark
def _get_formatted_csv(stock_folder_path):
    client = _get_minio_client()
    #find .csv file in formatted_prices directory after Spark finished processing
    prefix_name = f"{stock_folder_path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    #raise error to Airflow recognizes that task failed
    raise AirflowNotFoundException("The csv file does not exist in minio")

#Load data into PosgreSQL( data warehouse)
def _load_to_dw(csv_path):
    client = _get_minio_client()
    #connect to PosgreSQL
    conn = _get_postgres_connection()
    cur = conn.cursor()

    response = client.get_object(bucket_name=BUCKET_NAME, object_name=csv_path)
    next(response)  # Skip the first line for the header
    #create schema and dw.stock_market table if it doesn't exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS dw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dw.stock_market (
            "timestamp" bigint,
            close float,
            high float,
            low float,
            open float,
            volume bigint,
            date date
        );
    """)
    #delete old data to prepare load new
    cur.execute("TRUNCATE TABLE dw.stock_market")
    #read CSV data from MinIO
    #use COPY command to import quickly into PosgreSQL
    copy_query = """
        COPY dw.stock_market (timestamp, close, high, low, open, volume, date)
        FROM STDIN
        WITH CSV
        DELIMITER ',';
    """
    cur.copy_expert(copy_query, StringIO(response.read().decode("utf-8")))
    conn.commit()

    cur.close()
    conn.close()
    response.close()
    response.release_conn()
