#Use Pyspark to transform stock data from JSON and save result in CSV file
#into MinIO.

#python library to get env variable
import os
import sys

from pyspark import SparkContext
# Import the SparkSession module to work with Spark SQL and DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, explode, from_unixtime
from pyspark.sql.types import DateType

if __name__ == "__main__":

    def app():
        # Create a SparkSession and config connection to MinIO
        spark = (
            #spark app name "FormatStock"
            SparkSession.builder.appName("FormatStock")
            #config so that Spark can connect to MinIO (storage service like S3)
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"))
            .config(
                "fs.s3a.endpoint", 
                #endpoint: address of MinIO Server
                os.getenv("ENDPOINT", "http://host.docker.internal:9000"),
            )
            .config("fs.s3a.connection.ssl.enabled", "false")
            .config("fs.s3a.path.style.access", "true")
            .config("fs.s3a.attempts.maximum", "1")
            .config("fs.s3a.connection.establish.timeout", "5000")
            .config("fs.s3a.connection.timeout", "10000")
            .getOrCreate()
        )

        # Read a JSON file from an MinIO bucket using the access key, secret key,
        # and endpoint configured above
        df = spark.read.option("header", "false").json(
            f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/prices.json"
        )

        #Transform Data
        # Step1: Explode the necessary arrays
        #expode: if colum is array, split each item into each row
        df_exploded = df.select(
            "timestamp", explode("indicators.quote").alias("quote")
        ).select("timestamp", "quote.*")

        # Zip the arrays
        #Step2: Merge arrays for each timestamp, close,high....
        df_zipped = df_exploded.select(
            arrays_zip("timestamp", "close", "high", "low", "open", "volume").alias(
                "zipped"
            )
        )
        #Step3: Slit merge array into each row and select neccessary columns
        df_zipped = df_zipped.select(explode("zipped")).select(
            "col.timestamp",
            "col.close",
            "col.high",
            "col.low",
            "col.open",
            "col.volume",
        )
        #Step4: convert timestamp into date in Spark
        df_zipped = df_zipped.withColumn(
            "date", from_unixtime("timestamp").cast(DateType())
        )

        #Step5: Store result into CSV in Minio
        #overide if old data, add header, slit column into , and output to CSV file
        df_zipped.write.mode("overwrite").option("header", "true").option(
            "delimiter", ","
        ).csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted_prices")
    #end app by killing process.
    app()
    os.system("kill %d" % os.getpid())
