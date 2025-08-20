import requests
import time
import shutil, os, glob

from delta import *
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from airflow.models.baseoperator import BaseOperator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit, col, trim, when, regexp_replace, initcap, concat_ws, desc


class GoldListBreweriesOp(BaseOperator):
    def __init__(self, date_parameter, **kwargs) -> None:
        super().__init__(**kwargs)
        

    def execute(self, context):

        # Initialize Spark session with Delta Lake support
        spark = (
            SparkSession.builder
            .appName("BreweriesIngestion")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .getOrCreate()
        )

        # Path to Silver layer data
        SILVER_PATH = "/opt/airflow/dags/silver/storage/list_breweries"

        # Read data from Silver layer Delta table
        df = spark.read.format("delta").load(SILVER_PATH)

        # Aggregate breweries count by type and country
        agg_df = df.groupBy("brewery_type", "country").count()

        # Create temporary view for SQL queries
        agg_df.createOrReplaceTempView("vw_breweries_count_per_type_location")
        
        # Display aggregated results ordered by count
        spark.sql("""
            SELECT brewery_type, country, count
            FROM vw_breweries_count_per_type_location
            ORDER BY count DESC
        """).show(truncate=False)