import requests
import time
import shutil, os, glob
from delta import *
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit, col, trim, when, regexp_replace, initcap, concat_ws, desc


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

SILVER_PATH = "/home/pedromenegat/bees_breweries_case/dags/2-silver/storage/list_breweries"

df = spark.read.format("delta").load(SILVER_PATH)

df.show(truncate=False)


# df.groupBy("country") \
#   .count() \
#   .orderBy(desc("count")) \
#   .show(truncate=False, n=1000)
