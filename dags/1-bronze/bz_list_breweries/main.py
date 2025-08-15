import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


spark = SparkSession.builder \
    .appName("bees_app") \
    .master("local[*]") \
    .getOrCreate()


def get_breweries():      
    list_url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(list_url)
    breweries_json = response.json()

    return breweries_json

def _schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ])


df = spark.createDataFrame(get_breweries(), schema=_schema())

df.show(truncate=False)