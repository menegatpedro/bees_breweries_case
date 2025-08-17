import requests
import time
import shutil, os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


spark = (
    SparkSession.builder
    .appName("BreweriesIngestion")
    .master("local[2]")  # limit Spark to 2 cores
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")  # shrink shuffle overhead
    .config("spark.sql.files.maxPartitionBytes", "64m")  # prevent tiny-file problem
    .getOrCreate()
)

# API
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200
SLEEP_BETWEEN_CALLS = 2

# Paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUTPUT_BASE = os.path.join(PROJECT_ROOT, "1-bronze", "storage", "list_breweries")

ingest_date = datetime.now()
year = ingest_date.strftime("%Y")
month = ingest_date.strftime("%Y%m")
day = ingest_date.strftime("%Y%m%d")
OUTPUT_PATH = os.path.join(OUTPUT_BASE, year, month, day)
os.makedirs(OUTPUT_PATH, exist_ok=True)


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


# Fetch page function
def fetch_page(page):
    print(f"    > Requesting page {page}.")
    for attempt in range(5):
        try:
            resp = requests.get(BASE_URL, params={"page": page, "per_page": PER_PAGE}, timeout=10)
            if resp.status_code >= 500:
                raise requests.HTTPError(f"Server error {resp.status_code}")
            resp.raise_for_status()
            print("    > Request successful!")
            return resp.json()
        except (requests.RequestException, requests.exceptions.SSLError) as e:
            print(f"Error page {page}: {e}, retry {attempt+1}")
            time.sleep(2 ** attempt + 5)
    print(f"Skipping page {page} after multiple retries")
    return []


# Page-by-page ingestion
page = 1
while True:
    print(f"Starting page number {page}")
    data = fetch_page(page)
    if not data:
        break  # no more data

    # Convert to Spark DataFrame and write immediately
    print(f"    > Creating dataframe for page {page}!")
    df = spark.createDataFrame(data, schema = _schema())
    print(f"    > Writing parquet files for {page}!")
    df.write.mode("append").parquet(OUTPUT_PATH)
    df.unpersist()
    print(f"    > Page {page} ingested successfully!")
    page += 1
    time.sleep(SLEEP_BETWEEN_CALLS)

print("All data ingested successfully!")

df = spark.read.parquet(OUTPUT_PATH)
df.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "_tmp")


shutil.rmtree(OUTPUT_PATH)
os.rename(OUTPUT_PATH + "_tmp", OUTPUT_PATH)

print("Files compressed.")

"""
Next steps:
    - Decrease amount of writes.
    - 
"""