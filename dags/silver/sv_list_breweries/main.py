import time
import requests
import shutil, os

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from airflow.models.baseoperator import BaseOperator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit, col, trim, when, regexp_replace, initcap, concat_ws


# Initialize Spark session with Delta Lake configuration
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


class SilverListBreweriesOp(BaseOperator):
    def __init__(self, date_parameter, **kwargs) -> None:
        super().__init__(**kwargs)
       

    def execute(self, context):
        # Create Spark session for Airflow operator execution
        spark = (
            SparkSession.builder
            .appName("BreweriesIngestion")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .getOrCreate()
        )
                
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        
        # Define data paths
        BRONZE_PATH = "/opt/airflow/dags/bronze/storage/list_breweries/*/*/*/*.parquet"
        SILVER_PATH = "/opt/airflow/dags/silver/storage/list_breweries"

        def cast_dataframe_with_schema(df: DataFrame, schema: StructType) -> DataFrame:
            """
            Casts a DataFrame to the given schema.
            Trims strings and replaces empty strings with NULL.
            
            Parameters:
                df (DataFrame): Input PySpark DataFrame
                schema (StructType): Desired schema
            
            Returns:
                DataFrame: Casted DataFrame
            """
            for field in schema.fields:
                if isinstance(field.dataType, StringType):
                    df = df.withColumn(
                        field.name,
                        when(trim(col(field.name)) == "", None)
                        .otherwise(trim(col(field.name)))
                    )
                else:
                    df = df.withColumn(field.name, col(field.name).cast(field.dataType))
            return df


        def _schema():
            # Define Silver layer schema
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

        # Read raw data from Bronze layer
        df = spark.read.parquet(BRONZE_PATH)

        # Remove duplicate records
        df_distinct = df.dropDuplicates(["id"])

        # Apply schema casting and cleaning
        df_cast = cast_dataframe_with_schema(df_distinct, _schema())

        # Apply data transformations
        df_transform = df_cast.withColumn("full_address", concat_ws(", ", "address_1", "address_2", "address_3"))\
                              .withColumn("website_url",regexp_replace(col("website_url"), "/$", ""))\
                              .withColumn("has_valid_coordinates", when((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)), True).otherwise(False))\
                              .withColumn("phone", regexp_replace(col("phone"), r"\D", ""))\
                              .withColumn("postal_code", regexp_replace(col("postal_code"), r"\D", ""))\
                              .withColumn("brewery_type", initcap(col("brewery_type")))

        # Remove unnecessary columns
        df_clean = df_transform.drop("address_1", "address_2", "address_3", "state", "street")

        # Reorder columns for better organization
        df_reorder = df_clean.select(
            "id",
            "name",
            "brewery_type",
            "full_address",
            "city",
            "state_province",
            "country",
            "postal_code",    
            "longitude",
            "latitude",
            "has_valid_coordinates",
            "phone",
            "website_url"
        )

        # Write transformed data to Silver layer as Delta table
        df_reorder.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .option("overwriteSchema", "true") \
            .save(SILVER_PATH)