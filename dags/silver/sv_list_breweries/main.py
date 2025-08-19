import time
import requests
import shutil, os

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from airflow.models.baseoperator import BaseOperator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit, col, trim, when, regexp_replace, initcap, concat_ws


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

        df = spark.read.parquet(BRONZE_PATH)

        df_distinct = df.dropDuplicates(["id"])

        df_cast = cast_dataframe_with_schema(df_distinct, _schema())

        df_transform = df_cast.withColumn("full_address", concat_ws(", ", "address_1", "address_2", "address_3"))\
                              .withColumn("website_url",regexp_replace(col("website_url"), "/$", ""))\
                              .withColumn("has_valid_coordinates", when((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)), True).otherwise(False))\
                              .withColumn("phone", regexp_replace(col("phone"), r"\D", ""))\
                              .withColumn("postal_code", regexp_replace(col("postal_code"), r"\D", ""))\
                              .withColumn("brewery_type", initcap(col("brewery_type")))

        df_clean = df_transform.drop("address_1", "address_2", "address_3", "state", "street")

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

        df_reorder.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .option("overwriteSchema", "true") \
            .save(SILVER_PATH)


        # Method 1: as a DataFrame
        #df = spark.read.format("delta").load(SILVER_PATH)

        # Show the first 20 rows
        #df.show(truncate=False)

        # Optional: see distinct states
        #df.select("state_province").distinct().show()



        # # Save a single CSV in the same folder as this script
        # script_dir = os.path.dirname(os.path.abspath(__file__))
        # tmp_dir = os.path.join(script_dir, "sample_csv_tmp")
        # final_csv = os.path.join(script_dir, "sample.csv")

        # # Write as a single-part CSV folder
        # df_reorder.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)

        # # Move the part file to sample.csv and clean up the tmp folder
        # part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        # if not part_files:
        #     raise RuntimeError("CSV write produced no part files.")
        # shutil.move(part_files[0], final_csv)
        # shutil.rmtree(tmp_dir)

        # print(f"Saved CSV → {final_csv}")