# BEES Data Engineering – Breweries Case  

This repository demonstrates a **local data engineering pipeline** built with **Apache Airflow (Dockerized)** and **PySpark**, following the **Medallion Architecture** (Bronze → Silver → Gold).  

The pipeline ingests data from the [Open Brewery DB](https://www.openbrewerydb.org/) API, processes it through successive layers, and produces aggregated views for analysis.  

---

## Pipeline Overview  

- **Orchestration:** Apache Airflow (DAG inside Docker container)  
- **Processing:** Spark with PySpark  
- **Storage:** Local folders acting as a “datalake” (partitioned parquet and Delta tables)  
- **Monitoring:** Automatic email alerts on pipeline failure  

---

## Bronze Layer – Ingestion  

- Fetches data from the Open Brewery DB **List Breweries** endpoint.  
- Saves raw data as **Parquet files** with **Hive-style partitioning** by ingestion date.  
- Example file path:  
bees_breweries_case/dags/bronze/storage/list_breweries/2025/202508/20250819/
part-00000-49f794b6-846d-49b8-af07-b6b8a69810b0-c000.snappy.parquet


---

## Silver Layer – Transformation  

Reads Bronze data and applies structured transformations:  
1. Drops duplicates.  
2. Enforces schema consistency.  
3. Creates a `full_address` column (concatenating address_1, address_2, address_3).  
4. Removes trailing `/` from `website_url`.  
5. Validates coordinates → stores result in `has_valid_coordinates`.  
6. Cleans `phone` and `postal code` (removes spaces, dashes, symbols).  
7. Standardizes `brewery_type` with `INITCAP`.  
8. Drops unnecessary columns: `address_1`, `address_2`, `address_3`, `state`, `street`.  

Output is stored as a **Delta table**, partitioned by `country`.  

---

## Gold Layer – Aggregation  

- Reads Silver Delta tables.  
- Builds a temp view with **brewery counts grouped by type and country**.  

---

## Orchestration Details  

- **Schedule:** Runs daily at 03:00 AM  
- **Retries:** 3 attempts per task  
- **Timeout:** 2 hours  
- **Failure Handling:** Sends **email notification** to configured recipients  
- **DAG Tasks:**  
- `bronze_ingestion`  
- `silver_processing`  
- `gold_aggregation`

---

## Architecture
