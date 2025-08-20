from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from bronze.bz_list_breweries.main import BronzeListBreweriesOp
from silver.sv_list_breweries.main import SilverListBreweriesOp
from gold.gd_list_breweries.main import GoldListBreweriesOp

# Default arguments for the DAG
default_args = {
    "owner": "bees",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 16),
    "retries": 3,
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(hours=2),
    "email": ['pedromenegat@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": False
}

# Define the DAG
with DAG(
    "list_breweries_daily",
    default_args=default_args,
    description="Dag designed to get and transform data from Breweries.",
    catchup=False,
    schedule_interval="0 3 * * *",  # Runs daily at 3 AM
    dagrun_timeout=timedelta(minutes=10),
    tags=["List Breweries"],
) as dag:

    # Bronze layer task - data ingestion
    bronze_ingestion = BronzeListBreweriesOp(
        task_id="bronze_ingestion", date_parameter=datetime.now().date()
    )

    # Silver layer task - data processing and cleaning
    silver_processing = SilverListBreweriesOp(
        task_id="silver_processing", date_parameter=datetime.now().date()
    )

    # Gold layer task - data aggregation and analysis
    gold_aggregation = GoldListBreweriesOp(
        task_id="gold_aggregation", date_parameter=datetime.now().date()
    )

    # Define task dependencies
    bronze_ingestion >> silver_processing >> gold_aggregation