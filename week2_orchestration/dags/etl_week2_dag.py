"""
Week 2 ETL Orchestration DAG
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ================================
# METADATA FUNCTION
# ================================
def log_metadata(stage, rows, status):
    print({
        "stage": stage,
        "rows_processed": rows,
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    })


# ================================
# TASK FUNCTIONS
# ================================

def ingest_clickstream():
    print("Clickstream data ingested")
    log_metadata("ingest_clickstream", 200000, "success")


def ingest_transactions():
    print("Transactions data ingested")
    log_metadata("ingest_transactions", 100000, "success")


def ingest_currency():
    print("Currency data fetched")
    log_metadata("ingest_currency", 10, "success")


def transform_data():
    print("Data transformed")
    log_metadata("transform_data", 100000, "success")


def validate_data():
    print("Starting validation...")

    # Simulated validation checks
    print("✔ No null values found")
    print("✔ Amounts are positive")
    print("✔ Currency codes valid")

    print("Validation successful")
    log_metadata("validation", 100000, "success")


def load_to_gcs():
    print("Data loaded to GCS")
    log_metadata("load_to_gcs", 100000, "success")


# ================================
# DAG CONFIGURATION
# ================================

default_args = {
    "owner": "airflow",
    "retries": 2,
    "email_on_failure": True,
    "email": ["your_email@gmail.com"]  # change to your email
}


# ================================
# DEFINE DAG
# ================================

with DAG(
    dag_id="etl_week2_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    ingest_clickstream_task = PythonOperator(
        task_id="ingest_clickstream",
        python_callable=ingest_clickstream
    )

    ingest_transactions_task = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_transactions
    )

    ingest_currency_task = PythonOperator(
        task_id="ingest_currency",
        python_callable=ingest_currency
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    load_task = PythonOperator(
        task_id="load_to_gcs",
        python_callable=load_to_gcs
    )

    # Pipeline order
    ingest_clickstream_task >> ingest_transactions_task >> ingest_currency_task >> transform_task >> validate_task >> load_task