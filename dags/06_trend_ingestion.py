"""
===========================================================
GOOGLE TRENDS ETL PIPELINE
===========================================================

Pipeline

1. Download historical CSV & checkpoint from GCS

2. Crawl Google Trends

3. Merge with historical data

4. Update checkpoint

5. Upload updated CSV & checkpoint back to GCS

6. Load google_trends_long.csv
   from GCS -> BigQuery Bronze

Schedule

Daily at 00:00
"""

# ============================================================
# IMPORT LIBRARIES
# ============================================================

from datetime import datetime
from datetime import timedelta

import logging

import google.auth
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator

from crawl_trends_pipeline import crawl_trends_pipeline
from airflow.operators.bash import BashOperator

# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_ID = "project-7cfdad94-4b3b-452c-8da"

BUCKET_NAME = "datalake_cap2"

DATASET_ID = "bronze_raw"

TABLE_NAME = "google_trends"

TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"


# ============================================================
# LOAD GCS -> BIGQUERY
# ============================================================
def load_trends_to_bronze(**context):
    logging.info("=" * 60)
    logging.info("LOAD GOOGLE TRENDS TO BRONZE")
    logging.info("=" * 60)

    credentials, _ = google.auth.default()
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    gcs_uri = f"gs://{BUCKET_NAME}/raw/trends/google_trends_long.csv"

    schema = [
        bigquery.SchemaField("search_date", "DATE"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("search_interest", "INTEGER"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("crawl_time", "TIMESTAMP")
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        ignore_unknown_values=True,
        autodetect=False
    )

    logging.info(f"Loading: {gcs_uri}")
    logging.info(f"Destination: {TABLE_ID}")

    load_job = client.load_table_from_uri(gcs_uri, TABLE_ID, job_config=job_config)
    load_job.result()

    table = client.get_table(TABLE_ID)
    logging.info(f"Loaded {table.num_rows} rows.")
    logging.info("GOOGLE TRENDS BRONZE COMPLETED")

# ============================================================
# DEFAULT ARGUMENTS
# ============================================================
default_args = {
    "owner": "Thanh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10)
}

# ============================================================
# CREATE DAG
# ============================================================
with DAG(
    dag_id="google_trends_pipeline_v5", 
    description="Google Trends Incremental ETL Pipeline (Ultimate Optimized)",
    default_args=default_args,
    start_date=datetime(2026, 7, 5),
    schedule="0 0 * * *",
    catchup=False, 
    max_active_runs=1,
    max_active_tasks=1, 
    tags=["google_trends", "tourism", "etl", "bronze"]
) as dag:

    crawl_trends_task = PythonOperator(
        task_id="crawl_google_trends",
        python_callable=crawl_trends_pipeline,
        execution_timeout=timedelta(minutes=90) 
    )

    load_bronze_task = PythonOperator(
        task_id="load_google_trends_to_bronze",
        python_callable=load_trends_to_bronze,
        execution_timeout=timedelta(minutes=15)
    )

    dbt_run_trend = BashOperator(
        task_id="dbt_run_trend",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "export DBT_SEND_ANONYMOUS_USAGE_STATS=False && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt run --project-dir . --profiles-dir . --select stg_trend_cleansed --full-refresh --no-partial-parse --threads 1 --quiet"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    dbt_test_trend = BashOperator(
        task_id="dbt_test_trend",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "export DBT_SEND_ANONYMOUS_USAGE_STATS=False && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            # ÄÃ£ thay Ä‘á»•i dÃ²ng nÃ y:
            "dbt test --project-dir . --profiles-dir . --select stg_trend_cleansed --no-partial-parse --threads 1"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    crawl_trends_task >> load_bronze_task >> dbt_run_trend >> dbt_test_trend
