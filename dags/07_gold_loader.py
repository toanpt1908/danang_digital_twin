from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="gold_loader_v1",
    default_args=default_args,
    description="Build Gold layer for Danang Tourism Digital Twin",
    schedule="0 1 * * *",
    start_date=datetime(2026, 7, 21),
    catchup=False,
    tags=["gold", "dbt"],
) as dag:

    run_gold_models = BashOperator(
        task_id="run_gold_models",
        bash_command=(
            'export PATH="/opt/python3.11/bin:/home/airflow/.local/bin:$PATH" && '
            'cd /home/airflow/gcs/dags/dbt_project/ && '
            'dbt run --select gold --profiles-dir . --quiet'
        ),
        env={
            "DBT_MAX_MEMORY": "512M"
        }
    )

    run_gold_models