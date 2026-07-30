import io
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import google.auth
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = "project-7cfdad94-4b3b-452c-8da"
BUCKET_NAME = "datalake_cap2"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def get_retry_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def extract_weather_stream_to_gcs(project_id, bucket_name, execution_date, **kwargs):
    logging.info(f"🌤️ [TASK 1] Bắt đầu cào thời tiết cho ngày: {execution_date}")
    
    LAT, LON = 16.0678, 108.2208
    exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")
    session = get_retry_session()
    
    try:
        archive_end = (exec_dt - timedelta(days=3)).strftime("%Y-%m-%d")
        url_archive = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={LAT}&longitude={LON}&start_date=2020-01-01&end_date={archive_end}"
            f"&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
            f"&timezone=Asia%2FBangkok"
        )
        res_arch = session.get(url_archive, timeout=30).json()
        df_archive = pd.DataFrame({
            'date': res_arch['daily']['time'],
            'temp_mean': res_arch['daily']['temperature_2m_mean'],
            'temp_max': res_arch['daily']['temperature_2m_max'],
            'temp_min': res_arch['daily']['temperature_2m_min'],
            'rainfall_mm': res_arch['daily']['precipitation_sum'],
            'wind_speed_max': res_arch['daily']['wind_speed_10m_max'],
            'source': 'archive_api'
        })

        url_live = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={LAT}&longitude={LON}&past_days=2&forecast_days=1"
            f"&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
            f"&timezone=Asia%2FBangkok"
        )
        res_live = session.get(url_live, timeout=30).json()
        df_live = pd.DataFrame({
            'date': res_live['daily']['time'],
            'temp_mean': res_live['daily']['temperature_2m_mean'],
            'temp_max': res_live['daily']['temperature_2m_max'],
            'temp_min': res_live['daily']['temperature_2m_min'],
            'rainfall_mm': res_live['daily']['precipitation_sum'],
            'wind_speed_max': res_live['daily']['wind_speed_10m_max'],
            'source': 'live_forecast_api'
        })

        df_total = pd.concat([df_archive, df_live], ignore_index=True)
        df_total['date'] = pd.to_datetime(df_total['date'])
        df_total = df_total.drop_duplicates(subset=['date'], keep='last').sort_values('date')

        csv_buffer = io.StringIO()
        df_total.to_csv(csv_buffer, index=False, encoding='utf-8')
        
        date_folder = execution_date.replace("-", "/")
        gcs_dest_path = f"raw/weather/{date_folder}/data.csv"
        
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_dest_path,
            data=csv_buffer.getvalue(),
            mime_type='text/csv'
        )
        logging.info(f"✅ [SUCCESS] Đã stream thành công {len(df_total)} dòng qua GCS!")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ [API ERROR] Lỗi kết nối API sau nhiều lần thử: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"❌ [CRITICAL] Lỗi hệ thống: {str(e)}")
        raise

def load_weather_gcs_to_bq(project_id, bucket_name, execution_date, **kwargs):
    logging.info(f"📊 [TASK 2] Bắt đầu nạp BigQuery cho ngày: {execution_date}")
    
    credentials, _ = google.auth.default()
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    
    table_id = f"{project_id}.bronze_raw.weather"
    date_folder = execution_date.replace("-", "/")
    gcs_uri = f"gs://{bucket_name}/raw/weather/{date_folder}/data.csv"
    
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("temp_mean", "FLOAT"),
        bigquery.SchemaField("temp_max", "FLOAT"),
        bigquery.SchemaField("temp_min", "FLOAT"),
        bigquery.SchemaField("rainfall_mm", "FLOAT"),
        bigquery.SchemaField("wind_speed_max", "FLOAT"),
        bigquery.SchemaField("source", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True
    )
    
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    logging.info(f"🎉 [SUCCESS] Đã nạp thành công dữ liệu vào bảng: {table_id}")

default_args = {
    'owner': 'ThanhToan',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='weather_pipeline_v4',
    default_args=default_args,
    description='Pipeline thời tiết Đà Nẵng (Có cào bù quá khứ)',
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 7, 7),
    catchup=True, 
    max_active_runs=1,
    tags=['digital_twin', 'weather', 'bronze']
) as dag:
    
    t1_extract = PythonOperator(
        task_id='extract_weather_to_gcs',
        python_callable=extract_weather_stream_to_gcs,
        op_kwargs={
            'project_id': PROJECT_ID,
            'bucket_name': BUCKET_NAME,
            'execution_date': '{{ ds }}'
        },
        execution_timeout=timedelta(minutes=15)
    )

    t2_load = PythonOperator(
        task_id='load_weather_to_bq_bronze',
        python_callable=load_weather_gcs_to_bq,
        op_kwargs={
            'project_id': PROJECT_ID,
            'bucket_name': BUCKET_NAME,
            'execution_date': '{{ ds }}'
        },
        execution_timeout=timedelta(minutes=15)
    )

    t3_run_dbt = BashOperator(
        task_id='run_dbt_weather_model',
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt run --select stg_weather_cleansed --profiles-dir . --threads 1 --quiet"
        ),
        execution_timeout=timedelta(minutes=15)
    )
    
    t4_test_dbt = BashOperator(
        task_id="test_dbt_weather_model",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt test --profiles-dir . --select stg_weather_cleansed --threads 1 --quiet"
        )
    )

    t1_extract >> t2_load >> t3_run_dbt >> t4_test_dbt