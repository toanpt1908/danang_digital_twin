from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator

# Cấu hình dự án
PROJECT_ID = "project-7cfdad94-4b3b-452c-8da"
BUCKET_NAME = "datalake_cap2"
TABLE = f"{PROJECT_ID}.bronze_raw.flight"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_cloud_loader_hybrid_v5", 
    description="Pipeline chờ file từ máy tính local và xử lý",
    default_args=default_args,
    schedule_interval="0 5 * * *", 
    start_date=datetime(2026, 7, 19),
    catchup=True,
    max_active_runs=1,
    tags=["flight", "tourism", "etl", "bronze"]
) as dag:

    # 1. SENSOR: Đứng canh cho đến khi file CSV xuất hiện trên GCS
    wait_for_local_csv = GCSObjectExistenceSensor(
        task_id="wait_for_local_csv",
        bucket=BUCKET_NAME,
        # Sử dụng {{ ds }} để lấy ngày chạy DAG tự động thay vì gán cứng ngày
        object="bronze/flight/{{ ds }}/summary.csv", 
        google_cloud_conn_id="google_cloud_default",
        
        mode="poke",                # Giữ tác vụ thức để kiểm tra liên tục
        poke_interval=60,           # Cứ 60 giây (1 phút) sẽ hỏi GCS một lần
        timeout=60 * 30,            # Hết hạn sau 30 phút chờ đợi nếu không có tệp
        retries=2,                  # Thử lại 2 lần nếu có lỗi mạng bất ngờ
        retry_delay=timedelta(minutes=1)
    )

    # 2. Trực tiếp nạp file CSV đó vào BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=BUCKET_NAME,
        # ĐÃ SỬA: Dùng trực tiếp {{ ds }} để đồng bộ với tác vụ Sensor phía trên
        source_objects=["bronze/flight/{{ ds }}/summary.csv"],
        destination_project_dataset_table=TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=True, 
    )

    # 3. Chạy dbt (Đã gỡ bỏ giới hạn RAM 512MB để tối ưu tốc độ)
    run_dbt_flight = BashOperator(
        task_id="run_dbt_flight_model",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "export DBT_SEND_ANONYMOUS_USAGE_STATS=False && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt run --profiles-dir . --select stg_flight_cleansed --threads 1 --quiet"
        ),
        # Thêm cơ chế tự phục hồi chống lỗi nghẽn mạng nội bộ
        retries=3,
        retry_delay=timedelta(minutes=2)
    )
    
    # 4. Kiểm tra dữ liệu dbt (Đã gỡ bỏ giới hạn RAM)
    test_dbt_flight = BashOperator(
        task_id="test_dbt_flight_model",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "export DBT_SEND_ANONYMOUS_USAGE_STATS=False && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt test --profiles-dir . --select stg_flight_cleansed --threads 1 --quiet"
        )
    )

    # Cấu hình luồng thực thi
    wait_for_local_csv >> load_to_bq >> run_dbt_flight >> test_dbt_flight