# Tên tệp: 02_statistic_seed_loader.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0, # Không cần retry nhiều vì dữ liệu tĩnh
}

# 2. Khởi tạo DAG cho luồng dữ liệu thống kê tĩnh
with DAG(
    dag_id='statistic_reference_loader_v1',
    default_args=default_args,
    description='Pipeline chạy dbt seed để nạp dữ liệu tĩnh Statistic',
    schedule_interval=None, # Tắt chạy tự động, chỉ chạy khi bấm nút thủ công
    start_date=datetime(2026, 7, 19),
    catchup=False,
    tags=['statistic', 'seed', 'manual'],
) as dag:

    # 3. Các tác vụ (Tasks)
    
    # Task 3.1: Chạy lệnh dbt seed để đẩy tệp CSV từ thư mục dbt seeds lên BigQuery (Bronze)
    run_dbt_seed_statistic = BashOperator(
        task_id="run_dbt_seed_statistic",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            "dbt seed --select tourism_stats --profiles-dir . --quiet"
        ),
        env={"DBT_MAX_MEMORY": "512M"}
    )
    
    # Task 3.2: Chạy dbt model để chuyển dữ liệu statistic vừa nạp sang lớp tinh gọn (Silver)
    run_dbt_model_statistic = BashOperator(
        task_id="run_dbt_model_statistic",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            # CẬP NHẬT: Đổi tên model ở phần --select thành stg_tourism_stats_cleansed
            "dbt run --select stg_tourism_stats_cleansed --profiles-dir . --quiet"
        ),
        env={"DBT_MAX_MEMORY": "512M"}
    )

    # 4. Thiết lập thứ tự chạy
    run_dbt_seed_statistic >> run_dbt_model_statistic