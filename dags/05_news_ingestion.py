# ============================================================
# IMPORT LIBRARIES (NHẬP THƯ VIỆN)
# ============================================================

import io
import logging
from datetime import datetime, timedelta

import pandas as pd
import google.auth
from google.cloud import storage
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from crawl_news import crawl_news
# Thêm thư viện AI của dự án để chấm điểm cảm xúc
from news_sentiment import load_advanced_sentiment_model, analyze_sentiment_batch

# ============================================================
# PROJECT CONFIGURATION (CẤU HÌNH DỰ ÁN)
# ============================================================

PROJECT_ID = "project-7cfdad94-4b3b-452c-8da"
BUCKET_NAME = "datalake_cap2"
BUCKET_FOLDER = "raw/news"
TABLE_ID = f"{PROJECT_ID}.bronze_raw.news"

# ============================================================
# LOGGING (LƯU VẾT HỆ THỐNG)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ============================================================
# LOAD NEWS TO BIGQUERY BRONZE (NẠP DỮ LIỆU VÀO BIGQUERY)
# ============================================================

def load_news_to_bronze(project_id, bucket_name, **kwargs):
    logging.info("=" * 60)
    logging.info("Loading Daily News to BigQuery Bronze with AI...")
    logging.info("=" * 60)

    # Khởi tạo kết nối với các dịch vụ của Google Cloud
    credentials, _ = google.auth.default()
    storage_client = storage.Client(credentials=credentials, project=project_id)
    bucket = storage_client.bucket(bucket_name)
    bq_client = bigquery.Client(credentials=credentials, project=project_id)

    news_files = ["vnexpress_news.csv", "thanhnien_news.csv", "baodanang_news.csv"]
    dataframes = []

    # Danh sách các cột bắt buộc phải có
    required_columns = [
        "article_id", "publish_date", "title", "summary", 
        "url", "source", "source_system", "crawl_time"
    ]

    # --------------------------------------------------------
    # ĐỌC FILE CSV TỪ GOOGLE CLOUD STORAGE
    # --------------------------------------------------------
    for file_name in news_files:
        blob = bucket.blob(f"{BUCKET_FOLDER}/{file_name}")
        if not blob.exists():
            logging.warning(f"{file_name} not found. Skipping...")
            continue
        try:
            logging.info(f"Reading {file_name}...")
            csv_data = blob.download_as_text(encoding="utf-8-sig")
            df = pd.read_csv(io.StringIO(csv_data))
            if df.empty:
                continue
            
            # Kiểm tra xem file có đủ cột không
            missing_cols = [c for c in required_columns if c not in df.columns]
            if missing_cols:
                logging.error(f"{file_name} missing columns: {missing_cols}")
                continue
            
            dataframes.append(df)
            logging.info(f"{file_name}: {len(df)} rows loaded.")
        except Exception as e:
            logging.error(f"Failed to read {file_name}: {e}")

    # --------------------------------------------------------
    # GỘP VÀ LÀM SẠCH DỮ LIỆU
    # --------------------------------------------------------
    if not dataframes:
        logging.warning("No valid news data found today.")
        return

    logging.info("Merging news sources...")
    news_df = pd.concat(dataframes, ignore_index=True)
    
    # Xóa các bài báo trùng lặp dựa trên nguồn và đường dẫn
    news_df.drop_duplicates(subset=["source", "url"], keep="first", inplace=True)

    # Xử lý định dạng thời gian
    news_df["publish_date"] = pd.to_datetime(news_df["publish_date"], errors="coerce").dt.date
    news_df["crawl_time"] = pd.to_datetime(news_df["crawl_time"], errors="coerce")
    news_df.sort_values(by="publish_date", ascending=False, inplace=True)
    news_df.reset_index(drop=True, inplace=True)

    # --------------------------------------------------------
    # TÍCH HỢP TRÍ TUỆ NHÂN TẠO (BATCH PREDICTION CHUẨN XÁC)
    # --------------------------------------------------------
    logging.info("Khởi động AI chấm điểm cho dữ liệu tin tức mới...")
    
    # CẬP NHẬT: Nhập thêm hàm batch mới từ thư viện của bạn
    nlp_model = load_advanced_sentiment_model()
    
    news_df['summary'] = news_df['summary'].fillna("")
    texts = news_df['summary'].astype(str).tolist()
    
    logging.info("Bắt đầu chấm điểm AI theo lô siêu tốc...")
    
    # Gọi hàm xử lý lô: Tự động tách từ underthesea và tính điểm
    scores, labels = analyze_sentiment_batch(texts, nlp_model, batch_size=16)
    
    # Gán vào bảng
    news_df['sentiment_score'] = scores
    news_df['sentiment_label'] = labels
    
    logging.info("Chấm điểm AI hoàn tất thành công!")

    # --------------------------------------------------------
    # CẤU HÌNH BẢNG BIGQUERY MỚI (Bao gồm 2 cột AI)
    # --------------------------------------------------------
    schema = [
        bigquery.SchemaField("article_id", "STRING"),
        bigquery.SchemaField("publish_date", "DATE"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("summary", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("crawl_time", "TIMESTAMP"),
        bigquery.SchemaField("sentiment_score", "INTEGER"),
        bigquery.SchemaField("sentiment_label", "STRING")
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        # WRITE_APPEND: Chỉ chèn thêm dòng mới, giữ nguyên dữ liệu lịch sử bạn đã backfill
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    # --------------------------------------------------------
    # ĐẨY DỮ LIỆU LÊN BIGQUERY
    # --------------------------------------------------------
    logging.info("Uploading AI-scored data to BigQuery Bronze...")
    load_job = bq_client.load_table_from_dataframe(
        news_df, TABLE_ID, job_config=job_config
    )
    load_job.result()

    logging.info("=" * 60)
    logging.info(f"Appended {len(news_df)} new rows into {TABLE_ID}")
    logging.info("News Bronze with AI completed successfully.")
    logging.info("=" * 60)
   
# ============================================================
# CẤU HÌNH VÀ TẠO DAG (LUỒNG CÔNG VIỆC CHÍNH)
# ============================================================

default_args = {
    "owner": "Thanh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="news_ingestion_pipeline_v7",
    description="News Incremental ETL Pipeline with AI Integration (Cloud Airflow)",
    default_args=default_args,
    start_date=datetime(2026, 7, 23),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["news", "etl", "bronze", "ai", "cloud"]
) as dag:

    # TASK 1: Chạy file crawl_news để tải bài viết mới
    crawl_news_operator = PythonOperator(
        task_id="crawl_news",
        python_callable=crawl_news,
        execution_timeout=timedelta(minutes=30),
    )

    # TASK 2: Nạp dữ liệu vào BigQuery Bronze và AI chấm điểm
    load_news_operator = PythonOperator(
        task_id="load_news_to_bronze",
        python_callable=load_news_to_bronze,
        op_kwargs={"project_id": PROJECT_ID, "bucket_name": BUCKET_NAME},
        execution_timeout=timedelta(minutes=90),
    )

    # --------------------------------------------------------
    # TASK 3: dbt Run (Chuyển đổi Bronze -> Silver)
    # Tối ưu: Dùng lệnh run thay vì build để giảm tải RAM, chặn gửi log ngầm
    # --------------------------------------------------------
    # Khai báo tác vụ chạy dbt với cấu hình tự phục hồi
    dbt_run_news = BashOperator(
        task_id='dbt_run_news',
        # Bỏ tham số --no-partial-parse để dbt tận dụng cache, giảm tải cho CPU
        bash_command=(
            'export PATH="/opt/python3.11/bin:/home/airflow/.local/bin:$PATH" && '
            'export DBT_SEND_ANONYMOUS_USAGE_STATS=False && '
            'cd /home/airflow/gcs/dags/dbt_project/ && '
            'dbt run --project-dir . --profiles-dir . --select stg_news_cleansed --full-refresh --threads 1 --quiet'
        ),
        # CƠ CHẾ TỰ PHỤC HỒI (RESILIENCE)
        retries=3,                              # Nếu gặp lỗi mạng/token, tự động thử lại tối đa 3 lần
        retry_delay=timedelta(minutes=2),       # Nghỉ 2 phút giữa mỗi lần thử để giải phóng RAM/CPU
        execution_timeout=timedelta(minutes=30) # Đảm bảo tác vụ không bị kẹt vô hạn
    )

    # --------------------------------------------------------
    # TASK 4: dbt Test (Kiểm tra chất lượng dữ liệu)
    # Tối ưu: Thêm --no-partial-parse và tạm gỡ --quiet để theo dõi lỗi
    # --------------------------------------------------------
    dbt_test_news = BashOperator(
        task_id="dbt_test_news",
        bash_command=(
            "export PATH=\"/opt/python3.11/bin:/home/airflow/.local/bin:$PATH\" && "
            "export DBT_SEND_ANONYMOUS_USAGE_STATS=False && "
            "cd /home/airflow/gcs/dags/dbt_project/ && "
            # THÊM --no-partial-parse và XÓA --quiet
            "dbt test --project-dir . --profiles-dir . --select stg_news_cleansed --threads 1"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # --------------------------------------------------------
    # WORKFLOW
    # --------------------------------------------------------
    (
        crawl_news_operator
        >> load_news_operator
        >> dbt_run_news
        >> dbt_test_news
    )