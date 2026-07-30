# Digital Twin Du Lịch Đà Nẵng - Data Lakehouse Pipeline

Dự án xây dựng Hệ thống Thông tin Quản lý (MIS) và luồng xử lý dữ liệu tự động (Data Pipeline) nhằm mô phỏng và phân tích hệ sinh thái du lịch tại thành phố Đà Nẵng theo thời gian thực (Digital Twin).

Hệ thống áp dụng kiến trúc **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** để trích xuất, làm sạch và mô hình hóa 6 nguồn dữ liệu cốt lõi, phục vụ cho việc trực quan hóa trên BI Dashboard.

---

## Ngăn xếp Công nghệ (Tech Stack)

* **Orchestration:** Apache Airflow (Triển khai trên Google Cloud Composer 3)
* **Data Transformation:** dbt (Data Build Tool - dbt-core & dbt-bigquery)
* **Data Warehouse:** Google BigQuery
* **Data Lake / Storage:** Google Cloud Storage (GCS)
* **Ngôn ngữ Lập trình:** Python 3.11, SQL (Jinja)
* **BI & Visualization:** Looker Studio

---

## 6 Nguồn Dữ Liệu Tích Hợp

Hệ thống thu thập và xử lý đồng thời 6 nhóm dữ liệu định lượng và định tính:
1. **Chuyến bay (Flight):** Số lượng chuyến bay nội địa & quốc tế đến Đà Nẵng (Real-time).
2. **Thời tiết (Weather):** Nhiệt độ, lượng mưa, sức gió (Real-time).
3. **Thống kê Du lịch (Tourism Stats):** Lượng khách du lịch, doanh thu lưu trú (Generated/Static).
4. **Tin tức (News):** Các bài báo, tin tức liên quan đến du lịch Đà Nẵng.
5. **Sự kiện (Events):** Lịch trình lễ hội, sự kiện văn hóa, giải trí (Generated/Static).
6. **Xu hướng (Google Trends):** Chỉ số tìm kiếm các từ khóa du lịch Đà Nẵng trên mạng xã hội.

---

## Cấu trúc Thư mục (Repository Structure)

Mã nguồn được quy hoạch theo chuẩn phân tách nhiệm vụ giữa Data Engineering (Hạ tầng/Airflow) và Analytics Engineering (dbt/SQL):

```text
danang_digital_twin/
│
├── dags/                                # ☁️ Kịch bản Airflow (Triển khai trên GCS Bucket)
│   ├── 01_flight_ingestion.py           # DAG: Cào dữ liệu chuyến bay
│   ├── 02_weather_ingestion.py          # DAG: Cào dữ liệu thời tiết
│   ├── 03_news_ingestion.py
│   ├── 04_trend_ingestion.py       # DAG: Cào dữ liệu tin tức & xu hướng
│   │
│   └── dbt_project/                     # 🔄 Chuyển hóa dữ liệu bằng dbt
│       ├── dbt_project.yml              # Cấu hình gốc dbt
│       ├── profiles.yml                 # Cấu hình kết nối BigQuery
│       │
│       ├── seeds/                       # 🌱 Dữ liệu tĩnh/giả lập (Nạp tự động)
│       │   ├── tourism_stats.csv        # Dữ liệu thống kê du lịch (CSV)
│       │   └── events.csv               # Danh sách sự kiện lễ hội (CSV)
│       │
│       └── models/                 
│           ├── sources.yml              # Khai báo 6 bảng thô từ tầng Bronze
│           │
│           ├── silver/                  # 🥈 Tầng làm sạch (Staging Models)
│           │   ├── stg_flight_cleansed.sql         # Code SQL (Toàn)
│           │   ├── stg_weather_cleansed.sql        # Code SQL (Toàn)
│           │   ├── stg_tourism_stats_cleansed.sql  # Code SQL (Toàn)
│           │   ├── stg_news_cleansed.sql           # Code SQL (Mỹ Thanh)
│           │   ├── stg_events_cleansed.sql         # Code SQL (Mỹ Thanh)
│           │   ├── stg_trend_cleansed.sql          # Code SQL (Mỹ Thanh)
│           │   └── schema.yml                      # Data Quality Tests (Unique, Not Null)
│           │
│           └── gold/                    # 🥇 Tầng tổng hợp (Marts / Digital Twin)
│               └── fact_daily_tourism_twin.sql     # Code SQL Star Schema (Mỹ Thanh)
│
├── .gitignore                           # Bảo vệ file cấu hình và GCP Service Account Key
└── README.md                            # Tài liệu dự án

```

---

## Hướng dẫn Cài đặt & Triển khai (Local Setup)

### 1. Yêu cầu hệ thống (Prerequisites)

* Đã cài đặt **Python 3.9+** và **Git**.
* Đã có file **Google Cloud Service Account Key** (`gcp-key.json`).

### 2. Thiết lập Môi trường Cục bộ

Mở Terminal và thực hiện các lệnh sau:

```bash
# Clone dự án về máy
git clone <đường-dẫn-repo-của-bạn>
cd danang_digital_twin

# Tạo môi trường ảo (Virtual Environment)
python -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate

# Cài đặt thư viện dbt cho BigQuery
pip install dbt-bigquery

```

### 3. Cấu hình dbt

1. Đặt file `gcp-key.json` vào thư mục gốc của dự án (Lưu ý: Đảm bảo file `.gitignore` đã có dòng `*.json`).
2. Kiểm tra kết nối từ máy cá nhân đến BigQuery:

```bash
cd dags/dbt_project/
dbt debug --profiles-dir .

```

### 4. Vận hành dbt (Data Transformation)

Để nạp dữ liệu từ thư mục `seeds/` (events.csv, tourism_stats.csv) lên BigQuery:

```bash
dbt seed --profiles-dir .

```

Để thực thi mã SQL làm sạch (Silver) và tạo bảng tổng hợp (Gold):

```bash
dbt run --profiles-dir .

```

Để kiểm tra lỗi dữ liệu (Data Quality Tests):

```bash
dbt test --profiles-dir .

```

---

## Triển khai lên Google Cloud Composer (Airflow)

Hệ thống được tự động hóa 100% trên GCP. Quy trình triển khai:

1. Đồng bộ thư mục `dags/` (bao gồm code Python và thư mục `dbt_project/`) lên GCS Bucket của môi trường Cloud Composer.
2. Các DAG Python sẽ chạy lịch trình trích xuất dữ liệu thô (Bronze).
3. Sau khi hoàn tất, Task `BashOperator` sẽ tự động kích hoạt lệnh `dbt run` ngay trên máy chủ để chuyển hóa dữ liệu lên tầng Silver và Gold.

---

## Đội ngũ Phát triển (Contributors)

Hệ thống được phát triển song song dựa trên chuyên môn:

* **Toàn (Data / DevOps Engineer):** Phụ trách xây dựng hạ tầng GCP Cloud Composer, viết kịch bản cào dữ liệu tự động (Airflow DAGs), thiết lập luồng CI/CD, và phụ trách các nhóm dữ liệu cốt lõi (Flight, Weather, Tourism Stats).
* **Mỹ Thanh (Analytics Engineer / BI Designer):** Phụ trách kiến trúc mô hình dữ liệu trên BigQuery (dbt Models), tối ưu hóa các câu lệnh SQL tầng Silver/Gold, phụ trách nhóm dữ liệu sự kiện/xu hướng (News, Events, Trend) và thiết kế giao diện Trực quan hóa (Looker Studio Dashboard).

```

```