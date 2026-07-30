WITH raw_tourism AS (
    -- Lấy dữ liệu thô từ bảng nguồn
    SELECT * FROM {{ source('bronze_raw', 'tourism_stats') }}
),

cleansed AS (
    SELECT
        -- 1. Tạo khóa chính (Primary Key) bằng cách ghép năm, tháng và loại báo cáo lại với nhau.
        -- Ví dụ: "2024-3-monthly". Việc này giúp dbt dễ dàng kiểm tra trùng lặp.
        CONCAT(CAST(year AS STRING), '-', CAST(month AS STRING), '-', CAST(period_type AS STRING)) AS stat_id,
        
        -- 2. Chuyển đổi các cột thời gian và phân loại
        CAST(year AS INT64) AS stat_year,
        CAST(month AS INT64) AS stat_month,
        CAST(period_type AS STRING) AS period_type,
        
        -- 3. Xử lý giá trị rỗng cho các cột số lượng khách. 
        -- Nếu cột bị trống (NULL), hàm COALESCE sẽ tự động điền số 0.
        COALESCE(CAST(accommodation_guests AS INT64), 0) AS accommodation_guests,
        COALESCE(CAST(accommodation_intl_guests AS INT64), 0) AS accommodation_intl_guests,
        COALESCE(CAST(accommodation_domestic_guests AS INT64), 0) AS accommodation_domestic_guests,
        
        -- 4. Xử lý giá trị rỗng cho cột doanh thu (tỷ VNĐ). Dùng kiểu số thực (FLOAT64).
        COALESCE(CAST(tourism_revenue_billion_vnd AS FLOAT64), 0.0) AS tourism_revenue_billion_vnd,
        
        -- 5. Ghi nhận thời điểm mã dbt chạy làm sạch dòng dữ liệu này
        CURRENT_TIMESTAMP() AS dbt_processed_at

    FROM raw_tourism
    -- Loại bỏ các dòng không có dữ liệu năm hợp lệ
    WHERE year IS NOT NULL
    
    -- 6. Lọc trùng lặp: Nếu có nhiều dòng cùng năm, tháng và loại kỳ báo cáo, 
    -- hệ thống sẽ ưu tiên giữ lại dòng có số lượng khách cao nhất.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY year, month, period_type ORDER BY accommodation_guests DESC) = 1
)

SELECT * FROM cleansed