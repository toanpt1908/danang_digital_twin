WITH raw_flight AS (
    -- Lấy dữ liệu từ tầng Bronze khai báo trong sources.yml
    SELECT * FROM {{ source('bronze_raw', 'flight') }}
),

cleansed AS (
    SELECT
        -- 1. Tạo Khóa chính (Surrogate Key) dựa vào ngày bay
        CAST(FARM_FINGERPRINT(CAST(flight_date AS STRING)) AS STRING) AS flight_id,

        -- 2. Ép kiểu định dạng Ngày tháng
        CAST(flight_date AS DATE) AS flight_date,
        
        -- 3. Xử lý NULL: Nếu không có dữ liệu, mặc định là 0
        COALESCE(CAST(domestic_flights AS INT64), 0) AS domestic_flights,
        COALESCE(CAST(international_flights AS INT64), 0) AS international_flights,
        
        -- 4. Tự động tính lại tổng số chuyến bay để đảm bảo tính toàn vẹn 100%
        (COALESCE(CAST(domestic_flights AS INT64), 0) + COALESCE(CAST(international_flights AS INT64), 0)) AS total_flights,
        
        -- 5. Chuẩn hóa chuỗi văn bản
        CAST(data_source AS STRING) AS data_source,
        
        -- 6. Dấu vết thời gian xử lý
        CURRENT_TIMESTAMP() AS dbt_processed_at

    FROM raw_flight
    -- Chỉ lấy các dòng có ngày tháng hợp lệ
    WHERE flight_date IS NOT NULL

    -- 7. Khử trùng lặp: Nhóm theo ngày, lấy dòng có dữ liệu tổng chuyến bay ở nguồn cao nhất
    QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_date ORDER BY CAST(total_flights AS INT64) DESC) = 1
)

SELECT * FROM cleansed