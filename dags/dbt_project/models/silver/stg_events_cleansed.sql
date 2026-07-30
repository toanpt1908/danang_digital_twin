WITH raw_events AS (
    -- Lấy dữ liệu từ nguồn (Bây giờ các cột ngày đã là chuỗi văn bản - STRING)
    SELECT * FROM {{ source('bronze_raw', 'event') }}
),

parsed_dates AS (
    -- BƯỚC 1: XỬ LÝ VÀ ÉP KIỂU DỮ LIỆU BẰNG REGEX
    SELECT
        TRIM(event_name) AS event_name,
        TRIM(location) AS location,
        INITCAP(TRIM(category)) AS category,

        -- BẮT BỆNH VÀ CHỮA ĐỊNH DẠNG NGÀY BẮT ĐẦU
        CASE 
            -- Trường hợp 1: Chuẩn YYYY-MM-DD (VD: 2024-05-15)
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-\d{2}-\d{2}$') 
                THEN SAFE.PARSE_DATE('%Y-%m-%d', TRIM(start_date))
                
            -- Trường hợp 2: YYYY-MM (VD: 2024-05) -> Thêm ngày 01
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-\d{2}$') 
                THEN SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(TRIM(start_date), '-01'))
                
            -- Trường hợp 3: YYYY-Qx (VD: 2026-Q2) -> Đổi thành ngày mùng 1 của tháng đầu quý
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-Q1$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(start_date), '-Q1', '-01-01'))
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-Q2$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(start_date), '-Q2', '-04-01'))
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-Q3$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(start_date), '-Q3', '-07-01'))
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}-Q4$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(start_date), '-Q4', '-10-01'))
            
            -- Trường hợp 4: Chỉ có Năm (VD: 2024) -> Thêm tháng 1 ngày 1
            WHEN REGEXP_CONTAINS(TRIM(start_date), r'^\d{4}$') 
                THEN SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(TRIM(start_date), '-01-01'))
                
            ELSE NULL 
        END AS start_date,

        -- BẮT BỆNH VÀ CHỮA ĐỊNH DẠNG NGÀY KẾT THÚC (Tương tự như trên)
        CASE 
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-\d{2}-\d{2}$') THEN SAFE.PARSE_DATE('%Y-%m-%d', TRIM(end_date))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-\d{2}$') THEN SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(TRIM(end_date), '-01'))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-Q1$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(end_date), '-Q1', '-01-01'))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-Q2$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(end_date), '-Q2', '-04-01'))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-Q3$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(end_date), '-Q3', '-07-01'))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}-Q4$') THEN SAFE.PARSE_DATE('%Y-%m-%d', REPLACE(TRIM(end_date), '-Q4', '-10-01'))
            WHEN REGEXP_CONTAINS(TRIM(end_date), r'^\d{4}$') THEN SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(TRIM(end_date), '-01-01'))
            ELSE NULL 
        END AS end_date

    FROM raw_events
    WHERE
        event_name IS NOT NULL
        AND start_date IS NOT NULL
        AND end_date IS NOT NULL
),

cleansed_events AS (
    -- BƯỚC 2: TÍNH TOÁN VÀ TẠO KHÓA SAU KHI NGÀY THÁNG ĐÃ SẠCH 100%
    SELECT
        CAST(FARM_FINGERPRINT(CONCAT(CAST(start_date AS STRING), '_', event_name)) AS STRING) AS event_id,
        event_name,
        start_date,
        end_date,
        location,
        category,
        DATE_DIFF(end_date, start_date, DAY) + 1 AS duration_days,
        EXTRACT(YEAR FROM start_date) AS event_year
    FROM parsed_dates
)

SELECT * FROM cleansed_events