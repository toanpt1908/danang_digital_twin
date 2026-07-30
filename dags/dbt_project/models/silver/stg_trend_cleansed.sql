WITH cleansed_trends AS (
    SELECT 
        -- 1. TẠO KHÓA CHÍNH
        CAST(FARM_FINGERPRINT(CONCAT(CAST(search_date AS STRING), '_', keyword)) AS STRING) AS trend_id,
        
        -- 2. ĐỔI TÊN VÀ LÀM SẠCH DỮ LIỆU
        search_date AS report_date,
        LOWER(TRIM(keyword)) AS keyword,
        CAST(search_interest AS INT64) AS search_interest,
        LOWER(TRIM(source)) AS source,
        LOWER(TRIM(source_system)) AS source_system,
        crawl_time,
        
        -- 3. TRÍCH XUẤT THỜI GIAN
        EXTRACT(YEAR FROM search_date) AS year,
        EXTRACT(MONTH FROM search_date) AS month,
        FORMAT_DATE('%Y-%m', search_date) AS year_month

    FROM {{ source('bronze_raw', 'google_trends') }}
    
    -- 4. LỌC DỮ LIỆU RỖNG
    WHERE
        keyword IS NOT NULL
        AND search_interest IS NOT NULL
)

SELECT * FROM cleansed_trends