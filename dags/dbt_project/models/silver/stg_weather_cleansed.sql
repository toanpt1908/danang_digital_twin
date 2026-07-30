WITH raw_weather AS (
    SELECT * FROM {{ source('bronze_raw', 'weather') }}
),

cleansed AS (
    SELECT
        -- 1. TẠO KHÓA CHÍNH dựa vào ngày thời tiết
        CAST(FARM_FINGERPRINT(CAST(date AS STRING)) AS STRING) AS weather_id,

        CAST(date AS DATE) AS weather_date,
        
        -- 2. Logic tính toán: Nếu cột nhiệt độ trung bình bị NULL thì lấy trung bình cao và thấp
        ROUND(
            COALESCE(
                CAST(temp_mean AS FLOAT64), 
                (CAST(temp_max AS FLOAT64) + CAST(temp_min AS FLOAT64)) / 2
            ), 1
        ) AS temp_mean_celsius,
        
        -- 3. Đảm bảo lượng mưa và sức gió luôn là số thập phân
        COALESCE(CAST(rainfall_mm AS FLOAT64), 0.0) AS rainfall_mm,
        COALESCE(CAST(wind_speed_max AS FLOAT64), 0.0) AS wind_speed_max,
        
        CURRENT_TIMESTAMP() AS dbt_processed_at

    FROM raw_weather
    WHERE date IS NOT NULL

    -- 4. Khử trùng lặp theo ngày
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY CAST(temp_mean AS FLOAT64) DESC) = 1
)

SELECT * FROM cleansed