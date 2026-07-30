{{ config(materialized='view') }}

WITH deduplicated_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY TRIM(url) ORDER BY crawl_time DESC) as rn
    FROM {{ source('bronze_raw', 'news') }}
    WHERE
        article_id IS NOT NULL
        AND title IS NOT NULL
        AND publish_date IS NOT NULL
)

SELECT
    TRIM(article_id) AS article_id,
    DATE(publish_date) AS report_date,
    TRIM(title) AS title,
    TRIM(summary) AS summary,
    TRIM(url) AS url,
    LOWER(TRIM(source)) AS source,
    LOWER(TRIM(source_system)) AS source_system,
    crawl_time,
    LENGTH(TRIM(title)) AS title_length,
    LENGTH(TRIM(summary)) AS summary_length,
    -- BỔ SUNG: Chuyển tiếp kết quả từ mô hình AI cảm xúc
    COALESCE(sentiment_score, 0) AS sentiment_score,
    COALESCE(TRIM(sentiment_label), 'Uncategorized') AS sentiment_label
FROM deduplicated_source
WHERE rn = 1