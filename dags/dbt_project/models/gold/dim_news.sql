{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    ROW_NUMBER() OVER (
        ORDER BY report_date, article_id
    ) AS article_key,

    article_id,

    report_date,

    title,

    summary,

    url,

    source,

    source_system,

    crawl_time

FROM {{ ref('stg_news_cleansed') }}