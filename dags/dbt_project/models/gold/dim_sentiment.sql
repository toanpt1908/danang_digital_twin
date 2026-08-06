{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    ROW_NUMBER() OVER (
        ORDER BY sentiment_label
    ) AS sentiment_key,

    sentiment_label

FROM (

    SELECT DISTINCT sentiment_label

    FROM {{ ref('stg_news_cleansed') }}

    WHERE sentiment_label IS NOT NULL

)