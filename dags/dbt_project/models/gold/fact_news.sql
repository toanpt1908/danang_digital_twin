{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    n.article_key,

    t.date_key,

    s.sentiment_key,

    news.sentiment_score

FROM {{ ref('stg_news_cleansed') }} news

LEFT JOIN {{ ref('dim_news') }} n
ON news.article_id = n.article_id

LEFT JOIN {{ ref('dim_time') }} t
ON news.report_date = t.date_key

LEFT JOIN {{ ref('dim_sentiment') }} s
ON news.sentiment_label = s.sentiment_label