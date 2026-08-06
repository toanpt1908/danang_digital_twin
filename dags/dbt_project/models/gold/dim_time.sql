{{ config(
    materialized='table',
    schema='gold_mart'
) }}

WITH dates AS (

    SELECT report_date AS date_value
    FROM {{ ref('stg_news_cleansed') }}

    UNION DISTINCT

    SELECT report_date
    FROM {{ ref('stg_trend_cleansed') }}

    UNION DISTINCT

    SELECT weather_date
    FROM {{ ref('stg_weather_cleansed') }}

    UNION DISTINCT

    SELECT flight_date
    FROM {{ ref('stg_flight_cleansed') }}

)

SELECT

    date_value AS date_key,

    EXTRACT(YEAR FROM date_value) AS year,

    EXTRACT(QUARTER FROM date_value) AS quarter,

    EXTRACT(MONTH FROM date_value) AS month,

    EXTRACT(DAY FROM date_value) AS day,

    EXTRACT(WEEK FROM date_value) AS week_of_year,

    FORMAT_DATE('%A', date_value) AS day_of_week,

    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_value) IN (1,7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM dates

ORDER BY date_key