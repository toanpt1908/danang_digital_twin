{{ config(
    materialized='table',
    schema='gold_mart'
) }}

WITH news AS (

SELECT

    EXTRACT(YEAR FROM n.report_date) AS year,
    EXTRACT(MONTH FROM n.report_date) AS month,

    COUNT(*) AS news_count,

    SUM(CASE WHEN fn.sentiment_key = 3 THEN 1 ELSE 0 END) AS positive_news,
    SUM(CASE WHEN fn.sentiment_key = 2 THEN 1 ELSE 0 END) AS neutral_news,
    SUM(CASE WHEN fn.sentiment_key = 1 THEN 1 ELSE 0 END) AS negative_news

FROM {{ ref('fact_news') }} fn

JOIN {{ ref('dim_news') }} n
ON fn.article_key = n.article_key

GROUP BY
    year,
    month

),

trend AS (

SELECT

    EXTRACT(YEAR FROM report_date) AS year,
    EXTRACT(MONTH FROM report_date) AS month,

    AVG(search_interest) AS avg_search_interest

FROM {{ ref('stg_trend_cleansed') }}

GROUP BY
    year,
    month

),

weather AS (

SELECT

    EXTRACT(YEAR FROM weather_date) AS year,
    EXTRACT(MONTH FROM weather_date) AS month,

    AVG(temp_mean_celsius) AS temp_mean_celsius,
    AVG(rainfall_mm) AS rainfall_mm,
    AVG(wind_speed_max) AS wind_speed_max

FROM {{ ref('stg_weather_cleansed') }}

GROUP BY
    year,
    month

),

flight AS (

SELECT

    EXTRACT(YEAR FROM flight_date) AS year,
    EXTRACT(MONTH FROM flight_date) AS month,

    SUM(domestic_flights) AS domestic_flights,
    SUM(international_flights) AS international_flights,
    SUM(total_flights) AS total_flights

FROM {{ ref('stg_flight_cleansed') }}

GROUP BY
    year,
    month

),

event_daily AS (

SELECT

    d AS report_date,

    event_key

FROM {{ ref('dim_event') }}

CROSS JOIN UNNEST(

    GENERATE_DATE_ARRAY(

        start_date,

        COALESCE(end_date,start_date)

    )

) d

),

event_summary AS (

SELECT

    EXTRACT(YEAR FROM report_date) AS year,
    EXTRACT(MONTH FROM report_date) AS month,

    COUNT(event_key) AS event_count

FROM event_daily

GROUP BY
    year,
    month

),

tourism AS (

SELECT

    stat_year AS year,

    stat_month AS month,

    accommodation_guests,

    accommodation_intl_guests,

    accommodation_domestic_guests,

    tourism_revenue_billion_vnd

FROM {{ ref('stg_tourism_stats_cleansed') }}

WHERE stat_year >= 2020
AND stat_month BETWEEN 1 AND 12

)

SELECT

    DATE(t.year,t.month,1) AS report_date,

    t.year,

    t.month,

    1 AS day,

    COALESCE(n.news_count,0) AS news_count,

    COALESCE(n.positive_news,0) AS positive_news,

    COALESCE(n.neutral_news,0) AS neutral_news,

    COALESCE(n.negative_news,0) AS negative_news,

    tr.avg_search_interest,

    w.temp_mean_celsius,

    w.rainfall_mm,

    w.wind_speed_max,

    f.domestic_flights,

    f.international_flights,

    f.total_flights,

    COALESCE(e.event_count,0) AS event_count,

    t.accommodation_guests,

    t.accommodation_intl_guests,

    t.accommodation_domestic_guests,

    t.tourism_revenue_billion_vnd

FROM tourism t

LEFT JOIN news n

ON t.year = n.year
AND t.month = n.month

LEFT JOIN trend tr

ON t.year = tr.year
AND t.month = tr.month

LEFT JOIN weather w

ON t.year = w.year
AND t.month = w.month

LEFT JOIN flight f

ON t.year = f.year
AND t.month = f.month

LEFT JOIN event_summary e

ON t.year = e.year
AND t.month = e.month

ORDER BY
t.year,
t.month