{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    year,

    month,

    SUM(news_count) AS total_news,

    AVG(avg_search_interest) AS avg_search_interest,

    AVG(temp_mean_celsius) AS avg_temperature,

    SUM(rainfall_mm) AS total_rainfall,

    MAX(wind_speed_max) AS max_wind_speed,

    SUM(domestic_flights) AS total_domestic_flights,

    SUM(international_flights) AS total_international_flights,

    SUM(total_flights) AS total_flights,

    SUM(event_count) AS total_events,

    MAX(accommodation_guests) AS accommodation_guests,

    MAX(accommodation_intl_guests) AS accommodation_intl_guests,

    MAX(accommodation_domestic_guests) AS accommodation_domestic_guests,

    MAX(tourism_revenue_billion_vnd) AS tourism_revenue_billion_vnd

FROM {{ ref('fact_tourism') }}

GROUP BY

    year,

    month

ORDER BY

    year,

    month