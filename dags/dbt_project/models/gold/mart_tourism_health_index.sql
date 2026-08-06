{{ config(
    materialized='table',
	schema='gold_mart'
) }}

WITH tourism AS (

    SELECT *
    FROM {{ ref('fact_tourism') }}

),

normalized AS (

SELECT

    report_date,
    year,
    month,
    day,

    accommodation_guests,
    tourism_revenue_billion_vnd,
    total_flights,
    avg_search_interest,
    event_count,

    SAFE_DIVIDE(
        accommodation_guests -
        MIN(accommodation_guests) OVER (),
        NULLIF(MAX(accommodation_guests) OVER ()
             - MIN(accommodation_guests) OVER (),0)
    ) * 100 AS visitor_score,

    SAFE_DIVIDE(
        tourism_revenue_billion_vnd -
        MIN(tourism_revenue_billion_vnd) OVER (),
        NULLIF(MAX(tourism_revenue_billion_vnd) OVER ()
             - MIN(tourism_revenue_billion_vnd) OVER (),0)
    ) * 100 AS revenue_score,

    SAFE_DIVIDE(
        total_flights -
        MIN(total_flights) OVER (),
        NULLIF(MAX(total_flights) OVER ()
             - MIN(total_flights) OVER (),0)
    ) * 100 AS flight_score,

    SAFE_DIVIDE(
        avg_search_interest -
        MIN(avg_search_interest) OVER (),
        NULLIF(MAX(avg_search_interest) OVER ()
             - MIN(avg_search_interest) OVER (),0)
    ) * 100 AS search_score,

    SAFE_DIVIDE(
        event_count -
        MIN(event_count) OVER (),
        NULLIF(MAX(event_count) OVER ()
             - MIN(event_count) OVER (),0)
    ) * 100 AS event_score

FROM tourism

),

health AS (

SELECT

*,

ROUND(

      visitor_score * 0.30
    + revenue_score * 0.25
    + flight_score * 0.20
    + search_score * 0.15
    + event_score * 0.10

,2) AS tourism_health_index

FROM normalized

)

SELECT

*,

CASE

WHEN tourism_health_index >= 80
THEN 'Excellent'

WHEN tourism_health_index >= 60
THEN 'Good'

WHEN tourism_health_index >= 40
THEN 'Moderate'

ELSE 'Poor'

END AS health_status

FROM health