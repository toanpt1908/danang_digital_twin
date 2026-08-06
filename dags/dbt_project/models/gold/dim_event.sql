{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    ROW_NUMBER() OVER (

        ORDER BY

            SAFE.PARSE_DATE('%Y-%m-%d', e.start_date),

            e.event_name

    ) AS event_key,

    e.event_name,

    SAFE.PARSE_DATE('%Y-%m-%d', e.start_date) AS start_date,

    SAFE.PARSE_DATE('%Y-%m-%d', e.end_date) AS end_date,

    l.location_key,

    e.category

FROM `project-7cfdad94-4b3b-452c-8da.silver_clean.event` e

LEFT JOIN {{ ref('dim_location') }} l

ON e.location = l.location