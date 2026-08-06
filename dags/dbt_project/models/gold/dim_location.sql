{{ config(
    materialized='table',
    schema='gold_mart'
) }}

SELECT

    ROW_NUMBER() OVER (ORDER BY location) AS location_key,

    location

FROM (

    SELECT DISTINCT location

    FROM `project-7cfdad94-4b3b-452c-8da.silver_clean.event`

    WHERE location IS NOT NULL

)