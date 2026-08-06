{{ config(
    materialized='table',
    schema='gold_mart'
) }}

WITH base AS (

SELECT *

FROM {{ ref('tourism_health_index') }}

)

SELECT

report_date,

'Baseline' AS scenario,

tourism_health_index,

demand_score,

mobility_score,

environment_score,

event_score,

sentiment_score

FROM base

UNION ALL

SELECT

report_date,

'High Demand',

ROUND(
(
demand_score*1.20*0.30+
mobility_score*0.25+
environment_score*0.15+
event_score*0.15+
sentiment_score*0.15
),2
),

demand_score*1.20,

mobility_score,

environment_score,

event_score,

sentiment_score

FROM base

UNION ALL

SELECT

report_date,

'Festival Season',

ROUND(
(
demand_score*0.30+
mobility_score*0.25+
environment_score*0.15+
(event_score*1.5)*0.15+
sentiment_score*0.15
),2
),

demand_score,

mobility_score,

environment_score,

event_score*1.5,

sentiment_score

FROM base