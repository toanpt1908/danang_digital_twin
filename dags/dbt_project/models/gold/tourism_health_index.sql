{{ config(
    materialized='table',
    schema='gold_mart'
) }}

WITH base AS (

    SELECT *

    FROM {{ ref('fact_tourism') }}

),

normalized AS (

SELECT

    report_date,
    year,
    month,

    ------------------------------------------------------------------
    -- Demand
    ------------------------------------------------------------------

	COALESCE(
		SAFE_DIVIDE(
			avg_search_interest - MIN(avg_search_interest) OVER(),
			NULLIF(
				MAX(avg_search_interest) OVER() -
				MIN(avg_search_interest) OVER(),
				0
			)
		),
	0) * 100 AS demand_score,

    ------------------------------------------------------------------
    -- Mobility
    ------------------------------------------------------------------

	COALESCE(

		SAFE_DIVIDE(
			total_flights - MIN(total_flights) OVER(),
			NULLIF(
				MAX(total_flights) OVER() -
				MIN(total_flights) OVER(),
				0
			)
		),

	0) * 100 AS mobility_score,

    ------------------------------------------------------------------
    -- Environment
    ------------------------------------------------------------------

	(
		COALESCE(
			SAFE_DIVIDE(
				temp_mean_celsius - MIN(temp_mean_celsius) OVER(),
				NULLIF(
					MAX(temp_mean_celsius) OVER() -
					MIN(temp_mean_celsius) OVER(),
					0
				)
			),
		0)

		+

		(
			1 - COALESCE(
				SAFE_DIVIDE(
					rainfall_mm - MIN(rainfall_mm) OVER(),
					NULLIF(
						MAX(rainfall_mm) OVER() -
						MIN(rainfall_mm) OVER(),
						0
					)
				),
			0)
		)

		+

		(
			1 - COALESCE(
				SAFE_DIVIDE(
					wind_speed_max - MIN(wind_speed_max) OVER(),
					NULLIF(
						MAX(wind_speed_max) OVER() -
						MIN(wind_speed_max) OVER(),
						0
					)
				),
			0)
		)

	) / 3 * 100 AS environment_score,

    ------------------------------------------------------------------
    -- Event
    ------------------------------------------------------------------
	COALESCE(

		SAFE_DIVIDE(
			event_count - MIN(event_count) OVER(),
			NULLIF(
				MAX(event_count) OVER() -
				MIN(event_count) OVER(),
				0
			)
		),

	0) * 100 AS event_score,

    ------------------------------------------------------------------
    -- Sentiment
    ------------------------------------------------------------------

	ROUND(

		COALESCE(
			SAFE_DIVIDE(
				positive_news,
				NULLIF(news_count,0)
			),
			0
		) * 100

	,2) AS sentiment_score

FROM base

),

health_index AS (

SELECT

    report_date,

    year,

    month,

    ROUND(demand_score,2) AS demand_score,

    ROUND(mobility_score,2) AS mobility_score,

    ROUND(environment_score,2) AS environment_score,

    ROUND(event_score,2) AS event_score,

    ROUND(sentiment_score,2) AS sentiment_score,

	ROUND(
		(
			demand_score * 0.30 +
			mobility_score * 0.25 +
			environment_score * 0.15 +
			event_score * 0.15 +
			sentiment_score * 0.15
		),
		2
	) AS tourism_health_index

FROM normalized

)

SELECT

    *,

	CASE
		WHEN tourism_health_index >= 80 THEN 'Healthy'
		WHEN tourism_health_index >= 60 THEN 'Watch'
		WHEN tourism_health_index >= 40 THEN 'Warning'
		ELSE 'Critical'
	END AS health_status

FROM health_index

ORDER BY report_date