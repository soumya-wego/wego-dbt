{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_rates_analysis
-- Destination: analysis.hotels_rates_analysis  (unchanged)
-- Schedule   : every mon 02:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('wego_rates_analysis') }}
-- depends_on: {{ source('place_services', 'public_holidays') }}
-- depends_on: {{ source('rates_data', 'country_code_array') }}
{% raw %}
WITH rates_data as 
(SELECT * 
FROM
	(SELECT created_at, check_in, check_out, lead_time, trip_duration, provider_code, hotel_id, hotel_name, 
	site_code, locale,
	ARRAY_TO_STRING(room_ids,",") as room_ids,
	country_code, 
	total_amount_usd, amount_per_room_night,
	click,
	conversions_tracked,
	GENERATE_DATE_ARRAY(check_in, check_out, INTERVAL 1 DAY) as date_array,
	[check_in] as check_in_array,
	[check_out] as check_out_array, 
	[country_code] as country_code_array,
	FROM `wego-cloud.analysis.wego_rates_analysis` 
	WHERE check_in is not null
	--AND country_code = "AE"
	-- LIMIT 1000
	)
)

SELECT *,
ARRAY_LENGTH(public_holiday_dates) as public_holidays,
ARRAY_LENGTH(weekend_dates) as weekends,

FROM
	(SELECT * EXCEPT (check_in_array, check_out_array, date_array, country_code_array),

	ARRAY(
	SELECT DISTINCT stay_dates
	FROM
		(SELECT * FROM UNNEST(date_array) as stay_dates CROSS JOIN rates_data.country_code_array as country_code) as search_combination 
		INNER JOIN
		(SELECT DISTINCT site_code, CAST(date as DATE) as holiday_date
		FROM `wego-cloud.place_services.public_holidays`) as holidays
		ON holidays.site_code = search_combination.country_code 
		AND holidays.holiday_date >= search_combination.stay_dates 
		AND holidays.holiday_date <= search_combination.stay_dates
	) as public_holiday_dates,

	ARRAY(
	SELECT DISTINCT weekend_dates
	FROM
		(SELECT
		IF(country_code in ('DZ', 'BH', 'EG', 'IQ', 'IL', 'JO', 'KW', 'LY', 'MV', 'OM', 'QA', 'SA', 'SS', 'SY', 'YE', 'AE'), 
		IF(stay_dow in (6,7), stay_dates, NULL), IF(stay_dow in (7,1), stay_dates, NULL) ) as weekend_dates
		FROM
			(SELECT stay_dates,
			EXTRACT(DAYOFWEEK from stay_dates) as stay_dow
			FROM UNNEST(date_array) as stay_dates CROSS JOIN rates_data.country_code_array as country_code) as search_combination 
		)
	WHERE weekend_dates IS NOT NULL
	) as weekend_dates,

	FROM rates_data
	)
{% endraw %}
