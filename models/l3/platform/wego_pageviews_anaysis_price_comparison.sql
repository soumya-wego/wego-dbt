{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : wego_pageviews_anaysis_price_comparison
-- Destination: analysis.wego_pageviews_anaysis_price_comparison  (unchanged)
-- Schedule   : every day 01:50   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('wego_pageviews_analysis') }}
{% raw %}
-- CREATE OR REPLACE TABLE `wego-cloud.analysis.wego_pageviews_anaysis_price_comparison` PARTITION BY DATE(created_at) AS
-- WITH
-- seo_price AS(
--   SELECT
--   created_at,
--   JSON_VALUE(event_value, '$.price_comparison_id') AS price_comparison_id,
--   JSON_VALUE(event_value, '$.departure_city_code') AS departure_city_code,
--   JSON_VALUE(event_value, '$.arrival_city_code') AS arrival_city_code,
--   JSON_VALUE(event_value, '$.departure_date') AS departure_date,
--   JSON_VALUE(event_value, '$.return_date') AS return_date,
--   JSON_VALUE(event_value, '$.trip_duration_days') AS trip_duration_days,
--   JSON_VALUE(event_value, '$.trip_type') AS trip_type,
--   JSON_VALUE(event_value, '$.ticket_class') AS ticket_class,
--   JSON_VALUE(event_value, '$.price_usd') AS price_usd,
--   JSON_VALUE(event_value, '$.airline_code') AS airline_code,
--   JSON_VALUE(event_value, '$.page_type') AS page_type,
--   JSON_VALUE(event_value, '$.page_url') AS page_url
--   FROM `wego-cloud.analysis.wego_pageviews_analysis` 
--   WHERE TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2026-01-01")
--   AND event_action='seo_price_click'
-- ),
-- search_click AS(
--   SELECT
--   created_at,
--   JSON_VALUE(event_value, '$.price_comparison_id') AS price_comparison_id,
--   JSON_VALUE(event_value, '$.departure_city_code') AS departure_city_code,
--   JSON_VALUE(event_value, '$.arrival_city_code') AS arrival_city_code,
--   JSON_VALUE(event_value, '$.departure_date') AS departure_date,
--   JSON_VALUE(event_value, '$.return_date') AS return_date,
--   JSON_VALUE(event_value, '$.search_id') AS search_id,
--   JSON_VALUE(event_value, '$.trip_type') AS trip_type,
--   JSON_VALUE(event_value, '$.ticket_class') AS ticket_class,
--   JSON_VALUE(event_value, '$.lowest_price_usd') AS lowest_price_usd,
--   JSON_VALUE(event_value, '$.lowest_price_airline_code') AS lowest_price_airline_code
--   FROM `wego-cloud.analysis.wego_pageviews_analysis` 
--   WHERE TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2026-01-01")
--   AND event_action='search_result_price'
-- )
-- SELECT
-- a.created_at,
-- COALESCE(a.price_comparison_id, b.price_comparison_id) AS price_comparison_id,
-- a.departure_city_code AS departure_city_code_homepage,
-- a.arrival_city_code AS arrival_city_code_homepage,
-- a.departure_date AS departure_date_homepage,
-- a.return_date AS return_date_homepage,
-- a.trip_duration_days AS trip_duration_days_homepage,
-- a.trip_type AS trip_type_homepage,
-- a.ticket_class AS ticket_class_homepage,
-- a.price_usd AS lowest_price_homepage,
-- a.airline_code AS airline_code_homepage,
-- a.page_type AS page_type_homepage,
-- a.page_url,
-- b.departure_city_code AS departure_city_code_search,
-- b.arrival_city_code AS arrival_city_code_search,
-- b.departure_date AS departure_date_search,
-- b.return_date AS return_date_search,
-- b.search_id,
-- b.trip_type AS trip_type_search,
-- b.ticket_class AS ticket_class,
-- b.lowest_price_usd AS lowest_price_search,
-- b.lowest_price_airline_code AS lowest_price_airline_code_search
-- FROM
-- seo_price a
-- LEFT JOIN
-- search_click b
-- ON
-- a.price_comparison_id=b.price_comparison_id

INSERT INTO wego-cloud.analysis.wego_pageviews_anaysis_price_comparison
WITH
seo_price AS(
  SELECT
  created_at,
  JSON_VALUE(event_value, '$.price_comparison_id') AS price_comparison_id,
  JSON_VALUE(event_value, '$.departure_city_code') AS departure_city_code,
  JSON_VALUE(event_value, '$.arrival_city_code') AS arrival_city_code,
  JSON_VALUE(event_value, '$.departure_date') AS departure_date,
  JSON_VALUE(event_value, '$.return_date') AS return_date,
  JSON_VALUE(event_value, '$.trip_duration_days') AS trip_duration_days,
  JSON_VALUE(event_value, '$.trip_type') AS trip_type,
  JSON_VALUE(event_value, '$.ticket_class') AS ticket_class,
  JSON_VALUE(event_value, '$.price_usd') AS price_usd,
  JSON_VALUE(event_value, '$.airline_code') AS airline_code,
  JSON_VALUE(event_value, '$.page_type') AS page_type,
  JSON_VALUE(event_value, '$.page_url') AS page_url
  FROM `wego-cloud.analysis.wego_pageviews_analysis` 
  WHERE TIMESTAMP_TRUNC(created_at, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  AND event_action='seo_price_click'
),
search_click AS(
  SELECT
  created_at,
  JSON_VALUE(event_value, '$.price_comparison_id') AS price_comparison_id,
  JSON_VALUE(event_value, '$.departure_city_code') AS departure_city_code,
  JSON_VALUE(event_value, '$.arrival_city_code') AS arrival_city_code,
  JSON_VALUE(event_value, '$.departure_date') AS departure_date,
  JSON_VALUE(event_value, '$.return_date') AS return_date,
  JSON_VALUE(event_value, '$.search_id') AS search_id,
  JSON_VALUE(event_value, '$.trip_type') AS trip_type,
  JSON_VALUE(event_value, '$.ticket_class') AS ticket_class,
  JSON_VALUE(event_value, '$.lowest_price_usd') AS lowest_price_usd,
  JSON_VALUE(event_value, '$.lowest_price_airline_code') AS lowest_price_airline_code
  FROM `wego-cloud.analysis.wego_pageviews_analysis` 
  WHERE TIMESTAMP_TRUNC(created_at, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  AND event_action='search_result_price'
)
SELECT
a.created_at,
COALESCE(a.price_comparison_id, b.price_comparison_id) AS price_comparison_id,
a.departure_city_code AS departure_city_code_homepage,
a.arrival_city_code AS arrival_city_code_homepage,
a.departure_date AS departure_date_homepage,
a.return_date AS return_date_homepage,
a.trip_duration_days AS trip_duration_days_homepage,
a.trip_type AS trip_type_homepage,
a.ticket_class AS ticket_class_homepage,
a.price_usd AS lowest_price_homepage,
a.airline_code AS airline_code_homepage,
a.page_type AS page_type_homepage,
a.page_url,
b.departure_city_code AS departure_city_code_search,
b.arrival_city_code AS arrival_city_code_search,
b.departure_date AS departure_date_search,
b.return_date AS return_date_search,
b.search_id,
b.trip_type AS trip_type_search,
b.ticket_class AS ticket_class,
b.lowest_price_usd AS lowest_price_search,
b.lowest_price_airline_code AS lowest_price_airline_code_search
FROM
seo_price a
LEFT JOIN
search_click b
ON
a.price_comparison_id=b.price_comparison_id
{% endraw %}
