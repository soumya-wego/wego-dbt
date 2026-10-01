{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : trains_sessions_daily_append
-- Destination: wego_analytics.trains_sessions  (unchanged)
-- Schedule   : every day 01:15   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_genzo', 'events_actions') }}
-- depends_on: {{ source('wego_analytics', 'clients') }}
-- depends_on: {{ source('wego_analytics', 'pageviews') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
-- -- backfill
-- create table wego_analytics.trains_sessions
-- partition by session_date
-- AS

-- WITH train_event as
-- (
-- SELECT DISTINCT
-- * 
-- FROM 
-- ((SELECT 
--   created_at as train_trigger_timestamp,
--   session_id
-- FROM
--   `wego-cloud.wego_analytics.pageviews`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN "2022-01-01" AND date_sub(current_date(), INTERVAL 1 DAY)
--  AND site_code = "IN"
--  AND product = "mini_app")
--  UNION ALL 
--  (SELECT 
-- e.created_at as train_trigger_timestamp,
-- session_id
-- FROM
--   `services_genzo.events_actions*` e
--   INNER JOIN 
--   (SELECT DISTINCT
-- session_id,
-- pageview_id
-- FROM
--   `wego-cloud.wego_analytics.pageviews`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN "2022-01-01" AND date_sub(current_date(), INTERVAL 1 DAY)
--  AND site_code = "IN") p ON e.event.id = p.pageview_id
-- WHERE
--   _TABLE_SUFFIX BETWEEN "20220101" AND format_date("%Y%m%d", date_sub(current_date(), interval 1 day))
--   AND device.platform = "DESKTOP"
--   AND geocode.country_code = "IN"
--   AND event.category = "trains_booking"
--   AND event.object = "trains"
--   AND event.action = "handoff")
--  ) )

-- SELECT distinct
--   date(created_at) as session_date,
--   FIRST_VALUE(train_trigger_timestamp) OVER(PARTITION BY s.session_id ORDER BY train_trigger_timestamp ASC) as train_trigger_timestamp,
--   created_at,
--   IF(first_visit = true, "new", "returning") as new_returning_status,
--   s.session_id,
--   client_id,
--   advertiser_id,
--   client_type,
--   device,
--   device_type,
--   os_type,
--   app_version,
--   device_version,
--   os_version,
--   user_country_code,
--   user_city,
--   network_type,
--   network_carrier_name,
--   landing_url,
--   referrer_url,
--   site_code,
--   locale,
--   channel,
--   wg_source,
--   wg_medium,
--   wg_campaign,
--   wg_adgroup,
--   wg_content,
--   wg_term,
--   wg_misc,
--   ts_code,
--   SOURCE,
--   market,
--   app_rt_source,
--   app_rt_medium,
--   app_rt_campaign,
--   app_rt_adgroup,
--   app_rt_content
--     FROM
--   `wego-cloud.wego_analytics.sessions` s 
--   LEFT JOIN
--   train_event e on s.session_id = e.session_id
--   LEFT JOIN 
--   (SELECT
--   created_at as c_created_at,
--   client_id as c_client_id,
--   first_visit
--   FROM 
--   `wego-cloud.wego_analytics.clients`
--   WHERE
--   DATE(_PARTITIONTIME) BETWEEN "2022-01-01" AND date_sub(current_date(), INTERVAL 1 DAY)) c on s.client_id = c.c_client_id and date(s.created_at) = date(c.c_created_at)
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN "2022-01-01" AND date_sub(current_date(), INTERVAL 1 DAY)
--   AND s.session_id IN(
-- SELECT
-- DISTINCT session_id
-- FROM 
-- train_event)

-- daily append:
WITH train_event as
(
SELECT DISTINCT
* 
FROM 
((SELECT 
  created_at as train_trigger_timestamp,
  session_id
FROM
  `wego-cloud.wego_analytics.pageviews`
WHERE
  DATE(_PARTITIONTIME) 
  -- BETWEEN "2022-01-01" AND 
  =
  date_sub(current_date(), INTERVAL 1 DAY)
 AND site_code = "IN"
 AND product = "mini_app")
 UNION ALL 
 (SELECT 
e.created_at as train_trigger_timestamp,
session_id
FROM
  `services_genzo.events_actions*` e
  INNER JOIN 
  (SELECT DISTINCT
session_id,
pageview_id
FROM
  `wego-cloud.wego_analytics.pageviews`
WHERE
  DATE(_PARTITIONTIME) = date_sub(current_date(), INTERVAL 1 DAY)
 AND site_code = "IN") p ON e.event.id = p.pageview_id
WHERE
  _TABLE_SUFFIX = format_date("%Y%m%d", date_sub(current_date(), interval 1 day))
  AND device.platform = "DESKTOP"
  AND geocode.country_code = "IN"
  AND event.category = "trains_booking"
  AND event.object = "trains"
  AND event.action = "handoff")
 ) )

SELECT distinct
  date(created_at) as session_date,
  FIRST_VALUE(train_trigger_timestamp) OVER(PARTITION BY s.session_id ORDER BY train_trigger_timestamp ASC) as train_trigger_timestamp,
  created_at,
  IF(first_visit = true, "new", "returning") as new_returning_status,
  s.session_id,
  client_id,
  advertiser_id,
  client_type,
  device,
  device_type,
  os_type,
  app_version,
  device_version,
  os_version,
  user_country_code,
  user_city,
  network_type,
  network_carrier_name,
  landing_url,
  referrer_url,
  site_code,
  locale,
  channel,
  wg_source,
  wg_medium,
  wg_campaign,
  wg_adgroup,
  wg_content,
  wg_term,
  wg_misc,
  ts_code,
  SOURCE,
  market,
  app_rt_source,
  app_rt_medium,
  app_rt_campaign,
  app_rt_adgroup,
  app_rt_content
    FROM
  `wego-cloud.wego_analytics.sessions` s 
  LEFT JOIN
  train_event e on s.session_id = e.session_id
  LEFT JOIN 
  (SELECT
  created_at as c_created_at,
  client_id as c_client_id,
  first_visit
  FROM 
  `wego-cloud.wego_analytics.clients`
  WHERE
  DATE(_PARTITIONTIME) = date_sub(current_date(), INTERVAL 1 DAY)) c on s.client_id = c.c_client_id and date(s.created_at) = date(c.c_created_at)
WHERE
  DATE(_PARTITIONTIME) = date_sub(current_date(), INTERVAL 1 DAY)
  AND s.session_id IN(
SELECT
DISTINCT session_id
FROM 
train_event)
{% endraw %}
