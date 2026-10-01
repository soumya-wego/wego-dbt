{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : ltv_unique_new_clients_raw
-- Destination: wego_LTV.ltv_unique_new_clients_raw  (unchanged)
-- Schedule   : 3 of month 07:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'clients') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
#BQ table: ltv_unique_new_clients_raw
#table to generate historical distinct client_ids
#other tables can join to this table to derive the first_visit_date
#take records after 2018-01-01 to eliminate wegonomics/genzo switchover noise

-- updated 15-03-2022
-- original:
-- SELECT
-- clients.*,
-- first_session.* EXCEPT (client_id)
-- test

-- FROM
-- (select
--  client_id,
--  date(created_at) as first_visit_date,
--  date(ua_timestamp) as install_date,
--  ua_source,
--  ua_medium,
--  ua_campaign
--  FROM `wego-cloud.wego_analytics.clients`
--  where date(_partitiontime) between ('2017-05-01') and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
--    and first_visit = true) as clients

-- left join
-- (SELECT * FROM
--  (SELECT 
--   client_id, 
--   # take only the first session device_type, os_type, channel, app_version, user_country_code, market of the user over their lifetime instead of the actual returning session
--   FIRST_VALUE(device_type) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_device_type, 
--   FIRST_VALUE(os_type) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_os_type, 
--   FIRST_VALUE(channel) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_channel, 
--   FIRST_VALUE(app_version) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_version,
--   FIRST_VALUE(user_country_code) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_user_country_code, 
--   FIRST_VALUE(market) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_market
--   FROM `wego-cloud.wego_analytics.sessions`
--   where date(_partitiontime) between ('2017-05-01') and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
--  )
--  group by 1, 2, 3, 4, 5, 6, 7
-- ) as first_session on clients.client_id = first_session.client_id

-- updated:
SELECT
clients.*,
 case when strpos(lower(first_app_version), 'hp') > 0 then 'huawei_preload'
 	    when strpos(lower(first_app_version), 'sp') > 0 then 'samsung_preload'
 	    when first_device_type = 'android-app' then 'android_manual_install'
 	    else first_device_type end first_device_type,
first_session.* EXCEPT (client_id,first_device_type)
FROM
(select
 client_id,
 date(created_at) as first_visit_date,
 date(ua_timestamp) as install_date,
 ua_source,
 ua_medium,
 ua_campaign
 FROM `wego-cloud.wego_analytics.clients`
 where date(_partitiontime) between ('2017-05-01') and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
   and first_visit = true) as clients

left join
(SELECT * FROM
 (SELECT 
  client_id, 
  # take only the first session device_type, os_type, channel, app_version, user_country_code, market of the user over their lifetime instead of the actual returning session
  FIRST_VALUE(device_type) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_device_type, 
  FIRST_VALUE(os_type) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_os_type, 
  FIRST_VALUE(channel) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_channel, 
  FIRST_VALUE(source) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_source, 
  FIRST_VALUE(app_version) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_version,
  FIRST_VALUE(user_country_code) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_user_country_code, 
  FIRST_VALUE(market) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_market,
  FIRST_VALUE(site_code) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_site_code,
  FIRST_VALUE(locale) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_locale,
  FIRST_VALUE(wg_source) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_wg_source,
  FIRST_VALUE(wg_medium) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_wg_medium,
  FIRST_VALUE(wg_campaign) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_wg_campaign,
  FIRST_VALUE(wg_adgroup) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_wg_adgroup,
  FIRST_VALUE(app_rt_source) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_rt_source,
  FIRST_VALUE(app_rt_medium) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_rt_medium,
  FIRST_VALUE(app_rt_campaign) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_rt_campaign,
  FIRST_VALUE(app_rt_adgroup) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_app_rt_adgroup,
  FIRST_VALUE(ts_code) OVER (PARTITION BY client_id ORDER BY created_at asc) as first_ts_code,
  FROM `wego-cloud.wego_analytics.sessions`
  where date(_partitiontime) between ('2017-05-01') and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
 )
 group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
) as first_session on clients.client_id = first_session.client_id
{% endraw %}
