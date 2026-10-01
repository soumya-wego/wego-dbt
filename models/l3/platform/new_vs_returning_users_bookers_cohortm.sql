{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : new_vs_returning_users_bookers_cohortM
-- Destination: wego_LTV.new_vs_returning_users_bookers_cohortm  (unchanged)
-- Schedule   : 3 of month 07:20   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- #BQ table: new_vs_returning_sessions_bookers_cohortM

-- SELECT
-- created_at as visit_date,
-- first_visit_date,
-- DATE_DIFF(created_at, first_visit_date, MONTH) as M,
-- channel, 
-- device_type,
-- user_country_code,
-- market,
-- sum(1) as sessions

-- FROM

-- (SELECT
--  created_at,
--  first_visit_date,
--  client_id,
--  session_id,
--  case when strpos(lower(first_app_version), 'hp') > 0 then 'huawei_preload'
-- 	  when strpos(lower(first_app_version), 'sp') > 0 then 'samsung_preload'
-- 	  when first_device_type = 'android-app' then 'android_manual_install'
-- 	  else first_device_type end device_type,
--  first_channel as channel,
--  first_user_country_code as user_country_code,
--  first_market as market
--  FROM `wego-cloud.wego_LTV.ltv_daily_sessions_bookers`
--  where created_at between DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
-- ) as session

-- group by 1, 2, 3, 4, 5, 6, 7

#BQ table: new_vs_returning_sessions_bookers_cohortM

SELECT
session.client_id as client_id,
created_at as visit_date,
first_visit_date,
DATE_DIFF(created_at, first_visit_date, MONTH) as M,
device_type, 
os_type,
channel,
source,
app_version,
user_country_code,
market,
site_code,
locale,
ua_source,
ua_medium,
ua_campaign,
wg_source,
wg_medium,
wg_campaign,
wg_adgroup,
app_rt_source,
app_rt_medium,
app_rt_campaign,
app_rt_adgroup,
ts_code,
sum(1) as users

FROM

(SELECT
 created_at,
 first_visit_date,
 client_id,
 session_id,
 case when strpos(lower(first_app_version), 'hp') > 0 then 'huawei_preload'
	  when strpos(lower(first_app_version), 'sp') > 0 then 'samsung_preload'
	  when first_device_type = 'android-app' then 'android_manual_install'
	  else first_device_type end device_type,
first_os_type as os_type,
first_channel as channel,
first_source as source,
first_app_version as app_version,
first_user_country_code as user_country_code,
first_market as market,
first_site_code as site_code,
first_locale as locale,
ua_source,
ua_medium,
ua_campaign,
first_wg_source as wg_source,
first_wg_medium as wg_medium,
first_wg_campaign as wg_campaign,
first_wg_adgroup as wg_adgroup,
first_app_rt_source as app_rt_source,
first_app_rt_medium as app_rt_medium,
first_app_rt_campaign as app_rt_campaign,
first_app_rt_adgroup as app_rt_adgroup,
first_ts_code as ts_code,
 FROM `wego-cloud.wego_LTV.ltv_daily_sessions_bookers`
 where created_at between ("2018-01-01") and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
) as session

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
{% endraw %}
