{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : new_vs_returning_revenue_cohortM
-- Destination: wego_LTV.new_vs_returning_revenue_cohortm  (unchanged)
-- Schedule   : 3 of month 07:20   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_ltv', 'ltv_daily_revenue') }}
-- depends_on: {{ source('wego_ltv', 'ltv_unique_new_clients_raw') }}
{% raw %}
-- # #BQ table: new_vs_returning_revenue_cohortM

-- SELECT
-- created_at as visit_date,
-- first_visit_date as first_visit_date,
-- DATE_DIFF(created_at, first_visit_date, MONTH) as M,
-- channel, 
-- device_type, 
-- product, 
-- user_country_code,
-- market,
-- sum(finance_revenue_usd) as finance_revenue_usd
-- FROM

-- (SELECT
--  created_at,
--  client_id,
--  first_channel as channel,
--  case when strpos(lower(first_app_version), 'hp') > 0 then 'huawei_preload'
--  	    when strpos(lower(first_app_version), 'sp') > 0 then 'samsung_preload'
--  	    when first_device_type = 'android-app' then 'android_manual_install'
--  	    else first_device_type end device_type,
--  product,
--  first_user_country_code as user_country_code,
--  first_market as market,
--  finance_revenue_usd,
--  revenue_in_usd
--  FROM `wego-cloud.wego_LTV.ltv_daily_revenue`
--  where created_at between DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 MONTH) and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
-- ) as revenue

-- LEFT JOIN

-- (SELECT
--  first_visit_date,
--  client_id
--  FROM `wego-cloud.wego_LTV.ltv_unique_new_clients_raw`
-- ) as clients on revenue.client_id = clients.client_id

-- group by 1, 2, 3, 4, 5, 6, 7, 8

# #BQ table: new_vs_returning_revenue_cohortM

SELECT
created_at as visit_date,
first_visit_date as first_visit_date,
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
sum(conversions_tracked) as tracked_bookings,
sum(finance_revenue_usd) as finance_revenue_usd,
sum(booking_revenue_usd) as booking_revenue_usd,
product,
FROM

(SELECT
 created_at,
 client_id,
 case when strpos(lower(first_app_version), 'hp') > 0 then 'huawei_preload'
 	    when strpos(lower(first_app_version), 'sp') > 0 then 'samsung_preload'
 	    when first_device_type = 'android-app' then 'android_manual_install'
 	    else first_device_type end device_type,
 product,
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
 finance_revenue_usd,
 revenue_in_usd,
 conversions_tracked,
 booking_revenue_usd
 FROM `wego-cloud.wego_LTV.ltv_daily_revenue`
 where created_at between ('2018-01-01') and DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
) as revenue

LEFT JOIN

(SELECT
 first_visit_date,
 client_id
 FROM `wego-cloud.wego_LTV.ltv_unique_new_clients_raw`
) as clients on revenue.client_id = clients.client_id

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 28
{% endraw %}
