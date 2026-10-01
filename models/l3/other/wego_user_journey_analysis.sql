{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : wego_user_journey_analysis_daily_append
-- Destination: analysis.wego_user_journey_analysis  (unchanged)
-- Schedule   : every day 01:40   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
-- depends_on: {{ source('wego_analytics', 'pageviews') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
-- -- DROP TABLE analysis.wego_user_journey_analysis

-- create table 
-- analysis.wego_user_journey_analysis
-- partition by session_start_date 
-- as
-- WITH
--   pageviews AS (
--   SELECT
--     DISTINCT created_at,
--     client_created_at,
--     pageview_id,
--     session_id,
--     client_id,
--     concat(session_id, client_id) as session_user,
--     event_id,
--     site_code,
--     locale,
--     page_name,
--     page_type,
--     page_subtype,
--     page_url,
--     referrer_url,
--     user_hash,
--     product,
--     advertiser_id,
--     ROW_NUMBER() OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS journey_order
--   FROM
--     `wego-cloud.wego_analytics.pageviews`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),

-- pageviews_flat as 
-- (select
--     session_id,
--     client_id,
--     concat(session_id, client_id) as session_user,
--     first_value(created_at) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS session_start_created_at,
--     first_value(created_at) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at desc) AS session_end_created_at,
--     if(journey_order = 1, page_type, null) as first_page_type,
--     if(journey_order = 1, page_subtype, null) as first_page_subtype,
--     if(journey_order = 2, page_type, null) as second_page_type,
--     if(journey_order = 2, page_subtype, null) as second_page_subtype,
--     if(journey_order = 3, page_type, null) as third_page_type,
--     if(journey_order = 3, page_subtype, null) as third_page_subtype,
--     if(journey_order = 4, page_type, null) as fourth_page_type,
--     if(journey_order = 4, page_subtype, null) as fourth_page_subtype,
--     if(journey_order = 5, page_type, null) as fifth_page_type,
--     if(journey_order = 5, page_subtype, null) as fifth_page_subtype,
--     if(journey_order = 6, page_type, null) as sith_page_type,
--     if(journey_order = 6, page_subtype, null) as sith_page_subtype,
--     if(journey_order = 7, page_type, null) as seventh_page_type,
--     if(journey_order = 7, page_subtype, null) as seventh_page_subtype,
--     if(journey_order = 8, page_type, null) as eighth_page_type,
--     if(journey_order = 8, page_subtype, null) as eighth_page_subtype,
--     if(journey_order = 9, page_type, null) as ninth_page_type,
--     if(journey_order = 9, page_subtype, null) as ninth_page_subtype,
--     if(journey_order = 10, page_type, null) as tenth_page_type,
--     if(journey_order = 10, page_subtype, null) as tenth_page_subtype,
--     last_value(page_type) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS last_page_type,
--     last_value(page_subtype) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS last_page_subtype
--     from 
--     pageviews),

-- login as 
-- (select distinct
--     concat(session_id, client_id) as logged_in_user
--     from 
--     pageviews
--     where user_hash is not null),

-- sessions as 
-- (SELECT distinct
--   session_id,
--   client_id,
--   concat(session_id, client_id) as session_user,
--   created_at,
--   client_type,
--   device,
--   device_type,
--   os_type,
--   app_version,
--   device_version,
--   os_version,
--   ip,
--   user_country_code,
--   user_city,
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
--   source,
--   market,
--   app_rt_timestamp,
--   app_rt_medium,
--   app_rt_campaign,
--   app_rt_adgroup
-- FROM
--   `wego-cloud.wego_analytics.sessions`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),

--   clicks as 
-- (
-- select 
-- distinct
-- click_session_id,
-- booking_session_id
-- from
-- (
-- select 
-- distinct
-- session_id,
-- client_id,
-- concat(session_id, client_id) as click_session_id,

-- FROM
--   `wego-cloud.wego_analytics.flights_clicks`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 

--   union all
--   select
--   distinct 
-- session_id,
-- client_id,
-- concat(session_id, client_id) as click_session_id,
-- FROM
--   `wego-cloud.wego_analytics.hotels_clicks`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
-- ) c 
-- left join
--  (
-- select 
-- distinct
-- concat(session_id, client_id) as booking_session_id
-- FROM
--   `wego-cloud.wego_analytics.flights_clicks`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
--   and conversions_tracked > 0

--   union all
--   select
--   distinct 
-- concat(session_id, client_id) as booking_session_id
-- FROM
--   `wego-cloud.wego_analytics.hotels_clicks`
-- WHERE
--   DATE(_PARTITIONTIME) BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
--   and conversions_tracked > 0
-- ) b on c.click_session_id = b.booking_session_id
-- )

--     select
--     p.session_id,
--     p.client_id,
--     p.session_user,
--     s.* except(session_id, client_id, created_at, session_user),
--     click_session_id as handoff_session,
--     booking_session_id as booking_session,
--     logged_in_user as logged_in_session,
--     date(max(session_start_created_at)) as session_start_date,
--     max(session_start_created_at) as session_start_created_at,
--     max(session_end_created_at) as session_end_created_at,
--     max(first_page_type) as first_page_type,
--     max(first_page_subtype) as first_page_subtype,
--     max(second_page_type) as second_page_type,
--     max(second_page_subtype) as second_page_subtype,
--     max(third_page_type) as third_page_type,
--     max(third_page_subtype) as third_page_subtype,
--     max(fourth_page_type) as fourth_page_type,
--     max(fourth_page_subtype) as fourth_page_subtype,
--     max(fifth_page_type) as fifth_page_type,
--     max(fifth_page_subtype) as fifth_page_subtype,
--     max(sith_page_type) as sith_page_type,
--     max(sith_page_subtype) as sith_page_subtype,
--     max(seventh_page_type) as seventh_page_type,
--     max(seventh_page_subtype) as seventh_page_subtype,
--     max(eighth_page_type) as eighth_page_type,
--     max(eighth_page_subtype) as eighth_page_subtype,
--     max(ninth_page_type) as ninth_page_type,
--     max(ninth_page_subtype) as ninth_page_subtype,
--     max(tenth_page_type) as tenth_page_type,
--     max(tenth_page_subtype) as tenth_page_subtype,
--     max(last_page_type) as last_page_type,
--     max(last_page_subtype) as last_page_subtype
--     from 
--     pageviews_flat 
--     p 
--     left join 
--     sessions s on p.session_user = s.session_user and date(p.session_start_created_at) = date(s.created_at)
--     left join 
--     clicks c on p.session_user = c.click_session_id
--     left join 
--     login l on p.session_user = l.logged_in_user
--     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33



-- -- daily append:

WITH
  pageviews AS (
  SELECT
    DISTINCT created_at,
    client_created_at,
    pageview_id,
    session_id,
    client_id,
    concat(session_id, client_id) as session_user,
    event_id,
    site_code,
    locale,
    page_name,
    page_type,
    page_subtype,
    page_url,
    referrer_url,
    user_hash,
    product,
    advertiser_id,
    ROW_NUMBER() OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS journey_order
  FROM
    `wego-cloud.wego_analytics.pageviews`
  WHERE
    DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),

pageviews_flat as 
(select
    session_id,
    client_id,
    concat(session_id, client_id) as session_user,
    first_value(created_at) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS session_start_created_at,
    first_value(created_at) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at desc) AS session_end_created_at,
    if(journey_order = 1, page_type, null) as first_page_type,
    if(journey_order = 1, page_subtype, null) as first_page_subtype,
    if(journey_order = 2, page_type, null) as second_page_type,
    if(journey_order = 2, page_subtype, null) as second_page_subtype,
    if(journey_order = 3, page_type, null) as third_page_type,
    if(journey_order = 3, page_subtype, null) as third_page_subtype,
    if(journey_order = 4, page_type, null) as fourth_page_type,
    if(journey_order = 4, page_subtype, null) as fourth_page_subtype,
    if(journey_order = 5, page_type, null) as fifth_page_type,
    if(journey_order = 5, page_subtype, null) as fifth_page_subtype,
    if(journey_order = 6, page_type, null) as sith_page_type,
    if(journey_order = 6, page_subtype, null) as sith_page_subtype,
    if(journey_order = 7, page_type, null) as seventh_page_type,
    if(journey_order = 7, page_subtype, null) as seventh_page_subtype,
    if(journey_order = 8, page_type, null) as eighth_page_type,
    if(journey_order = 8, page_subtype, null) as eighth_page_subtype,
    if(journey_order = 9, page_type, null) as ninth_page_type,
    if(journey_order = 9, page_subtype, null) as ninth_page_subtype,
    if(journey_order = 10, page_type, null) as tenth_page_type,
    if(journey_order = 10, page_subtype, null) as tenth_page_subtype,
    last_value(page_type) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS last_page_type,
    last_value(page_subtype) OVER(PARTITION BY concat(session_id, client_id) ORDER BY created_at) AS last_page_subtype
    from 
    pageviews),

login as 
(select distinct
    concat(session_id, client_id) as logged_in_user
    from 
    pageviews
    where user_hash is not null),

sessions as 
(SELECT distinct
  session_id,
  client_id,
  concat(session_id, client_id) as session_user,
  created_at,
  client_type,
  device,
  device_type,
  os_type,
  app_version,
  device_version,
  os_version,
  ip,
  user_country_code,
  user_city,
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
  source,
  market,
  app_rt_timestamp,
  app_rt_medium,
  app_rt_campaign,
  app_rt_adgroup
FROM
  `wego-cloud.wego_analytics.sessions`
WHERE
  DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) ),

  clicks as 
(
select 
distinct
click_session_id,
booking_session_id
from
(
select 
distinct
session_id,
client_id,
concat(session_id, client_id) as click_session_id,

FROM
  `wego-cloud.wego_analytics.flights_clicks`
WHERE
  DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 

  union all
  select
  distinct 
session_id,
client_id,
concat(session_id, client_id) as click_session_id,
FROM
  `wego-cloud.wego_analytics.hotels_clicks`
WHERE
  DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
) c 
left join
 (
select 
distinct
concat(session_id, client_id) as booking_session_id
FROM
  `wego-cloud.wego_analytics.flights_clicks`
WHERE
  DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
  and conversions_tracked > 0

  union all
  select
  distinct 
concat(session_id, client_id) as booking_session_id
FROM
  `wego-cloud.wego_analytics.hotels_clicks`
WHERE
  DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
  and conversions_tracked > 0
) b on c.click_session_id = b.booking_session_id
)

    select
    p.session_id,
    p.client_id,
    p.session_user,
    s.* except(session_id, client_id, created_at, session_user),
    click_session_id as handoff_session,
    booking_session_id as booking_session,
    logged_in_user as logged_in_session,
    date(max(session_start_created_at)) as session_start_date,
    max(session_start_created_at) as session_start_created_at,
    max(session_end_created_at) as session_end_created_at,
    max(first_page_type) as first_page_type,
    max(first_page_subtype) as first_page_subtype,
    max(second_page_type) as second_page_type,
    max(second_page_subtype) as second_page_subtype,
    max(third_page_type) as third_page_type,
    max(third_page_subtype) as third_page_subtype,
    max(fourth_page_type) as fourth_page_type,
    max(fourth_page_subtype) as fourth_page_subtype,
    max(fifth_page_type) as fifth_page_type,
    max(fifth_page_subtype) as fifth_page_subtype,
    max(sith_page_type) as sith_page_type,
    max(sith_page_subtype) as sith_page_subtype,
    max(seventh_page_type) as seventh_page_type,
    max(seventh_page_subtype) as seventh_page_subtype,
    max(eighth_page_type) as eighth_page_type,
    max(eighth_page_subtype) as eighth_page_subtype,
    max(ninth_page_type) as ninth_page_type,
    max(ninth_page_subtype) as ninth_page_subtype,
    max(tenth_page_type) as tenth_page_type,
    max(tenth_page_subtype) as tenth_page_subtype,
    max(last_page_type) as last_page_type,
    max(last_page_subtype) as last_page_subtype
    from 
    pageviews_flat 
    p 
    left join 
    sessions s on p.session_user = s.session_user and date(p.session_start_created_at) = date(s.created_at)
    left join 
    clicks c on p.session_user = c.click_session_id
    left join 
    login l on p.session_user = l.logged_in_user
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
{% endraw %}
