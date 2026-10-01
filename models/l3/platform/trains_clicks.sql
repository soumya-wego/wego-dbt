{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : trains_clicks_daily_append
-- Destination: analysis.trains_clicks  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
SELECT
MAX(created_at) as created_at,
* except(t_click_id, created_at, pageview_id, p_pageview_id, p_session_id),
case when transaction_id is not null then 1 else 0 end as conversions_tracked,
case when transaction_id is not null then 15 else 0 end as revenue,
case when transaction_id is not null then 0.20 end as revenue_in_usd,
case when transaction_id is not null then "INR" else null end as revenue_currency
FROM
(SELECT
*
FROM
(SELECT 
created_at,
event.value as click_id,
event.id as pageview_id
FROM
  `wego-cloud.services_genzo.events_actions*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
  AND event.category = "trains_booking") h
  left join 
  (SELECT
  transaction_id,
  conversion_id,
  click_id as t_click_id,
  booking_value
  FROM
  `wego-cloud.services_genzo.provider_conversions*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))) c on h.click_id = c.t_click_id
  left join 
  (SELECT 
  pageview_id as p_pageview_id,
  session_id as p_session_id
  FROM
  `wego-cloud.wego_analytics.pageviews`
  WHERE DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)) p on h.pageview_id = p.p_pageview_id 
  left join 
  (SELECT 
#   created_at,
  session_id,
  device_type,
  site_code,
  user_country_code,
  market,
  channel,
  wg_campaign,
  wg_content,
  wg_medium
  FROM
  `wego-cloud.wego_analytics.sessions`
  WHERE DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)) s on p.p_session_id = s.session_id
  )
  WHERE click_id is not null
  group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
  order by 1
{% endraw %}
