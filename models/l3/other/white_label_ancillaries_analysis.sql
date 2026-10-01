{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : white_label_ancillaries_analysis_daily_append
-- Destination: analysis.white_label_ancillaries_analysis  (unchanged)
-- Schedule   : every day 09:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.white_label_ancillaries_analysis
-- partition by date
-- as

SELECT
  DATE(p.created_at) AS date,
  user_country_code,
  site_code,
  locale,
  channel,
  device_type,
  `source`,
  ts_code,
  -- app_version,
  wg_source,
  wg_medium,
  wg_campaign,
  -- session_landing_url,
  new_vs_returning_status,
  if(user_hash is not null, "logged in", "not logged in") as login_status,
  if(event_category = "products", event_value, null) as product,
  conversion_id as provider_code,
  COUNT(DISTINCT session_id) AS sessions,
  COUNT(DISTINCT if(event_category = "products", session_id, null)) AS clicked_sessions,
  COUNT(DISTINCT if(conversion_id is not null, session_id, null)) AS booked_sessions,
  COUNT(DISTINCT client_id) AS users,
  COUNT(distinct transaction_id) AS bookings,
  SUM(commission * amount) AS commission_usd,
  AVG(commission * amount) AS avg_commission_usd,
  SUM(booking_value * amount) AS gmv_usd,
  AVG(booking_value * amount) AS abv_usd
FROM
  `wego-cloud.analysis.wego_pageviews_analysis` p
LEFT JOIN (
  SELECT
  * EXCEPT(row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY created_at DESC) AS row_num
  FROM
    `wego-cloud.services_genzo.provider_conversions*`
  WHERE
    _TABLE_SUFFIX between (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 3 day))))
    and (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day)))) )
WHERE
  row_num = 1
    ) AS c
ON
  p.event_action_id = c.click_id
LEFT JOIN
  (select * 
  from `wego-cloud.analytics.exchange_rates*` 
  where _TABLE_SUFFIX between (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 3 day))))
    and (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))) e
ON
  e.base = c.comm_currency_code
  AND DATE(c.created_at) = CAST(e.effective AS date)
WHERE
  TIMESTAMP_TRUNC(p.created_at, DAY) = TIMESTAMP(date_sub(current_date(), interval 1 day))
  and p.event_object = "section"
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
{% endraw %}
