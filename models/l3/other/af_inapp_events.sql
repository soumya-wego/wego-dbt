{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Apple Search Ads AF InApp Flattening
-- Destination: apple_search_ad.af_inapp_events  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
(SELECT
DATE(TIMESTAMP_ADD(inapp.event_time,INTERVAL 4 HOUR)) event_date_GST,
campaign,
campaign_id,
adset_id,
adset_name,
keywords,
COUNT(CASE WHEN event_name='start_session' THEN event_time END) AS start_session_events,
COUNT(CASE WHEN event_name IN ('af_add_to_cart','af_search') THEN event_time END) AS search_events,
COUNT(CASE WHEN event_name IN ('af_content_view') THEN event_time END) AS details_pageview_events,
COUNT(CASE WHEN event_name IN ('af_purchase') THEN event_time END) AS handoff_events,
COUNT(CASE WHEN event_name IN ('af_achievement_unlocked') THEN event_time END) AS booking_events,
COUNT(CASE WHEN event_name IN ('af_travel_booking') THEN event_time END) AS bow_events,
SUM(CASE WHEN event_name IN ('af_purchase') THEN event_revenue_usd END) AS handoff_revenue,
SUM(CASE WHEN event_name IN ('af_achievement_unlocked') THEN event_revenue_usd END) AS booking_revenue,
SUM(CASE WHEN event_name IN ('af_travel_booking') THEN event_revenue_usd END) AS bow_revenue

FROM `wego-cloud.external_appsflyer.inapp*` inapp
WHERE media_source='Apple Search Ads'
AND (
_TABLE_SUFFIX LIKE CONCAT('reattrReport',FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
OR 
_TABLE_SUFFIX LIKE FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
AND event_name IN ('start_session','af_add_to_cart','af_content_view','af_purchase','af_travel_booking','af_search','af_achievement_unlocked')
GROUP BY
1,2,3,4,5,6)
{% endraw %}
