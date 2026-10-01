{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : rtbhouse_conversion_report
-- Destination: marketing_analytics.rtbhouse_conversion_report  (unchanged)
-- Schedule   : every day 03:33   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
--  Delete only the partitions we are reprocessing
DELETE FROM `wego-cloud.marketing_analytics.rtbhouse_conversion_report`
WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY)
              AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- Reinsert fresh data for those partitions
INSERT INTO `wego-cloud.marketing_analytics.rtbhouse_conversion_report`

WITH ss AS
   (SELECT
    DATE(engagement_timestamp) AS date,
    wg_source, wg_campaign, device_type, 
    COUNT(DISTINCT IF(source IN ('install','re-engagement') AND LEFT(session_id,7) IN ('app_rt-','app_ua-'),SUBSTR(session_id, STRPOS(session_id, '-wg-') + 4),session_id)) AS sessions
  FROM `wego-cloud.marketing_analytics.sessions`
  WHERE DATE(engagement_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY)
                                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND LOWER(wg_source) LIKE '%rtbhouse%'
    GROUP BY ALL
   ),

attr AS
  (SELECT
    DATE(engagement_date) AS date,
    wg_source, wg_campaign, device_type,
    SUM(conversions_tracked_U) as conversions,
    SUM(finance_revenue_usd_U) AS conversion_value
  FROM `wego-cloud.marketing_analytics.attribution`
  WHERE DATE(engagement_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 38 DAY)
                                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND product IN ('flights_bow', 'hotels_bow')
    AND LOWER(wg_source) LIKE '%rtbhouse%'
    GROUP BY ALL
  )

SELECT ss.date as date, ss.wg_source, ss.wg_campaign, ss.device_type, sessions, conversions, conversion_value
FROM ss LEFT JOIN attr
ON ss.date=attr.date  
AND ss.wg_source = attr.wg_source
AND ss.wg_campaign = attr.wg_campaign
AND ss.device_type = attr.device_type;
{% endraw %}
