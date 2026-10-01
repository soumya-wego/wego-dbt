{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : goreward_coverage_daily
-- Destination: analysis.goreward_coverage_daily  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
DELETE FROM `wego-cloud.analysis.goreward_coverage_daily`
WHERE snapshot_date = CURRENT_DATE();

INSERT INTO `wego-cloud.analysis.goreward_coverage_daily`

WITH booked AS (
  SELECT CAST(hotel_id AS STRING) AS hotel_id, COUNT(*) AS bookings
  FROM `wego-cloud.wego_analytics.hotels_bookings`
  WHERE DATE(booking_at) >= '2026-01-01'
    AND conversions_adjusted = 1
  GROUP BY 1
),
ranked AS (
  SELECT hotel_id, NTILE(10) OVER (ORDER BY bookings DESC) AS decile
  FROM booked
),
top10 AS (
  SELECT hotel_id FROM ranked WHERE decile = 1
),
todays_list AS (
  SELECT hotel_id, loyalty_tier
  FROM `wego-cloud.analysis.goreward_hotel_list_v1`
  WHERE snapshot_date = CURRENT_DATE()
)
SELECT
  CURRENT_DATE()                                                                          AS snapshot_date,
  (SELECT COUNT(*) FROM todays_list)                                                      AS total_hotels,
  COUNT(DISTINCT t.hotel_id)                                                              AS top10_total,
  COUNT(DISTINCT l.hotel_id)                                                              AS top10_in_list,
  ROUND(COUNT(DISTINCT l.hotel_id) / COUNT(DISTINCT t.hotel_id) * 100, 1)                AS top10_coverage_pct,
  COUNTIF(l.loyalty_tier = 'Tier 1 (>=15%)')                                             AS tier1,
  COUNTIF(l.loyalty_tier = 'Tier 2 (>=10%)')                                             AS tier2,
  COUNTIF(l.loyalty_tier = 'Tier 3 (>=5%)')                                              AS tier3,
  COUNTIF(l.loyalty_tier = 'Cheaper only (<5%)')                                         AS cheaper_only
FROM top10 t
LEFT JOIN todays_list l USING (hotel_id)
{% endraw %}
