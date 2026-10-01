{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bowh_supplier_channel_availability_stats_append
-- Destination: analysis.bowh_supplier_channel_availability_stats  (unchanged)
-- Schedule   : every mon 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
INSERT INTO analysis.bowh_supplier_channel_availability_stats

WITH base_deduped AS (
  SELECT *,  DATE_TRUNC(DATE(timestamp), WEEK(MONDAY)) AS week_start_date
  FROM `wego-cloud.ib_hotels_supplier_worker.supplier_search_analytics*`
  WHERE _TABLE_SUFFIX  BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
                          AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY session_id, supplier_code, supplier_channel
    ORDER BY timestamp DESC
  ) = 1
),

-- Flatten requested hotels
requested AS (
  SELECT
    week_start_date,
    supplier_code,
    supplier_channel,
    session_id,
    hotel_id
  FROM base_deduped,
  UNNEST(requested_hotel_ids) AS hotel_id
  WHERE requested_hotel_ids IS NOT NULL


),

-- Flatten available hotels
available AS (
  SELECT
  week_start_date,
    supplier_code,
    supplier_channel,
    session_id,
    hotel_id
  FROM base_deduped,
  UNNEST(available_hotel_ids) AS hotel_id
  WHERE available_hotel_ids IS NOT NULL
),

-- Count when a hotel was requested but not available
unavailable_counts AS (
  SELECT
    r.week_start_date,
    r.supplier_code,
    r.supplier_channel,
    r.hotel_id,
    COUNT(DISTINCT r.session_id) AS times_requested_but_not_available
  FROM requested r
  LEFT JOIN available a
    ON r.supplier_code = a.supplier_code
    AND r.supplier_channel = a.supplier_channel
    AND r.hotel_id = a.hotel_id
    AND r.session_id = a.session_id
  WHERE a.hotel_id IS NULL
  GROUP BY 1,2,3,4
),

-- Count when a hotel was available
available_counts AS (
  SELECT
  week_start_date,
    supplier_code,
    supplier_channel,
    hotel_id,
    COUNT(DISTINCT session_id) AS times_available
  FROM available
  GROUP BY 1,2,3,4
),

-- Combine both metrics
combined AS (
  SELECT
  COALESCE(a.week_start_date, u.week_start_date) AS week_start_date,
    COALESCE(a.supplier_code, u.supplier_code) AS supplier_code,
    COALESCE(a.supplier_channel, u.supplier_channel) AS supplier_channel,
    COALESCE(a.hotel_id, u.hotel_id) AS hotel_id,
    IFNULL(u.times_requested_but_not_available, 0) AS times_requested_but_not_available,
    IFNULL(a.times_available, 0) AS times_available
  FROM available_counts a
  FULL OUTER JOIN unavailable_counts u
    ON a.supplier_code = u.supplier_code
    AND a.supplier_channel = u.supplier_channel
    AND a.hotel_id = u.hotel_id
)

-- Save final output
SELECT *
FROM combined
{% endraw %}
