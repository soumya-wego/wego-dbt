{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_searches_funnel_p2
-- Destination: analysis.hotels_searches_funnel  (unchanged)
-- Schedule   : every day 02:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
{% raw %}
MERGE `wego-cloud.analysis.hotels_searches_funnel` AS target
USING (
  WITH paid_booking AS (
    SELECT
      session_id,
      MIN(created_at) AS created_at
    FROM `wego-cloud.wego_analytics.hotels_bookings`
    WHERE conversions_adjusted > 0
      AND DATE(created_at) >= DATE_SUB(@run_date, INTERVAL 90 DAY)
    GROUP BY session_id
  ),

  searches_to_evaluate AS (
    SELECT
      s.search_id,
      pb.session_id AS matched_session,
      pb.created_at AS new_booking_time
    FROM `wego-cloud.analysis.hotels_searches_funnel` s
    LEFT JOIN paid_booking pb ON s.session_id = pb.session_id
    WHERE
      DATE(s.created_at) >= DATE_SUB(@run_date, INTERVAL 30 DAY)
      OR (s.paid_flag = 1 AND DATE(s.created_at) >= DATE_SUB(@run_date, INTERVAL 90 DAY))
  )

  SELECT
    search_id,
    MAX(CASE WHEN matched_session IS NOT NULL THEN 1 ELSE 0 END) AS new_paid_flag,
    MAX(new_booking_time) AS new_booking_time
  FROM searches_to_evaluate
  GROUP BY search_id
) AS source
ON target.search_id = source.search_id
WHEN MATCHED AND target.paid_flag != source.new_paid_flag THEN
  UPDATE SET
    paid_flag = source.new_paid_flag,
    booking_time = source.new_booking_time,
    booking_count = NULL
{% endraw %}
