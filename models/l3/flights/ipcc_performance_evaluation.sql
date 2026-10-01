{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : ipcc_performance_evaluation
-- Destination: analysis.ipcc_performance_evaluation  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('bow_branded_fares_selected') }}
-- depends_on: {{ source('analysis', 'pricing_fares_analysis') }}
-- depends_on: {{ ref('wego_pageviews_analysis') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
{% raw %}
DELETE FROM `wego-cloud.analysis.ipcc_performance_evaluation`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

INSERT INTO `wego-cloud.analysis.ipcc_performance_evaluation`
WITH
  dd AS (
    SELECT
      DATE(created_at) AS day,
      fare_ipcc AS ipcc,
      validating_carrier AS airline,
      route_airports AS route,
      vendor,
      COUNT(*) AS attempts,
      COUNTIF(
        booking_cancellation_reason IS NULL
        OR booking_cancellation_reason
          NOT IN (
            'ABANDONED', 'DUPLICATED_BOOKING', 'AUTH_DECLINED',
            'FRAUD_DECLINED', 'AUTH_PENDING_CONFIRMATION', 'AUTH_FAILED'))
        AS base,
      COUNTIF(booking_cancellation_reason IS NULL) AS successful_bookings,
      SUM(
        IF(
          booking_cancellation_reason IS NULL,
          COALESCE(total_price_usd, price_in_usd, 0),
          0)) AS success_gmv,
      COUNTIF(
        booking_cancellation_reason IN (
          'SEGMENT_STATUS_NOT_OK', 'OFFLINE', 'MCT_NOT_OK', 'FAILED_TICKETING',
          'FAILED_VALIDATE_BOOKING')) AS fails,
      COUNTIF(
        booking_cancellation_reason IN (
          'SEGMENT_STATUS_NOT_OK', 'OFFLINE', 'MCT_NOT_OK')) AS avail_fails,
      COUNTIF(
        booking_cancellation_reason IN (
          'FAILED_TICKETING', 'FAILED_VALIDATE_BOOKING')) AS tech_fails,
      COUNTIF(fare_ipcc IS NOT NULL) AS switch_base,
      COUNTIF(fare_ipcc IS NOT NULL AND fare_ipcc != ipcc) AS switches,
      SUM(
        IF(
          booking_cancellation_reason IS NULL
            OR booking_cancellation_reason
              NOT IN (
                'ABANDONED', 'DUPLICATED_BOOKING', 'AUTH_DECLINED',
                'FRAUD_DECLINED', 'AUTH_PENDING_CONFIRMATION', 'AUTH_FAILED'),
          COALESCE(total_price_usd, price_in_usd, 0),
          0)) AS gmv_base,
      SUM(
        IF(
          booking_cancellation_reason
            IN (
              'SEGMENT_STATUS_NOT_OK', 'OFFLINE', 'MCT_NOT_OK',
              'FAILED_TICKETING', 'FAILED_VALIDATE_BOOKING'),
          COALESCE(total_price_usd, price_in_usd, 0),
          0)) AS gmv_fail
    FROM `wego-cloud.wego_analytics.flights_bookings`
    WHERE
      _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY))
      AND DATE(created_at)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND CURRENT_DATE()
      AND integration_type IN ('SABRE', 'TRAVELPORT')
      AND fare_ipcc IS NOT NULL
      AND validating_carrier IS NOT NULL
      AND route_airports IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
  ),
  freq AS (
    SELECT
      created_at_date AS day,
      fare_ipcc AS ipcc,
      validating_airline_code AS airline,
      airport_route AS route,
      COUNT(*) AS selections
    FROM `wego-cloud.analysis.bow_branded_fares_selected`
    WHERE
      created_at_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
      AND fare_ipcc IS NOT NULL
      AND validating_airline_code IS NOT NULL
      AND airport_route IS NOT NULL
    GROUP BY 1, 2, 3, 4
  ),
  freq_rt AS (
    SELECT day, route, SUM(selections) AS rt_sel FROM freq GROUP BY 1, 2
  ),
  pv AS (
    SELECT
      REPLACE(
        REGEXP_EXTRACT(
          page_url, r'([0-9a-f]+msr:[a-z0-9_]+:[0-9a-f]+:(?:ss|soo))'),
        '_',
        '.') AS fare_id,
      MAX(IF(page_type = 'flights_fare_comparison', 1, 0)) AS cmp,
      MAX(IF(page_type = 'flights_booking', 1, 0)) AS chk
    FROM `wego-cloud.analysis.wego_pageviews_analysis`
    WHERE
      DATE(created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
      AND event_category = 'flights_fare_unavailable'
      AND page_type IN ('flights_fare_comparison', 'flights_booking')
      AND REGEXP_CONTAINS(page_url, r'msr:[0-9A-Z][^/]*?/[0-9a-f]+msr:')
    GROUP BY 1
  ),
  bowf AS (
    SELECT DISTINCT
      created_at_date AS day,
      fare_ipcc AS ipcc,
      validating_airline_code AS airline,
      airport_route AS route,
      fare_id
    FROM `wego-cloud.analysis.bow_branded_fares_selected`
    WHERE
      created_at_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
      AND fare_ipcc IS NOT NULL
      AND validating_airline_code IS NOT NULL
      AND airport_route IS NOT NULL
      AND fare_id IS NOT NULL
  ),
  prepay AS (
    SELECT
      b.day,
      b.ipcc,
      b.airline,
      b.route,
      COUNTIF(pv.cmp = 1 OR pv.chk = 1) AS prepay_unavail,
      COUNT(*) AS prepay_fares
    FROM bowf b
    LEFT JOIN pv
      ON b.fare_id = pv.fare_id
    GROUP BY 1, 2, 3, 4
  ),
  price_ir AS (
    SELECT
      REPLACE(airport_route, ' - ', '-') AS route,
      bow_fare_ipcc AS ipcc,
      COUNT(DISTINCT IF(price_win > 0, fare_id, NULL)) AS price_win_fares,
      COUNT(DISTINCT IF(click_win > 0, fare_id, NULL)) AS click_win_fares,
      COUNT(DISTINCT fare_id) AS fare_n,
      SUM(click) AS total_clicks
    FROM `wego-cloud.analysis.pricing_fares_analysis`
    WHERE
      created_at BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
      AND bow_fare_ipcc IS NOT NULL
    GROUP BY 1, 2
  ),
  pbar AS (
    SELECT
      SAFE_DIVIDE(
        COUNTIF(
          booking_cancellation_reason IN (
            'SEGMENT_STATUS_NOT_OK', 'OFFLINE', 'MCT_NOT_OK',
            'FAILED_TICKETING', 'FAILED_VALIDATE_BOOKING')),
        COUNTIF(
          booking_cancellation_reason IS NULL
          OR booking_cancellation_reason
            NOT IN (
              'ABANDONED', 'DUPLICATED_BOOKING', 'AUTH_DECLINED',
              'FRAUD_DECLINED', 'AUTH_PENDING_CONFIRMATION', 'AUTH_FAILED')))
        AS p_bar
    FROM `wego-cloud.wego_analytics.flights_bookings`
    WHERE
      _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 100 DAY))
      AND DATE(created_at)
        BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        AND CURRENT_DATE()
      AND integration_type IN ('SABRE', 'TRAVELPORT')
  )
SELECT
  dd.day,
  dd.ipcc,
  dd.airline,
  dd.route,
  dd.vendor,
  f.selections,
  fr.rt_sel AS route_day_selections,
  ROUND(SAFE_DIVIDE(f.selections, fr.rt_sel), 4) AS selection_share,
  pp.prepay_unavail,
  pp.prepay_fares,
  ROUND(SAFE_DIVIDE(pp.prepay_unavail, pp.prepay_fares), 4)
    AS prepay_unavail_rate,
  dd.attempts AS all_booking_id,
  dd.base AS base_denominator,
  dd.successful_bookings,
  ROUND(dd.success_gmv, 0) AS success_gmv,
  SUM(dd.base) OVER (PARTITION BY dd.route, dd.day) AS route_day_base,
  ROUND(
    SAFE_DIVIDE(dd.base, SUM(dd.base) OVER (PARTITION BY dd.route, dd.day)), 4)
    AS route_share_day,
  dd.fails,
  ROUND(SAFE_DIVIDE(dd.fails, dd.base), 4) AS failure_rate,
  dd.avail_fails,
  ROUND(SAFE_DIVIDE(dd.avail_fails, dd.base), 4) AS availability_fail_rate,
  dd.tech_fails,
  ROUND(SAFE_DIVIDE(dd.tech_fails, dd.base), 4) AS technical_fail_rate,
  ROUND(dd.gmv_base, 0) AS gmv_base,
  ROUND(dd.gmv_fail, 0) AS gmv_fail,
  ROUND(SAFE_DIVIDE(dd.gmv_fail, dd.gmv_base), 4) AS gmv_weighted_failure_rate,
  dd.switch_base,
  dd.switches,
  ROUND(SAFE_DIVIDE(dd.switches, dd.switch_base), 4) AS switch_rate,
  price_ir.fare_n AS price_n,
  ROUND(SAFE_DIVIDE(price_ir.price_win_fares, price_ir.fare_n), 4)
    AS price_win_rate,
  ROUND(SAFE_DIVIDE(price_ir.click_win_fares, price_ir.fare_n), 4)
    AS click_win_rate,
  ROUND(p.p_bar, 4) AS p_bar,
  ROUND(
    SQRT(
      SAFE_DIVIDE(
        SAFE_DIVIDE(dd.fails, dd.base) * (1 - SAFE_DIVIDE(dd.fails, dd.base)),
        dd.base)),
    4) AS std_error,
  ROUND(p.p_bar + 3 * SQRT(SAFE_DIVIDE(p.p_bar * (1 - p.p_bar), dd.base)), 4)
    AS ucl_k3,
  SAFE_DIVIDE(dd.fails, dd.base)
    > p.p_bar + 3 * SQRT(SAFE_DIVIDE(p.p_bar * (1 - p.p_bar), dd.base))
    AS above_ucl,
  dd.base >= 30 AS n_ge_30,
  price_ir.total_clicks AS total_clicks
FROM dd
CROSS JOIN pbar p
LEFT JOIN freq f
  ON
    dd.day = f.day
    AND dd.ipcc = f.ipcc
    AND dd.airline = f.airline
    AND dd.route = f.route
LEFT JOIN freq_rt fr
  ON dd.day = fr.day AND dd.route = fr.route
LEFT JOIN prepay pp
  ON
    dd.day = pp.day
    AND dd.ipcc = pp.ipcc
    AND dd.airline = pp.airline
    AND dd.route = pp.route
LEFT JOIN price_ir
  ON dd.ipcc = price_ir.ipcc AND dd.route = price_ir.route;
{% endraw %}
