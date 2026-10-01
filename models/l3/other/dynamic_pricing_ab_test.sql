{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : dynamic_pricing_ab_test
-- Destination: analysis.dynamic_pricing_ab_test  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.dynamic_pricing_ab_test
-- partition by  created_at_date as

WITH
  sessions AS (
    SELECT date(created_at) AS created_at_date, session_id, variant, experiment
    FROM wego_analytics.ab_testing
    WHERE
      TIMESTAMP_TRUNC(created_at, DAY) >= TIMESTAMP("2025-12-01")
        AND LOWER(experiment) LIKE '%dynamic pricing%'
      OR experiment = 'FS690 Dynamic Multi Experiments Auto Pricing v1'
    GROUP BY 1, 2, 3, 4
  ),
  searches AS (
    SELECT
      a.*,
      b.search_id,
      b.trip_type,
      b.trip_category,
      b.site_code,
      b.lead_time,
      concat(b.first_departure_airport_code, "-", b.first_arrival_airport_code)
        AS route_cities,
      concat(b.first_departure_country_code, "-", b.first_arrival_country_code)
        AS route_Countries,
      b.device_type,
      CASE
      WHEN array_length(searched_routes)>1 THEN 'multi_routes'
      ELSE 'one_route' END AS multi_routes_flag
    FROM sessions AS a
    LEFT JOIN
      (
        SELECT *
        FROM `wego-cloud.wego_analytics.flights_searches`
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-12-01")
      ) AS b
      ON a.session_id = b.session_id
  ),
  clicks AS (
    SELECT
      a.*,
      b.click_id,
      b.total_price_usd,
      b.provider_code,
      b.conversions_tracked,
      b.stops
    FROM searches AS a
    LEFT JOIN
      (
        SELECT *
        FROM `wego-cloud.wego_analytics.flights_clicks`
        WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-12-01")
      ) AS b
      ON a.search_id = b.search_id
  ),
  flights_insurance_agg AS (
    SELECT
      _partitiondate,
      session_id,
      booking_id,
      currency_code,
      charged_currency_code,
      min(created_at) AS created_at,
      avg(exchange_rate) AS exchange_rate,
      avg(exchange_rate_usd) AS exchange_rate_usd,
      avg(exchange_rate_charged) AS exchange_rate_charged,
      COUNT(DISTINCT (insurance_ids)) AS total_insurances,
      avg(exchange_rate_gateway_processing_payout)
        AS exchange_rate_gateway_processing_payout,
      avg(exchange_rate_gateway_usd) AS exchange_rate_gateway_usd,
      sum(gateway_rolling_reserve_processing)
        AS gateway_rolling_reserve_processing,
      sum(gateway_rolling_reserve_processing_outstanding)
        AS gateway_rolling_reserve_processing_outstanding,
      sum(gateway_authorisation_fee_processing)
        AS gateway_authorisation_fee_processing,
      sum(gateway_blended_fee_processing) AS gateway_blended_fee_processing,
      sum(gateway_refund_fee_processing) AS gateway_refund_fee_processing,
      sum(gateway_void_fee_processing) AS gateway_void_fee_processing,
      sum(gateway_scheme_fixed_fee_processing)
        AS gateway_scheme_fixed_fee_processing,
      sum(gateway_scheme_variable_fee_processing)
        AS gateway_scheme_variable_fee_processing,
      sum(gateway_premium_fee_processing) AS gateway_premium_fee_processing,
      sum(gateway_scheme_ic_processing) AS gateway_scheme_ic_processing,
      sum(gateway_authorisation_fee_tax_processing)
        AS gateway_authorisation_fee_tax_processing,
      sum(gateway_blended_fee_tax_processing)
        AS gateway_blended_fee_tax_processing,
      sum(gateway_refund_fee_tax_processing)
        AS gateway_refund_fee_tax_processing,
      sum(gateway_void_fee_tax_processing) AS gateway_void_fee_tax_processing,
      sum(gateway_scheme_fixed_fee_tax_processing)
        AS gateway_scheme_fixed_fee_tax_processing,
      sum(gateway_scheme_variable_fee_tax_processing)
        AS gateway_scheme_variable_fee_tax_processing,
      sum(gateway_premium_fee_tax_processing)
        AS gateway_premium_fee_tax_processing,
      sum(gateway_scheme_ic_tax_processing) AS gateway_scheme_ic_tax_processing,
      sum(gateway_rolling_reserve_payout) AS gateway_rolling_reserve_payout,
      sum(gateway_rolling_reserve_payout_outstanding)
        AS gateway_rolling_reserve_payout_outstanding,
      sum(gateway_authorisation_fee_payout) AS gateway_authorisation_fee_payout,
      sum(gateway_blended_fee_payout) AS gateway_blended_fee_payout,
      sum(gateway_refund_fee_payout) AS gateway_refund_fee_payout,
      sum(gateway_void_fee_payout) AS gateway_void_fee_payout,
      sum(gateway_scheme_fixed_fee_payout) AS gateway_scheme_fixed_fee_payout,
      sum(gateway_scheme_variable_fee_payout)
        AS gateway_scheme_variable_fee_payout,
      sum(gateway_premium_fee_payout) AS gateway_premium_fee_payout,
      sum(gateway_scheme_ic_payout) AS gateway_scheme_ic_payout,
      sum(gateway_authorisation_fee_tax_payout)
        AS gateway_authorisation_fee_tax_payout,
      sum(gateway_blended_fee_tax_payout) AS gateway_blended_fee_tax_payout,
      sum(gateway_refund_fee_tax_payout) AS gateway_refund_fee_tax_payout,
      sum(gateway_void_fee_tax_payout) AS gateway_void_fee_tax_payout,
      sum(gateway_scheme_fixed_fee_tax_payout)
        AS gateway_scheme_fixed_fee_tax_payout,
      sum(gateway_scheme_variable_fee_tax_payout)
        AS gateway_scheme_variable_fee_tax_payout,
      sum(gateway_premium_fee_tax_payout) AS gateway_premium_fee_tax_payout,
      sum(gateway_scheme_ic_tax_payout) AS gateway_scheme_ic_tax_payout,
      sum(gateway_rolling_reserve_usd) AS gateway_rolling_reserve_usd,
      sum(gateway_rolling_reserve_outstanding_usd)
        AS gateway_rolling_reserve_outstanding_usd,
      sum(gateway_authorisation_fee_usd) AS gateway_authorisation_fee_usd,
      sum(gateway_blended_fee_usd) AS gateway_blended_fee_usd,
      sum(gateway_refund_fee_usd) AS gateway_refund_fee_usd,
      sum(gateway_void_fee_usd) AS gateway_void_fee_usd,
      sum(gateway_scheme_fixed_fee_usd) AS gateway_scheme_fixed_fee_usd,
      sum(gateway_scheme_variable_fee_usd) AS gateway_scheme_variable_fee_usd,
      sum(gateway_premium_fee_usd) AS gateway_premium_fee_usd,
      sum(gateway_scheme_ic_usd) AS gateway_scheme_ic_usd,
      sum(est_total_gateway_fees_usd) AS est_total_gateway_fees_usd,
      sum(total_gateway_fees_usd) AS total_gateway_fees_usd,
      sum(blended_total_gateway_fees_usd) AS blended_total_gateway_fees_usd,
      sum(cost_of_sales_usd) AS cost_of_sales_usd,
      sum(base_amount) AS base_amount,
      sum(tax_amount) AS tax_amount,
      sum(total_amount) AS total_amount,
      sum(total_price) AS total_price,
      sum(price_in_usd) AS price_in_usd,
      sum(total_price_usd) AS total_price_usd,
      sum(gross_revenue) AS gross_revenue,
      sum(revenue) AS revenue,
      sum(revenue_in_usd) AS revenue_in_usd,
      sum(finance_revenue_usd) AS finance_revenue_usd,
      sum(booking_value_usd) AS booking_value_usd,
      sum(booking_value_adjusted) AS booking_value_adjusted,
      sum(booking_revenue_usd) AS booking_revenue_usd,
      sum(gross_revenue_in_usd) AS gross_revenue_in_usd
    FROM
      `wego-cloud.wego_analytics.flights_insurance`
    GROUP BY 1, 2, 3, 4, 5
  ),
  flights_bookings AS (
    SELECT
      a.click_id,
      a.booking_id,
      a.total_price_usd AS booking_value_usd,
      a.finance_revenue_usd,
      COALESCE(
        a.finance_revenue_usd + ifnull(
          (
            IF(
              (DATE(flights_insurance_agg.created_at)) > '2022-08-18'
                AND flights_insurance_agg.charged_currency_code = "SAR",
              flights_insurance_agg.finance_revenue_usd
                + flights_insurance_agg.blended_total_gateway_fees_usd
                - flights_insurance_agg.est_total_gateway_fees_usd,
              flights_insurance_agg.finance_revenue_usd)),
          0)) AS net_margin_usd,
      a.gross_revenue_in_usd + ifnull(
        flights_insurance_agg.gross_revenue_in_usd, 0) AS gross_margin_usd,
      coalesce(ancillary_seats_revenue_usd, 0)
        + coalesce(ancillary_meals_revenue_usd, 0)
        + coalesce(ancillary_baggage_revenue_usd, 0) AS ancillary_revenue_usd,

      coalesce(a.payment_fee_usd,0) as payment_fee_usd   
    FROM
      `wego-cloud.wego_analytics.flights_bookings`
        AS a
    LEFT JOIN flights_insurance_agg
      ON a.booking_id = flights_insurance_agg.booking_id
    WHERE (a.conversions_adjusted) = 1 AND a._PARTITIONDATE >= "2025-12-01"
  ),
  final_table AS (
    SELECT a.*, b.* EXCEPT (click_id)
    FROM clicks AS a
    LEFT JOIN flights_bookings AS b
      ON a.click_id = b.click_id
  ),
  base_table AS (
    SELECT
      *
    FROM final_table
    WHERE created_at_date = current_date - 1
  ),
  dim AS (
    SELECT
      concat(
        flights_pricing_fares_analysis_v2.first_departure_airport_code,
        "-",
        flights_pricing_fares_analysis_v2.first_arrival_airport_code)
        AS airport_route,
      flights_pricing_fares_analysis_v2.first_airline AS airline,
      flights_pricing_fares_analysis_v2.site_code AS site_code,
      COALESCE(SUM(flights_pricing_fares_analysis_v2.click), 0) AS total_clicks,
      sum(COALESCE(SUM(flights_pricing_fares_analysis_v2.click), 0))
        OVER (
          ORDER BY
            COALESCE(SUM(flights_pricing_fares_analysis_v2.click), 0) DESC
        ) AS cum_sum,
      sum(COALESCE(SUM(flights_pricing_fares_analysis_v2.click), 0))
        OVER () AS total
    FROM
      `wego-cloud.analysis.pricing_fares_analysis`
        AS flights_pricing_fares_analysis_v2
    WHERE
      (
        (
          (
            (flights_pricing_fares_analysis_v2.created_at)
              >= (DATE('2026-01-01'))
            AND (flights_pricing_fares_analysis_v2.created_at)
              < (DATE('2026-06-30')))))
      AND provider_code = 'wego.com'
      AND providers_status = 'multiple providers'
      AND trip_type = 'oneway'
      AND first_stop_count = 0
      AND bow_original_total_fare_usd IS NOT NULL
    GROUP BY
      1,
      2,
      3
    ORDER BY 4 DESC
  )
SELECT
  a.* EXCEPT (multi_routes_flag),
  CASE
    WHEN
      b.site_code IS NOT NULL
      AND trip_type = "oneway"
      --AND lower(stops) = "direct"
      THEN 1
    ELSE 0
    END AS autopricing_scope,
    a.multi_routes_flag
FROM base_table AS a
LEFT JOIN
  (
    SELECT airport_route, site_code
    FROM dim
    WHERE cum_sum / total <= 0.60
    GROUP BY 1, 2
  ) AS b
  ON
    a.route_cities = b.airport_route
    AND a.site_code = b.site_code;
{% endraw %}
