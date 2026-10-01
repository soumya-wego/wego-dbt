{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : dynamic_pricing_markup_analysis
-- Destination: analysis.dynamic_pricing_markup_markdown_analysis  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_curiosity', 'dynamic_pricing_adjustments') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
{% raw %}
INSERT INTO `wego-cloud.analysis.dynamic_pricing_markup_markdown_analysis`
WITH
dynamic_pricing_adjustment AS(
  SELECT
  * EXCEPT(action),
  CASE
  WHEN (action IS NULL AND delta_amount_usd > 0) THEN 'markup - keep the position'
  ELSE action END AS action
  FROM `wego-cloud.services_curiosity.dynamic_pricing_adjustments*`
  WHERE
  _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bow_fare_id ORDER BY poll_count DESC) = 1
),
flight_click AS(
  SELECT
  created_at,
  search_id,
  click_id,
  COALESCE(COUNT(DISTINCT IF(conversions_tracked IS NOT NULL,click_id,NULL)),0) AS total_clicks,
  COALESCE(SUM(conversions_tracked),0) AS conversion_click
  FROM `wego-cloud.wego_analytics.flights_clicks` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = DATE_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY)
  GROUP BY 1,2,3
),
flight_booking AS(
  SELECT
  a.created_at,
  a.search_id,
  a.click_id,
  booking_id,
  ms_fare_id,
  integration_type,
  booking_type,
  fare_type,
  route_airports,
  route_cities,
  route_countries,
  departure_city_code,
  departure_airport_code,
  departure_country_code,
  arrival_city_code,
  arrival_airport_code,
  arrival_country_code,
  airline,
  ipcc AS booking_ipcc,
  fare_ipcc,
  total_clicks,
  conversion_click,
  finance_revenue_usd,
  gross_revenue_in_usd,
  total_price_usd,
  SUM(conversions_adjusted) AS total_booking
  FROM flight_click a
  LEFT JOIN
  `wego-cloud.wego_analytics.flights_bookings` b
  ON
  a.click_id=b.click_id
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = DATE_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 1 DAY)
  AND conversions_adjusted IS NOT NULL
  AND conversions_tracked=1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
),
flight_insurance AS (
    SELECT
      DATE(created_at) AS date_key,
      booking_id,
      SUM(IF(
        charged_currency_code = "SAR",
        finance_revenue_usd
          + blended_total_gateway_fees_usd
          - est_total_gateway_fees_usd,
        finance_revenue_usd))
        insurance_net_margin_usd,
      SUM(gross_revenue_in_usd) AS insurance_gross_revenue_in_usd
    FROM `wego-cloud.wego_analytics.flights_insurance`
    WHERE
      TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
     GROUP BY 1,2
),
final_booking AS(
  SELECT
  a.*,
  b.insurance_net_margin_usd,
  b.insurance_gross_revenue_in_usd,
  finance_revenue_usd + IFNULL(insurance_net_margin_usd, 0) AS total_net_margin,
  gross_revenue_in_usd + IFNULL(insurance_gross_revenue_in_usd, 0)
    AS total_gross_margin,
  SAFE_DIVIDE(
    (finance_revenue_usd + IFNULL(insurance_net_margin_usd, 0)),
    total_price_usd) AS net_margin_percentage,
  SAFE_DIVIDE(
    (gross_revenue_in_usd + IFNULL(insurance_gross_revenue_in_usd, 0)),
    total_price_usd) AS gross_margin_percentage
  FROM
  flight_booking a
  LEFT JOIN
  flight_insurance b
  ON
  a.booking_id=b.booking_id
)
SELECT
    a.search_id,
    flight_id,
    bow_fare_id,
    vendor_amount_usd,
    original_amount_usd,
    ref_fare_id,
    ref_amount_usd,
    delta_amount_usd,
    poll_count,
    position,
    all_meta_fares_amount_usd,
    rule_id,
    site_code,
    priority,
    target_position,
    current_position,
    achieved_position,
    action,
    markdown_cap_triggered,
    markup_cap_triggered,
    recovery_markup_applied,
    COALESCE(final_amount_usd,total_price_usd) AS final_amount_usd,
    adjustment_percent,
    a.created_at,
    booking_id,
    integration_type,
    a.booking_type,
    fare_type,
    route_airports,
    route_cities,
    route_countries,
    departure_city_code,
    departure_airport_code,
    departure_country_code,
    arrival_city_code,
    arrival_airport_code,
    arrival_country_code,
    airline,
    booking_ipcc,
    fare_ipcc,
    total_clicks,
    conversion_click,
    finance_revenue_usd,
    gross_revenue_in_usd,
    total_net_margin,
    total_gross_margin,
    net_margin_percentage,
    gross_margin_percentage,
    total_booking,
    total_price_usd,
    CASE
    WHEN b.bow_fare_id IS NULL THEN 'dynamic pricing not applied'
    ELSE 'dynamic pricing applied'
    END AS dynamic_pricing_applied
FROM
final_booking a
LEFT JOIN
(SELECT
  *
  FROM
  dynamic_pricing_adjustment
  WHERE action IS NOT NULL) b
ON
a.click_id=b.bow_fare_id
{% endraw %}
