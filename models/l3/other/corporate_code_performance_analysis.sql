{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : corporate_code_performance_analysis
-- Destination: analysis.corporate_code_performance_analysis  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('integrated_bookings_flights', 'branded_fares') }}
-- depends_on: {{ source('integrated_bookings_flights', 'itineraries') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'provider_fares') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'flights_searches') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
INSERT INTO `wego-cloud.analysis.corporate_code_performance_analysis` 
WITH
fares AS (
  SELECT
    DATE(created_at) AS date_key,
    search_id,
    extra_info.fare_ipcc,
    extra_info.account_code,
    extra_info.fare_type,
    CASE
      WHEN extra_info.account_code IS NULL THEN 'NO ACCOUNT CODE'
      WHEN extra_info.account_code IS NOT NULL THEN 'KNOWN CORPORATE CODE'
      ELSE 'UNKNOWN CODE'
    END AS account_code_type,
    id
  FROM `wego-cloud.services_curiosity.provider_fares*`
  WHERE _TABLE_SUFFIX = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
),
branded_fares AS(
  SELECT
    calc.booking_ipcc AS revalidation_ipcc,
    calc.search_id,
    bf.account_code,
    CASE
      WHEN bf.account_code IS NULL THEN 'NO ACCOUNT CODE'
      WHEN bf.account_code IS NOT NULL THEN 'KNOWN CORPORATE CODE'
      ELSE 'UNKNOWN CODE'
    END AS account_code_type,
    bf.id
  FROM `wego-cloud.integrated_bookings_flights.branded_fares*` bf
  INNER JOIN `wego-cloud.services_curiosity.branded_fare_calculations*` calc
    ON bf.id = calc.branded_fare_id
    AND bf.ms_fare_id = calc.fare_id
  WHERE 
  bf._TABLE_SUFFIX = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  AND calc._TABLE_SUFFIX = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
  AND (bf.selected = TRUE OR bf.selected IS NULL)
  AND bf.endpoint = 'COMPARE'
  
),
booking AS(
  SELECT
    ipcc,
    account_code,
    CASE
      WHEN account_code IS NULL THEN 'NO ACCOUNT CODE'
      WHEN account_code IS NOT NULL THEN 'KNOWN CORPORATE CODE'
      ELSE 'UNKNOWN CODE'
    END AS account_code_type,
    id,
    booking_id
  FROM `wego-cloud.integrated_bookings_flights.itineraries*`
  WHERE _TABLE_SUFFIX = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
),
ticketing AS(
  SELECT
    i.ipcc,
    i.validating_carrier_code,
    i.account_code,
    CASE
      WHEN i.account_code IS NULL THEN 'NO ACCOUNT CODE'
      WHEN i.account_code IS NOT NULL THEN 'KNOWN CORPORATE CODE'
      ELSE 'UNKNOWN CODE'
    END AS account_code_type,
    i.id,
    booking_id,
    itinerary_status
  FROM `wego-cloud.integrated_bookings_flights.itineraries*` i
  WHERE _TABLE_SUFFIX = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
    AND itinerary_status = 'TICKETED'
),
search_revalidation AS(
  SELECT
  DISTINCT
  date_key,
  a.search_id,
  fare_ipcc,
  a.account_code_type AS account_code_type_fare,
  a.account_code AS account_code_fare,
  revalidation_ipcc,
  b.account_code_type AS account_code_type_branded_fare,
  b.account_code AS account_code_branded_fare
  FROM
  fares a
  LEFT JOIN
  branded_fares b
  ON
  a.search_id=b.search_id
),
booked_ticket AS(
  SELECT
  a.booking_id,
  a.ipcc AS booking_ipcc,
  a.account_code AS account_code_booking,
  a.account_code_type AS account_code_type_booking,
  b.ipcc AS ticketed_ipcc,
  b.account_code AS account_code_ticketing,
  b.account_code_type AS account_code_type_ticketing,
  CASE
  WHEN itinerary_status IS NULL THEN 'NOT TICKETED'
  ELSE 'TICKETED'
  END AS itinerary_status
  FROM
  booking a
  LEFT JOIN
  ticketing b
  ON
  a.booking_id=b.booking_id
),
  flights_bookings AS(
    SELECT
      DATE(created_at) AS date_key,
      booking_id,
      fare_type,
      click_id,
      search_id,
      fare_ipcc,
      ipcc,
      booking_type,
      integration_type,
      finance_revenue_usd,
      gross_revenue_in_usd,
      total_price_usd,
      conversions_tracked AS conversions_tracked,
      conversions_adjusted AS conversions_adjusted,
      IF(overall_booking_status LIKE "%ancellation%", conversions_tracked, 0) AS cancelled_booking
    FROM `wego-cloud.wego_analytics.flights_bookings`
    WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
    AND conversions_adjusted=1
),
  flights_insurance AS (
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
flight_searches AS(
  SELECT
  DISTINCT
    DATE(a.created_at) AS date_key,
    a.session_id,
    b.search_id,
    channel,
    ts_code,
    user_country_code,
    a.site_code,
    first_departure_airport_code,
    first_departure_city_code,
    first_arrival_airport_code,
    first_arrival_city_code,
    trip_type,
    trip_category,
    route_code,
    route_cities,
    CASE WHEN ARRAY_LENGTH(b.searched_routes) > 1 THEN 'multi_routes'    
    ELSE 'one_route' END AS multi_routes_flag
    FROM
    `wego-cloud.wego_analytics.sessions` a
    LEFT JOIN
    `wego-cloud.wego_analytics.flights_searches` b
    ON
    a.session_id=b.session_id
    WHERE
    a._PARTITIONTIME = TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
    AND b._PARTITIONTIME = TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
),
search_activities AS(
  SELECT
    date_key,
    a.session_id,
    a.search_id,
    click_id,
    channel,
    ts_code,
    user_country_code,
    a.site_code,
    first_departure_airport_code,
    first_departure_city_code,
    first_arrival_airport_code,
    first_arrival_city_code,
    a.trip_type,
    a.trip_category,
    route_code,
    a.route_cities,
    departure_airport_code,
    departure_city_code,
    arrival_airport_code,
    arrival_city_code,
    airline,
    a.multi_routes_flag,
    COALESCE(COUNT(DISTINCT IF(conversions_tracked IS NOT NULL,click_id,NULL)),0) AS clicks,
    COALESCE(SUM(conversions_tracked),0) AS conversion_click
    FROM
    flight_searches a
    LEFT JOIN
    `wego-cloud.wego_analytics.flights_clicks` b
    ON
    a.search_id=b.search_id
    WHERE
    b._PARTITIONTIME = TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
),
booking_margin AS(
  SELECT
  a.date_key,
  a.booking_id,
  fare_type,
  search_id,
  click_id,
  fare_ipcc,
  ipcc,
  booking_type,
  integration_type,
  total_price_usd,
  conversions_tracked,
  conversions_adjusted,
  cancelled_booking,
  finance_revenue_usd AS revenue,
  gross_revenue_in_usd AS gross_revenue,
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
  flights_bookings a
  LEFT JOIN
  flights_insurance b
  ON
  a.booking_id=b.booking_id
),
final_search AS(
  SELECT
  a.*,
  channel,
  ts_code,
  airline,
  user_country_code,
  site_code,
  departure_airport_code,
  departure_city_code,
  arrival_airport_code,
  arrival_city_code,
  route_code,
  route_cities,
  click_id,
  trip_type,
  trip_category,
  clicks,
  conversion_click,
  multi_routes_flag
  FROM
  search_revalidation a
  LEFT JOIN
  search_activities b
  ON
  a.search_id=b.search_id
),
final_booking AS(
  SELECT
  a.*,
  search_id,
  integration_type,
  total_price_usd,
  fare_ipcc,
  ipcc,
  click_id,
  fare_type,
  booking_type,
  SUM(conversions_tracked) conversions_tracked,
  SUM(conversions_adjusted) conversions_adjusted,
  SUM(cancelled_booking) cancelled_booking,
  AVG(revenue) revenue,
  AVG(gross_revenue) gross_revenue,
  AVG(total_net_margin) total_net_margin,
  AVG(total_gross_margin) total_gross_margin,
  AVG(net_margin_percentage) net_margin_percentage,
  AVG(gross_margin_percentage) gross_margin_percentage
  FROM
  booked_ticket a
  LEFT JOIN
  booking_margin b
  ON
  a.booking_id=b.booking_id AND
  a.booking_ipcc=b.ipcc AND
  a.ticketed_ipcc=b.ipcc
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
SELECT
DISTINCT
a.* EXCEPT(click_id, multi_routes_flag),
b.* EXCEPT(search_id, fare_ipcc,ipcc,click_id),
a.multi_routes_flag
FROM
final_search a
LEFT JOIN
final_booking b
ON
a.search_id=b.search_id AND
a.click_id=b.click_id AND
a.fare_ipcc=b.fare_ipcc AND
a.revalidation_ipcc=b.ipcc
{% endraw %}
