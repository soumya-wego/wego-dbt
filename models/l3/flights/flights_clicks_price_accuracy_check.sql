{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_clicks_price_accuracy_check_daily_append
-- Destination: analysis.flights_clicks_price_accuracy_check  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table 
-- analysis.flights_clicks_price_accuracy_check
-- partition by date(created_at)
-- as

-- SELECT
--   timestamp AS created_at,
--   p.provider_code,
--   p.pos_provider_code,
--   p.site_code,
--   p.locale,
--   p.search_id,
--   p.fare_id,
--   c.session_id,
--   client_id,
--   device_type,
--   cabin_class,
--   adults_count,
--   children_count,
--   infants_count,
--   legs,
--   stops,
--   trip_type,
--   trip_category,
--   departure_airport_code,
--   departure_city_code,
--   departure_country_code,
--   arrival_airport_code,
--   arrival_city_code,
--   arrival_country_code,
--   first_departure_date,
--   last_departure_date,
--   lead_time,
--   trip_duration,
--   route_airports,
--   route_cities,
--   route_countries,
--   airlines,
--   airline,
--   agreement_term_provider_code,
--   agreement_term_product,
--   agreement_term_bucket,
--   agreement_term_display_type,
--   agreement_term_ecpc,
--   agreement_term_id,
--   conversions_tracked,
--   finance_revenue_usd, 
--   wego_fare.amount AS wego_clicked_price,
--   wego_fare.currency AS wego_clicked_currency,
--   wego_fare.usd_amount AS wego_clicked_price_usd,
--   partner_fare.amount AS partner_landing_price,
--   partner_fare.currency AS partner_landing_currency,
--   partner_fare.usd_amount AS partner_landing_price_usd,
--   state,
--   disparity_info.usd_amount AS price_difference_usd,
--   disparity_info.percentage AS price_difference_percentage
-- FROM
--   `wego-cloud.services_partner.flight_fares_accuracy_check*` p
-- INNER JOIN
--   `wego-cloud.wego_analytics.flights_clicks` c
-- ON
--   p.fare_id = c.fare_id and date(p.timestamp) = date(c.created_at)
-- WHERE
--   _TABLE_SUFFIX BETWEEN "20240701"
--   AND "20240910"
--   AND DATE(_PARTITIONTIME) BETWEEN '2024-07-01'
--   AND '2024-09-10'

-- daily append: 

SELECT
  timestamp AS created_at,
  p.provider_code,
  p.pos_provider_code,
  p.site_code,
  p.locale,
  p.search_id,
  p.fare_id,
  c.session_id,
  client_id,
  device_type,
  cabin_class,
  adults_count,
  children_count,
  infants_count,
  legs,
  stops,
  trip_type,
  trip_category,
  departure_airport_code,
  departure_city_code,
  departure_country_code,
  arrival_airport_code,
  arrival_city_code,
  arrival_country_code,
  first_departure_date,
  last_departure_date,
  lead_time,
  trip_duration,
  route_airports,
  route_cities,
  route_countries,
  airlines,
  airline,
  agreement_term_provider_code,
  agreement_term_product,
  agreement_term_bucket,
  agreement_term_display_type,
  agreement_term_ecpc,
  agreement_term_id,
  conversions_tracked,
  finance_revenue_usd, 
  wego_fare.amount AS wego_clicked_price,
  wego_fare.currency AS wego_clicked_currency,
  wego_fare.usd_amount AS wego_clicked_price_usd,
  partner_fare.amount AS partner_landing_price,
  partner_fare.currency AS partner_landing_currency,
  partner_fare.usd_amount AS partner_landing_price_usd,
  state,
  disparity_info.usd_amount AS price_difference_usd,
  disparity_info.percentage AS price_difference_percentage
FROM
  `wego-cloud.services_partner.flight_fares_accuracy_check*` p
INNER JOIN
  `wego-cloud.wego_analytics.flights_clicks` c
ON
  p.fare_id = c.fare_id and date(p.timestamp) = date(c.created_at)
WHERE
  _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
  AND DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
{% endraw %}
