{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_bookings_passengers_daily_run
-- Destination: wego_analytics.flights_bookings_passengers  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table 
-- wego_analytics.flights_bookings_passengers
-- partition by date
-- as


SELECT
  p.* except(booking_id),
  b.created_at,
  date(created_at) as date,
  b.client_id,
  b.session_id,
  b.search_id,
  b.click_id,
  b.appsflyer_id,
  b.device_type,
  b.site_code,
  b.locale,
  b.user_country_code,
  b.channel,
  b.distribution_ts_code,
  b.distribution_currency_code,
  b.distribution_agreement_term_type,
  b.distribution_provider_id,
  b.market,
  b.user_id_hash,
  b.contact_email_hash,
  b.contact_email_sha256,
  b.cabin_class,
  b.adults_count,
  b.children_count,
  b.infants_count,
  b.tickets,
  b.trip_type,
  b.trip_category,
  b.departure_airport_code,
  b.departure_city_code,
  b.departure_country_code,
  b.arrival_airport_code,
  b.arrival_city_code,
  b.arrival_country_code,
  b.first_departure_date,
  b.last_departure_date,
  b.last_arrival_date,
  b.lead_time,
  b.trip_duration,
  b.route_airports,
  b.route_cities,
  b.route_countries,
  b.airlines,
  b.airline,
  b.integration_type,
  b.booking_id,
  b.itinerary_id,
  b.order_id,
  b.gds_ref,
  b.booking_ref,
  b.booking_mode,
  b.fare_type,
  b.payment_id,
  b.payment_ref_id,
  b.cost_of_sales_usd,
  b.currency_code,
  b.price_in_usd,
  b.total_price_usd,
  b.gross_revenue_in_usd,
  b.finance_revenue_usd,
  b.card_type,
  b.card_category,
  b.card_scheme,
  b.card_issuer,
  b.card_issuer_country_code,
  b.payment_method,
  b.promo_discount_amount_usd,
  b.promo_code,
  b.promo_campaign_name,
  b.promo_uuid,
  b.promo_type,
  b.promo_campaign_type,
  b.promo_cost_usd,
  b.attribution_channel,
  b.attribution_ts_code,
  b.attribution_wg_source,
  b.attribution_wg_medium,
  b.attribution_wg_campaign,
  b.source
FROM
  `wego-cloud.wego_analytics.flights_bookings` b 
  inner join 
  (SELECT 
  booking_id,
  id as passenger_id,
  name_id,
  date_diff(current_date(), date_of_birth, YEAR) as age,
  nationality,
  passenger_type,
  case
  when title_type = "MR" then "male"
  when title_type = "MASTER" then "male"
  else "female"
  end as gender
FROM
  `wego-pii-data.flights_booking_pii_production.passengers`
WHERE
  date(created_at) != current_date()
  group by 1,2,3,4,5,6,7) p on b.booking_id = p.booking_id
WHERE
  date(created_at) != current_date()
  and conversions_adjusted = 1
{% endraw %}
