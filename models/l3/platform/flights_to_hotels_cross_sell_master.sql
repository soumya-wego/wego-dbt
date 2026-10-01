{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : flights_to_hotels_cross_sell_master
-- Destination: analysis.flights_to_hotels_cross_sell_master  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('hotels_searches_funnel') }}
-- depends_on: {{ ref('flights_bookings_passengers') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
{% raw %}
DELETE FROM `wego-cloud.analysis.flights_to_hotels_cross_sell_master`
WHERE created_date BETWEEN DATE_SUB(@run_date, INTERVAL 180 DAY) AND DATE_SUB(@run_date, INTERVAL 1 DAY);

INSERT INTO `wego-cloud.analysis.flights_to_hotels_cross_sell_master`

WITH

flight_bookings AS (
  SELECT
    booking_id,
    order_id,
    booking_ref,
    ANY_VALUE(client_id)                  AS client_id,
    ANY_VALUE(created_at)                 AS created_at,
    DATE(ANY_VALUE(created_at))           AS created_date,
    ANY_VALUE(site_code)                  AS site_code,
    ANY_VALUE(market)                     AS market,
    CASE ANY_VALUE(site_code)
      WHEN 'SA' THEN 'KSA'
      WHEN 'AE' THEN 'UAE'
      WHEN 'KW' THEN 'Kuwait'
      WHEN 'OM' THEN 'Rest GCC'
      WHEN 'QA' THEN 'Rest GCC'
      WHEN 'BH' THEN 'Rest GCC'
      WHEN 'EG' THEN 'Egypt'
      WHEN 'PK' THEN 'Pakistan'
      WHEN 'US' THEN 'US'
      ELSE 'RoW'
    END                                   AS strategy_market,
    ANY_VALUE(device_type)                AS device_type,
    ANY_VALUE(channel)                    AS channel,
    ANY_VALUE(attribution_channel)        AS attribution_channel,
    ANY_VALUE(attribution_ts_code)        AS attribution_ts_code,
    ANY_VALUE(trip_type)                  AS trip_type,
    ANY_VALUE(trip_category)              AS trip_category,
    ANY_VALUE(departure_country_code)     AS departure_country_code,
    ANY_VALUE(departure_city_code)        AS departure_city_code,
    ANY_VALUE(arrival_country_code)       AS arrival_country_code,
    ANY_VALUE(arrival_city_code)          AS arrival_city_code,
    ANY_VALUE(route_countries)            AS route_countries,
    ANY_VALUE(route_airports)             AS route_airports,
    DATE(ANY_VALUE(first_departure_date)) AS first_departure_date,
    DATE(ANY_VALUE(last_departure_date))  AS last_departure_date,
    ANY_VALUE(lead_time)                  AS lead_time,
    CASE
      WHEN ANY_VALUE(lead_time) = 0   THEN 'same day'
      WHEN ANY_VALUE(lead_time) <= 3  THEN 'last minute (1-3 days)'
      WHEN ANY_VALUE(lead_time) <= 7  THEN 'short term (4-7 days)'
      WHEN ANY_VALUE(lead_time) <= 14 THEN 'medium term (8-14 days)'
      WHEN ANY_VALUE(lead_time) <= 30 THEN 'long term (15-30 days)'
      ELSE                                 'advanced (> 30 days)'
    END                                   AS lead_time_bucket,
    ANY_VALUE(cabin_class)                AS cabin_class,
    ANY_VALUE(payment_method)             AS payment_method,
    ANY_VALUE(total_price_usd)            AS flight_total_price_usd,
    ANY_VALUE(finance_revenue_usd)        AS flight_finance_revenue_usd,
    ANY_VALUE(adults_count)               AS adults_count,
    ANY_VALUE(children_count)             AS children_count,
    ANY_VALUE(infants_count)              AS infants_count,
    ANY_VALUE(tickets)                    AS tickets,
    ANY_VALUE(user_country_code)          AS user_country_code,
    ANY_VALUE(locale)                     AS locale,
    MAX(CASE WHEN name_id = 1 THEN nationality END) AS main_pax_nationality,
    CASE
      WHEN MAX(CASE WHEN nationality = arrival_country_code THEN 1 ELSE 0 END) = 1
        AND MAX(CASE WHEN nationality != arrival_country_code THEN 1 ELSE 0 END) = 0
        THEN 'Locals'
      WHEN MAX(CASE WHEN nationality = arrival_country_code THEN 1 ELSE 0 END) = 0
        AND MAX(CASE WHEN nationality != arrival_country_code THEN 1 ELSE 0 END) = 1
        THEN 'Foreigners'
      ELSE 'Mixed'
    END                                   AS booking_nationality_group,
    CASE
      WHEN SUM(CASE WHEN passenger_type = 'ADULT' THEN 1 ELSE 0 END) = 1
        AND SUM(CASE WHEN passenger_type IN ('CHILD', 'INFANT') THEN 1 ELSE 0 END) = 0
        THEN 'solo trip'
      WHEN SUM(CASE WHEN passenger_type = 'ADULT' THEN 1 ELSE 0 END) = 2
        AND SUM(CASE WHEN passenger_type IN ('CHILD', 'INFANT') THEN 1 ELSE 0 END) = 0
        THEN 'couple trip'
      WHEN SUM(CASE WHEN passenger_type = 'ADULT' THEN 1 ELSE 0 END) > 2
        AND SUM(CASE WHEN passenger_type IN ('CHILD', 'INFANT') THEN 1 ELSE 0 END) = 0
        THEN 'group trip (adults only)'
      WHEN SUM(CASE WHEN passenger_type = 'ADULT' THEN 1 ELSE 0 END) > 0
        AND SUM(CASE WHEN passenger_type IN ('CHILD', 'INFANT') THEN 1 ELSE 0 END) > 0
        THEN 'group trip (with kids)'
      ELSE 'others'
    END                                   AS pax_trip_type,
    CASE
      WHEN ANY_VALUE(trip_type) = 'oneway'
        THEN DATE_ADD(DATE(ANY_VALUE(first_departure_date)), INTERVAL 7 DAY)
      ELSE DATE(ANY_VALUE(last_departure_date))
    END                                   AS travel_window_end
  FROM `wego-cloud.wego_analytics.flights_bookings_passengers`
  WHERE date BETWEEN DATE_SUB(@run_date, INTERVAL 180 DAY) AND DATE_SUB(@run_date, INTERVAL 1 DAY)
  GROUP BY booking_id, order_id, booking_ref
),

-- Each search attributed to 1 flight (most recent flight before the search)
hotel_searches_attributed AS (
  SELECT
    sf.search_id,
    fb.booking_id,
    fb.created_at                         AS flight_created_at,
    sf.created_at                         AS search_created_at,
    sf.city_code,
    sf.check_in,
    sf.check_out,
    sf.hotel_detail_page_flag,
    sf.room_detail_page_flag,
    sf.hotels_booking_page_flag,
    sf.payment_page_flag,
    sf.paid_flag,
    ROW_NUMBER() OVER (
      PARTITION BY sf.search_id
      ORDER BY fb.created_at DESC
    )                                     AS search_rn
  FROM flight_bookings fb
  JOIN `wego-cloud.analysis.hotels_searches_funnel` sf
    ON  fb.client_id            = sf.client_id
    AND fb.arrival_country_code = sf.country_code
    AND sf.check_in BETWEEN fb.first_departure_date AND fb.travel_window_end
    AND DATE(sf.created_at) BETWEEN DATE(fb.created_at) AND fb.travel_window_end
),

-- After dedup, rank searches within each booking to identify the first one
hotel_searches_deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY booking_id
      ORDER BY search_created_at
    )                                     AS booking_search_rn
  FROM hotel_searches_attributed
  WHERE search_rn = 1
),

hotel_searches_agg AS (
  SELECT
    booking_id,
    COUNT(DISTINCT search_id)                                   AS hotel_search_count,
    1                                                           AS has_hotel_search,
    MIN(search_created_at)                                      AS first_search_timestamp,
    MIN(CASE WHEN search_created_at > flight_created_at
             THEN search_created_at END)                        AS first_search_after_booking_timestamp,
    MAX(CASE WHEN booking_search_rn = 1 THEN city_code END)     AS first_search_city_code,
    MAX(CASE WHEN booking_search_rn = 1 THEN check_in END)      AS first_search_checkin,
    MAX(CASE WHEN booking_search_rn = 1 THEN check_out END)     AS first_search_checkout,
    MAX(CASE WHEN booking_search_rn = 1 THEN hotel_detail_page_flag END)   AS first_search_reached_detail,
    MAX(CASE WHEN booking_search_rn = 1 THEN room_detail_page_flag END)    AS first_search_reached_room,
    MAX(CASE WHEN booking_search_rn = 1 THEN hotels_booking_page_flag END) AS first_search_reached_booking_page,
    MAX(CASE WHEN booking_search_rn = 1 THEN payment_page_flag END)        AS first_search_reached_payment,
    MAX(CASE WHEN booking_search_rn = 1 THEN paid_flag END)                AS first_search_paid,
    MAX(hotel_detail_page_flag)                                 AS ever_reached_detail,
    MAX(room_detail_page_flag)                                  AS ever_reached_room,
    MAX(hotels_booking_page_flag)                               AS ever_reached_booking_page,
    MAX(payment_page_flag)                                      AS ever_reached_payment,
    MAX(paid_flag)                                              AS ever_paid
  FROM hotel_searches_deduped
  GROUP BY booking_id
),

-- Each hotel booking attributed to 1 flight (most recent flight before the booking)
hotel_bookings_attributed AS (
  SELECT
    hb.client_booking_id,
    fb.booking_id,
    fb.created_at                         AS flight_created_at,
    hb.created_at                         AS hotel_booking_created_at,
    hb.hotel_city,
    hb.hotel_country_code,
    DATE(hb.check_in)                     AS hotel_checkin_date,
    DATE(hb.check_out)                    AS hotel_checkout_date,
    hb.wego_total_price_usd,
    ROW_NUMBER() OVER (
      PARTITION BY hb.client_booking_id
      ORDER BY fb.created_at DESC
    )                                     AS booking_rn
  FROM flight_bookings fb
  JOIN `wego-cloud.wego_analytics.hotels_bookings` hb
    ON  fb.client_id            = hb.client_id
    AND fb.arrival_country_code = hb.hotel_country_code
    AND DATE(hb.check_in) BETWEEN fb.first_departure_date AND fb.travel_window_end
    AND hb.conversions_adjusted > 0
    AND DATE(hb.created_at) BETWEEN DATE(fb.created_at) AND fb.travel_window_end
),

-- After dedup, rank hotel bookings within each flight to identify the first one
hotel_bookings_deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY booking_id
      ORDER BY hotel_booking_created_at
    )                                     AS flight_booking_rn
  FROM hotel_bookings_attributed
  WHERE booking_rn = 1
),

hotel_bookings_agg AS (
  SELECT
    booking_id,
    COUNT(DISTINCT client_booking_id)                                           AS hotel_booking_count,
    1                                                                           AS has_hotel_booking,
    MIN(hotel_booking_created_at)                                               AS first_hotel_booking_timestamp,
    MAX(CASE WHEN flight_booking_rn = 1 THEN client_booking_id END)             AS hotel_client_booking_id,
    MAX(CASE WHEN flight_booking_rn = 1 THEN hotel_city END)                    AS hotel_city,
    MAX(CASE WHEN flight_booking_rn = 1 THEN hotel_country_code END)            AS hotel_country_code,
    MAX(CASE WHEN flight_booking_rn = 1 THEN hotel_checkin_date END)            AS hotel_checkin_date,
    MAX(CASE WHEN flight_booking_rn = 1 THEN hotel_checkout_date END)           AS hotel_checkout_date,
    SUM(wego_total_price_usd)                                                   AS hotel_total_price_usd
  FROM hotel_bookings_deduped
  GROUP BY booking_id
)

SELECT
  fb.booking_id,
  fb.order_id,
  fb.booking_ref,
  fb.client_id,
  fb.created_at,
  fb.created_date,
  fb.site_code,
  fb.market,
  fb.strategy_market,
  fb.device_type,
  fb.channel,
  fb.attribution_channel,
  fb.attribution_ts_code,
  fb.trip_type,
  fb.trip_category,
  fb.departure_country_code,
  fb.departure_city_code,
  fb.arrival_country_code,
  fb.arrival_city_code,
  fb.route_countries,
  fb.route_airports,
  fb.first_departure_date,
  fb.last_departure_date,
  fb.travel_window_end,
  fb.lead_time,
  fb.lead_time_bucket,
  fb.cabin_class,
  fb.payment_method,
  fb.flight_total_price_usd,
  fb.flight_finance_revenue_usd,
  fb.adults_count,
  fb.children_count,
  fb.infants_count,
  fb.tickets,
  fb.user_country_code,
  fb.locale,
  fb.main_pax_nationality,
  fb.booking_nationality_group,
  fb.pax_trip_type,
  IFNULL(hs.has_hotel_search, 0)                          AS has_hotel_search,
  IFNULL(hs.hotel_search_count, 0)                        AS hotel_search_count,
  hs.first_search_timestamp,
  hs.first_search_after_booking_timestamp,
  TIMESTAMP_DIFF(hs.first_search_after_booking_timestamp, fb.created_at, HOUR)
                                                          AS hours_flight_to_first_search_after_booking,
  DATE_DIFF(DATE(hs.first_search_timestamp), fb.created_date, DAY)
                                                          AS days_flight_to_first_search,
  TIMESTAMP_DIFF(hs.first_search_timestamp, fb.created_at, HOUR)
                                                          AS hours_flight_to_first_search,
  hs.first_search_city_code,
  hs.first_search_checkin,
  hs.first_search_checkout,
  IFNULL(hs.first_search_reached_detail, 0)               AS first_search_reached_detail,
  IFNULL(hs.first_search_reached_room, 0)                 AS first_search_reached_room,
  IFNULL(hs.first_search_reached_booking_page, 0)         AS first_search_reached_booking_page,
  IFNULL(hs.first_search_reached_payment, 0)              AS first_search_reached_payment,
  IFNULL(hs.first_search_paid, 0)                         AS first_search_paid,
  IFNULL(hs.ever_reached_detail, 0)                       AS ever_reached_detail,
  IFNULL(hs.ever_reached_room, 0)                         AS ever_reached_room,
  IFNULL(hs.ever_reached_booking_page, 0)                 AS ever_reached_booking_page,
  IFNULL(hs.ever_reached_payment, 0)                      AS ever_reached_payment,
  IFNULL(hs.ever_paid, 0)                                 AS ever_paid,
  IFNULL(hb.has_hotel_booking, 0)                         AS has_hotel_booking,
  IFNULL(hb.hotel_booking_count, 0)                       AS hotel_booking_count,
  hb.first_hotel_booking_timestamp,
  hb.hotel_client_booking_id,
  hb.hotel_city,
  hb.hotel_country_code,
  hb.hotel_checkin_date,
  hb.hotel_checkout_date,
  hb.hotel_total_price_usd,
  DATE_DIFF(DATE(hb.first_hotel_booking_timestamp), fb.created_date, DAY)
                                                          AS days_flight_to_hotel_booking,
  TIMESTAMP_DIFF(hb.first_hotel_booking_timestamp, fb.created_at, HOUR)
                                                          AS hours_flight_to_hotel_booking,
  DATE_DIFF(fb.first_departure_date, DATE(hb.first_hotel_booking_timestamp), DAY)
                                                          AS days_hotel_booking_to_departure,
  CASE
    WHEN hb.first_hotel_booking_timestamp IS NULL
      THEN 'no hotel booking'
    WHEN hb.first_hotel_booking_timestamp < fb.created_at
      THEN 'before flight booking'
    WHEN TIMESTAMP_DIFF(hb.first_hotel_booking_timestamp, fb.created_at, HOUR) <= 168
      THEN 'within 7d'
    WHEN TIMESTAMP_DIFF(hb.first_hotel_booking_timestamp, fb.created_at, HOUR) <= 336
      THEN 'within 14d'
    WHEN TIMESTAMP_DIFF(hb.first_hotel_booking_timestamp, fb.created_at, HOUR) <= 720
      THEN 'within 30d'
    ELSE '30d+'
  END                                                     AS hotel_booking_attribution_window

FROM flight_bookings fb
LEFT JOIN hotel_searches_agg  hs ON fb.booking_id = hs.booking_id
LEFT JOIN hotel_bookings_agg  hb ON fb.booking_id = hb.booking_id
;
{% endraw %}
