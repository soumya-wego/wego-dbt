{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : fare_family_price_analysis
-- Destination: analysis.fare_family_price_analysis  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('integrated_bookings_flights', 'branded_fares') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
INSERT INTO wego-cloud.analysis.fare_family_price_analysis
WITH
flights_sessions AS(
    SELECT
    DATE(created_at) AS date_key,
    session_id,
    channel,
    user_country_code,
    wg_source,
    wg_campaign
    FROM
    `wego-cloud.wego_analytics.sessions`
    WHERE
    _PARTITIONTIME =TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  ),
  flights_clicks AS (
    SELECT
      DATE(created_at) AS date_key,
      search_id,
      site_code,
      a.channel,
      a.user_country_code,
      a.wg_source,
      a.wg_campaign,
      fare_id,
      click_id,
      trip_type,
      trip_category,
      departure_airport_code,
      departure_city_code,
      arrival_airport_code,
      arrival_city_code,
      route_airports,
      route_cities,
      airline,
      COUNT(DISTINCT IF(conversions_tracked IS NOT NULL,click_id,NULL)) AS clicks,
      SUM(conversions_tracked) AS conversion_click
    FROM
    flights_sessions a
    LEFT JOIN
    `wego-cloud.wego_analytics.flights_clicks` b
    ON
    a.session_id=b.session_id
    WHERE
      _PARTITIONTIME = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
  ),
  clicked_fares AS(
    SELECT
      date_key,
      b.fare_id,
      b.search_id,
      airline,
      trip_category,
      trip_type,
      departure_airport_code,
      departure_city_code,
      arrival_airport_code,
      arrival_city_code,
      route_airports,
      route_cities,
      conversion_click,
      bf.ms_fare_id,
      clicks,
      channel,
      user_country_code,
      wg_source,
      wg_campaign,
      site_code
    FROM flights_clicks b
    JOIN `wego-cloud.integrated_bookings_flights.branded_fares*` bf
      ON b.fare_id=bf.ms_fare_id
    WHERE
    bf._table_suffix = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      AND bf.booking_price.total_amount_usd IS NOT NULL
      AND bf.ms_fare_id IS NOT NULL
      AND bf.endpoint
        = 'COMPARE'  -- Only COMPARE endpoint where branded fares are fetched
      AND (bf.selected = TRUE OR bf.selected IS NULL)
  ),
  booked_fares AS (
    SELECT
      DATE(b.created_at) AS date_key,
      b.booking_id,
      booking_margin_id,
      b.search_id,
      b.integration_type,
      finance_revenue_usd,
      gross_revenue_in_usd,
      airline,
      trip_category,
      trip_type,
      departure_airport_code,
      departure_city_code,
      arrival_airport_code,
      arrival_city_code,
      route_airports,
      route_cities,
      conversions_tracked AS conversion,
      bf.ms_fare_id,
      branded_fare_id,
      AVG(bf.booking_price.total_amount_usd)
        AS booked_price_usd,  -- Use AVG in case of slight differences
      MAX(bf.booking_price.currency_code) AS currency_code
    FROM `wego-cloud.wego_analytics.flights_bookings` b
    JOIN `wego-cloud.integrated_bookings_flights.branded_fares*` bf
      ON b.branded_fare_id = bf.id
      AND b.ms_fare_id=bf.ms_fare_id
    WHERE
      TIMESTAMP_TRUNC(b._PARTITIONTIME, DAY)
      = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND bf._table_suffix = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      AND bf.booking_price.total_amount_usd IS NOT NULL
      AND bf.ms_fare_id IS NOT NULL
      AND bf.endpoint
        = 'COMPARE'  -- Only COMPARE endpoint where branded fares are fetched
      AND (bf.selected = TRUE OR bf.selected IS NULL)
      AND conversions_adjusted=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
  ),
  all_branded_fares AS (
    -- Get all branded fares (selected=TRUE) to see all available options
    -- Use GROUP BY to deduplicate branded_fare_ids with different prices
    SELECT
      id AS branded_fare_id,
      ms_fare_id,
      AVG(booking_price.total_amount_usd)
        AS fare_price_usd,  -- Use AVG to consolidate multiple prices
      MAX(brand.name) AS brand_name,
      MAX(booking_price.currency_code) AS currency_code
    FROM `wego-cloud.integrated_bookings_flights.branded_fares*`
    WHERE
      _table_suffix = (FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
      AND booking_price.total_amount_usd IS NOT NULL
      AND ms_fare_id IS NOT NULL
      AND endpoint = 'COMPARE'  -- Only COMPARE endpoint
      AND (selected = TRUE OR selected IS NULL)
    GROUP BY 1, 2
  ),
  ranked AS (
    SELECT
      ms_fare_id,
      name,
      total_amount_usd,
      rk,
      is_single_brand_fare,
      MIN(total_amount_usd) OVER (PARTITION BY ms_fare_id) AS cheapest_price_usd
    FROM 
    (
      SELECT
        ms_fare_id,
        brand.name AS name,
        AVG(booking_price.total_amount_usd) AS total_amount_usd,
        ROW_NUMBER() OVER (
          PARTITION BY ms_fare_id
          ORDER BY AVG(booking_price.total_amount_usd) ASC
        ) AS rk,

        -- Total number of distinct brand.names for this ms_fare_id
        COUNT(*) OVER (PARTITION BY ms_fare_id) AS brand_count,

        -- Flag: TRUE if this ms_fare_id has only one brand.name
        (COUNT(*) OVER (PARTITION BY ms_fare_id) = 1) AS is_single_brand_fare
      FROM `wego-cloud.integrated_bookings_flights.branded_fares*`
      WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
        AND booking_price.total_amount_usd IS NOT NULL
        AND ms_fare_id IS NOT NULL
        AND endpoint = 'COMPARE'
        AND selected = TRUE
      GROUP BY ms_fare_id, brand.name
    )
  ),
  min_prices AS (
  SELECT
    ms_fare_id,
    is_single_brand_fare,
    MAX(IF(rk = 1, name,             NULL)) AS min_brand_name,
    MAX(IF(rk = 1, total_amount_usd, NULL)) AS min_price_usd,

    -- rk=2
    MAX(IF(rk = 2, name,             NULL)) AS tier2_brand_name,
    MAX(IF(rk = 2, total_amount_usd, NULL)) AS tier2_price_usd,
    ROUND(MAX(IF(rk = 2, total_amount_usd - cheapest_price_usd, NULL)), 4) AS tier2_diff_usd,
    ROUND(MAX(IF(rk = 2, SAFE_DIVIDE(total_amount_usd - cheapest_price_usd, cheapest_price_usd) * 100, NULL)), 4) AS tier2_diff_pct,

    -- rk=3
    MAX(IF(rk = 3, name,             NULL)) AS tier3_brand_name,
    MAX(IF(rk = 3, total_amount_usd, NULL)) AS tier3_price_usd,
    ROUND(MAX(IF(rk = 3, total_amount_usd - cheapest_price_usd, NULL)), 4) AS tier3_diff_usd,
    ROUND(MAX(IF(rk = 3, SAFE_DIVIDE(total_amount_usd - cheapest_price_usd, cheapest_price_usd) * 100, NULL)), 4) AS tier3_diff_pct,

    -- rk=4
    MAX(IF(rk = 4, name,             NULL)) AS tier4_brand_name,
    MAX(IF(rk = 4, total_amount_usd, NULL)) AS tier4_price_usd,
    ROUND(MAX(IF(rk = 4, total_amount_usd - cheapest_price_usd, NULL)), 4) AS tier4_diff_usd,
    ROUND(MAX(IF(rk = 4, SAFE_DIVIDE(total_amount_usd - cheapest_price_usd, cheapest_price_usd) * 100, NULL)), 4) AS tier4_diff_pct,

    -- rk=5
    MAX(IF(rk = 5, name,             NULL)) AS tier5_brand_name,
    MAX(IF(rk = 5, total_amount_usd, NULL)) AS tier5_price_usd,
    ROUND(MAX(IF(rk = 5, total_amount_usd - cheapest_price_usd, NULL)), 5) AS tier5_diff_usd,
    ROUND(MAX(IF(rk = 5, SAFE_DIVIDE(total_amount_usd - cheapest_price_usd, cheapest_price_usd) * 100, NULL)), 5) AS tier5_diff_pct

    FROM ranked
    GROUP BY 1,2
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
      TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)
     = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
     GROUP BY 1,2
  )
SELECT DISTINCT
  cf.date_key,
  bf.booking_id,
  booking_margin_id,
  cf.search_id,
  cf.clicks,
  cf.conversion_click,
  cf.trip_category,
  cf.trip_type,
  cf.departure_airport_code,
  cf.departure_city_code,
  cf.arrival_airport_code,
  cf.arrival_city_code,
  cf.route_airports,
  cf.route_cities,
  conversion,
  cf.airline,
  channel,
  user_country_code,
  site_code,
  wg_source,
  wg_campaign,
  integration_type,
  abf.branded_fare_id,
  cf.ms_fare_id,
  is_single_brand_fare,
  abf.brand_name,
  finance_revenue_usd AS revenue,
  gross_revenue_in_usd AS gross_revenue,
  finance_revenue_usd + IFNULL(insurance_net_margin_usd, 0) AS total_net_margin,
  gross_revenue_in_usd + IFNULL(insurance_gross_revenue_in_usd, 0)
    AS total_gross_margin,
  SAFE_DIVIDE(
    (finance_revenue_usd + IFNULL(insurance_net_margin_usd, 0)),
    booked_price_usd) AS net_margin_percentage,
  SAFE_DIVIDE(
    (gross_revenue_in_usd + IFNULL(insurance_gross_revenue_in_usd, 0)),
    booked_price_usd) AS gross_margin_percentage,
  -- Only populate these fields for the actually booked fare
  CASE
    WHEN abf.branded_fare_id = bf.branded_fare_id
      THEN bf.booked_price_usd
    ELSE NULL
    END AS booked_price_usd,
  CASE
    WHEN abf.branded_fare_id = bf.branded_fare_id THEN mp.min_price_usd
    ELSE NULL
    END AS cheapest_price_usd,
  tier2_brand_name,
  tier2_price_usd,
  tier2_diff_usd,
  tier2_diff_pct,
  tier3_brand_name,
  tier3_price_usd,
  tier3_diff_usd,
  tier3_diff_pct,
  tier4_brand_name,
  tier4_price_usd,
  tier4_diff_usd,
  tier4_diff_pct,
  tier5_brand_name,
  tier5_price_usd,
  tier5_diff_usd,
  tier5_diff_pct,
  CASE
    WHEN abf.branded_fare_id = bf.branded_fare_id
      THEN (bf.booked_price_usd = mp.min_price_usd)
    ELSE NULL
    END AS is_cheapest,
  CASE
    WHEN abf.branded_fare_id = bf.branded_fare_id
      THEN ROUND(bf.booked_price_usd - mp.min_price_usd, 2)
    ELSE NULL
    END AS price_diff_usd,
  bf.currency_code
FROM 
  clicked_fares cf
LEFT JOIN 
  booked_fares bf
  ON
  cf.ms_fare_id=bf.ms_fare_id
LEFT JOIN all_branded_fares abf
  ON bf.branded_fare_id = abf.branded_fare_id
LEFT JOIN min_prices mp
  ON bf.ms_fare_id = mp.ms_fare_id
LEFT JOIN flights_insurance fi
  ON bf.booking_id = fi.booking_id
{% endraw %}
