{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_pricing_daily_append
-- Destination: analysis.hotels_pricing_analysis  (unchanged)
-- Schedule   : every day 02:25   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- -- backfill:
-- create table 
-- analysis.hotels_pricing_analysis
-- partition by created_at 
-- as

-- WITH
--   click_table AS (
--   SELECT
--     search_hotel_id,
--     provider_code,
--     click_id,
--     click_provider_code,
--     conversions_tracked
--   FROM
--     `wego-cloud.analysis.wego_rates_analysis`
--   WHERE
--     DATE(created_at) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--     AND click_id IS NOT NULL),
--   providers AS (
--   SELECT
--     *,
--     CASE
--       WHEN SUM(conversions_tracked) OVER(PARTITION BY search_hotel_id) > 0 THEN "booked result"
--     ELSE
--     "clicked result"
--   END
--     AS booking_type,
--     "clicked result" AS click_type
--   FROM
--     `wego-cloud.analysis.wego_rates_analysis`
--   WHERE
--     DATE(created_at) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
--   hotels_searches AS (
--   SELECT
--     _PARTITIONTIME AS created_at,
--     search_id,
--     session_id,
--   FROM
--     `wego-cloud.wego_analytics.hotels_searches`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)),
--   sessions AS (
--   SELECT
--     _PARTITIONTIME AS created_at,
--     session_id,
--     device_type,
--     app_version,
--     os_type,
--     channel,
--     market,
--     user_country_code,
--     ts_code
--   FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)),
--   t0 AS (
--   SELECT
--     providers.*,
--     sessions.* EXCEPT(created_at,
--       session_id),
--     COUNT(DISTINCT search_hotel_id) OVER(PARTITION BY site_code) AS searches_partition_site_code,
--   FROM
--     providers
--   LEFT JOIN
--     hotels_searches
--   ON
--     providers.search_id = hotels_searches.search_id
--   LEFT JOIN
--     sessions
--   ON
--     hotels_searches.session_id = sessions.session_id
--     AND DATE(hotels_searches.created_at) = DATE(sessions.created_at)),
--   t1 AS (
--   SELECT
--     created_at,
--     search_hotel_id,
--     hotel_name,
--     provider_code,
--     city_code,
--     city_name,
--     country_code,
--     country_name,
--     trip_category,
--     brand_id,
--     chain_id,
--     hotel_brand_code,
--     hotel_brand,
--     hotel_chain_code,
--     hotel_chain,
--     rooms_count,
--     guests_count,
--     check_in,
--     check_out,
--     hotel_stars,
--     device_type,
--     app_version,
--     ts_code,
--     os_type,
--     channel,
--     market,
--     user_country_code,
--     site_code,
--     AVG(searches_partition_site_code) AS searches_partition_site_code,
--     MIN(total_amount_per_night) AS total_amount_per_night,
--     /* minimum amount per provider on hotel level */ MIN(MIN(total_amount_per_night)) OVER(PARTITION BY search_hotel_id) AS hotel_min_rate /* minimum amount per hotel */
--   FROM
--     t0
--   GROUP BY
--     1,
--     2,
--     3,
--     4,
--     5,
--     6,
--     7,
--     8,
--     9,
--     10,
--     11,
--     12,
--     13,
--     14,
--     15,
--     16,
--     17,
--     18,
--     19,
--     20,
--     21,
--     22,
--     23,
--     24,
--     25,
--     26,
--     27,
--     28),
--   t2 AS (
--   SELECT
--     *,
--     COUNT(DISTINCT provider_code) OVER(PARTITION BY search_hotel_id) AS hotel_provider_count,
--     CASE
--       WHEN total_amount_per_night = hotel_min_rate THEN TRUE
--     ELSE
--     FALSE
--   END
--     AS is_hotel_cheapest_provider,
--     COUNT(DISTINCT
--     IF
--       (total_amount_per_night = hotel_min_rate,
--         provider_code,
--         NULL)) OVER (PARTITION BY search_hotel_id) AS hotel_cheapest_provider_count,
--     STRING_AGG(CAST(total_amount_per_night AS STRING), ',') OVER (PARTITION BY search_hotel_id ORDER BY total_amount_per_night RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hotel_rate_agg
--   FROM
--     t1),
--   t3 AS (
--   SELECT
--     *,
--   IF
--     (is_hotel_cheapest_provider
--       AND hotel_cheapest_provider_count = 1,
--       CAST(SPLIT(hotel_rate_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC),
--       hotel_min_rate) AS hotel_min_rate_exclude_own_rate
--   FROM
--     t2),
--   t4 AS (
--   SELECT
--     *,
--     CASE
--       WHEN hotel_provider_count = 1 THEN NULL /* if it's the only provider then NULL, but this case should have been filtered out */
--       WHEN is_hotel_cheapest_provider
--     AND hotel_cheapest_provider_count > 1 THEN 0 /* if >1 provider being cheapest then 0 parity */
--     ELSE
--     (total_amount_per_night - hotel_min_rate_exclude_own_rate) / total_amount_per_night * 100
--   END
--     hotel_rate_parity_pct
--   FROM
--     t3
--     --WHERE hotel_provider_count > 1
--     ),
--   t5 AS (
--   SELECT
--     t4.*,
--     click_id,
--     click_provider_code,
--     conversions_tracked,
--     CASE
--       WHEN hotel_rate_parity_pct < 0 THEN 1 /* 1 if it's the winner AND there's more than 1 provider */
--       WHEN hotel_rate_parity_pct >= 0 THEN 0 /* 0 if it's not the winner */
--     ELSE
--     NULL /* NULL if it's the only provider */
--   END
--     AS hotel_rate_won,
--     CASE
--       WHEN hotel_rate_parity_pct = 0 THEN 1
--     ELSE
--     0
--   END
--     AS hotel_rate_drew,
--     CASE
--       WHEN hotel_rate_parity_pct > 0 THEN 1
--     ELSE
--     0
--   END
--     AS hotel_rate_lost
--   FROM
--     t4
--   LEFT JOIN
--     click_table
--   ON
--     t4.provider_code = click_table.provider_code
--     AND t4.search_hotel_id = click_table.search_hotel_id )
-- SELECT
--   *
-- FROM
--   t5


-- daily append:


WITH
  click_table AS (
  SELECT
    search_hotel_id,
    provider_code,
    click_id,
    click_provider_code,
    conversions_tracked
  FROM
    `wego-cloud.analysis.wego_rates_analysis`
  WHERE
    DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND click_id IS NOT NULL),
  providers AS (
  SELECT
    *,
    CASE
      WHEN SUM(conversions_tracked) OVER(PARTITION BY search_hotel_id) > 0 THEN "booked result"
    ELSE
    "clicked result"
  END
    AS booking_type,
    "clicked result" AS click_type
  FROM
    `wego-cloud.analysis.wego_rates_analysis`
  WHERE
    DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
  hotels_searches AS (
  SELECT
    _PARTITIONTIME AS created_at,
    search_id,
    session_id,
  FROM
    `wego-cloud.wego_analytics.hotels_searches`
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)),
  sessions AS (
  SELECT
    _PARTITIONTIME AS created_at,
    session_id,
    device_type,
    app_version,
    os_type,
    channel,
    market,
    user_country_code,
    ts_code
  FROM
    `wego-cloud.wego_analytics.sessions`
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)),
  t0 AS (
  SELECT
    providers.*,
    sessions.* EXCEPT(created_at,
      session_id),
    COUNT(DISTINCT search_hotel_id) OVER(PARTITION BY site_code) AS searches_partition_site_code,
  FROM
    providers
  LEFT JOIN
    hotels_searches
  ON
    providers.search_id = hotels_searches.search_id
  LEFT JOIN
    sessions
  ON
    hotels_searches.session_id = sessions.session_id
    AND DATE(hotels_searches.created_at) = DATE(sessions.created_at)),
  t1 AS (
  SELECT
    created_at,
    search_hotel_id,
    hotel_name,
    provider_code,
    city_code,
    city_name,
    country_code,
    country_name,
    trip_category,
    brand_id,
    chain_id,
    hotel_brand_code,
    hotel_brand,
    hotel_chain_code,
    hotel_chain,
    rooms_count,
    guests_count,
    check_in,
    check_out,
    hotel_stars,
    device_type,
    app_version,
    ts_code,
    os_type,
    channel,
    market,
    user_country_code,
    site_code,
    AVG(searches_partition_site_code) AS searches_partition_site_code,
    MIN(total_amount_per_night) AS total_amount_per_night, /* minimum amount per provider on hotel level */ 
    MIN(MIN(total_amount_per_night)) OVER(PARTITION BY search_hotel_id) AS hotel_min_rate
  FROM
    t0
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28),
  t2 AS (
  SELECT
    *,
    COUNT(DISTINCT provider_code) OVER(PARTITION BY search_hotel_id) AS hotel_provider_count,
    CASE
      WHEN total_amount_per_night = hotel_min_rate THEN TRUE
    ELSE
    FALSE
  END
    AS is_hotel_cheapest_provider,
    COUNT(DISTINCT
    IF
      (total_amount_per_night = hotel_min_rate,
        provider_code,
        NULL)) OVER (PARTITION BY search_hotel_id) AS hotel_cheapest_provider_count,
    STRING_AGG(CAST(total_amount_per_night AS STRING), ',') OVER (PARTITION BY search_hotel_id ORDER BY total_amount_per_night RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hotel_rate_agg
  FROM
    t1),
  t3 AS (
  SELECT
    *,
  IF
    (is_hotel_cheapest_provider
      AND hotel_cheapest_provider_count = 1,
      CAST(SPLIT(hotel_rate_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC),
      hotel_min_rate) AS hotel_min_rate_exclude_own_rate
  FROM
    t2),
  t4 AS (
  SELECT
    *,
    CASE
      WHEN hotel_provider_count = 1 THEN NULL /* if it's the only provider then NULL, but this case should have been filtered out */
      WHEN is_hotel_cheapest_provider
    AND hotel_cheapest_provider_count > 1 THEN 0 /* if >1 provider being cheapest then 0 parity */
    ELSE
    (total_amount_per_night - hotel_min_rate_exclude_own_rate) / total_amount_per_night * 100
  END
    hotel_rate_parity_pct
  FROM
    t3
    --WHERE hotel_provider_count > 1
    ),
  t5 AS (
  SELECT
    t4.*,
    click_id,
    click_provider_code,
    conversions_tracked,
    CASE
      WHEN hotel_rate_parity_pct < 0 THEN 1 /* 1 if it's the winner AND there's more than 1 provider */
      WHEN hotel_rate_parity_pct >= 0 THEN 0 /* 0 if it's not the winner */
    ELSE
    NULL /* NULL if it's the only provider */
  END
    AS hotel_rate_won,
    CASE
      WHEN hotel_rate_parity_pct = 0 THEN 1
    ELSE
    0
  END
    AS hotel_rate_drew,
    CASE
      WHEN hotel_rate_parity_pct > 0 THEN 1
    ELSE
    0
  END
    AS hotel_rate_lost
  FROM
    t4
  LEFT JOIN
    click_table
  ON
    t4.provider_code = click_table.provider_code
    AND t4.search_hotel_id = click_table.search_hotel_id )
SELECT
  *
FROM
  t5
{% endraw %}
