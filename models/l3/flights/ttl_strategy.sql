{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : flights_rate_cache_ttl
-- Destination: flights_cache_analysis.ttl_strategy  (unchanged)
-- Schedule   : every mon 02:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('flights_cache_analysis', 'provider_fares_sample') }}
-- depends_on: {{ source('services_curiosity', 'provider_fares') }}
-- depends_on: {{ source('services_curiosity', 'search_requests') }}
-- depends_on: {{ source('services_curiosity', 'search_requests20240905') }}
{% raw %}
--- 1. INITIALISATION ---
-- DECLARE start_date DATE;
-- DECLARE end_date DATE DEFAULT "2024-12-18";
-- DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
-- DECLARE date_window INT64 DEFAULT 13; -- i.e. If 12 day range, put 11 for window

-- EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO start_date USING end_date, date_window;

-- CREATE OR REPLACE TABLE flights_cache_analysis.ttl_strategy AS (
-- SELECT * EXCEPT (changed_array),
-- changed_array[OFFSET(0)] as p0_changed,
-- changed_array[OFFSET(5)] as p5_changed,
-- changed_array[OFFSET(10)] as p10_changed,
-- changed_array[OFFSET(15)] as p15_changed,
-- changed_array[OFFSET(20)] as p20_changed,
-- changed_array[OFFSET(25)] as p25_changed,
-- changed_array[OFFSET(30)] as p30_changed,
-- changed_array[OFFSET(35)] as p35_changed,
-- changed_array[OFFSET(40)] as p40_changed,
-- changed_array[OFFSET(45)] as p45_changed,
-- changed_array[OFFSET(50)] as p50_changed,
-- changed_array[OFFSET(55)] as p55_changed,
-- changed_array[OFFSET(60)] as p60_changed,
-- changed_array[OFFSET(65)] as p65_changed,
-- changed_array[OFFSET(70)] as p70_changed,
-- changed_array[OFFSET(75)] as p75_changed,
-- changed_array[OFFSET(80)] as p80_changed,
-- changed_array[OFFSET(85)] as p85_changed,
-- changed_array[OFFSET(90)] as p90_changed,
-- changed_array[OFFSET(95)] as p95_changed,
-- changed_array[OFFSET(100)] as p100_changed,
-- changed_array[OFFSET(75)] - changed_array[OFFSET(25)] as iqr_changed,
-- start_date,
-- end_date,
-- date_window + 1 as date_window,
-- processing_timestamp
-- FROM 
--   (SELECT 
--   od,
--   advance_purchase,
--   SUM(1) as total_changed_count, 
--   APPROX_QUANTILES(CAST(IF(since_last_change_min != 0, since_last_change_min, NULL) AS INT64), 100) as changed_array,
--   FROM 
--     (SELECT od, advance_purchase, key,prev_key, 
--     is_changed,
--     IF(is_changed != 0, TIMESTAMP_DIFF(TIMESTAMP(search_time), TIMESTAMP(MAX(IF(is_changed != 0, search_time, NULL)) OVER (PARTITION BY key ORDER BY search_time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), MINUTE), NULL) as since_last_change_min,
--     FROM 
--       (SELECT *, 
--       CASE  WHEN key = prev_key THEN CASE  WHEN total_amount!=prev_total_amount THEN 1 ELSE 0 END ELSE 0 END as is_changed,
--       FROM 
--         (SELECT  
--         f.flight_id,
--         f.departure_airport_code,
--         f.arrival_airport_code,
--         f.od,
--         r.search_id,
--         r.search_time as search_time,
--         r.time_slot,
--         r.timestamp AS timestamp,
--         f.advance_purchase,
--         f.price_total_amount AS total_amount,
--         LAG(f.price_total_amount) OVER (ORDER BY flight_id, departure_airport_code, arrival_airport_code, search_time) as prev_total_amount,
--         f.key as key,
--         LAG(key) OVER (ORDER BY flight_id, departure_airport_code, arrival_airport_code, search_time) as prev_key,

--         CONCAT(f.advance_purchase,"x",r.time_slot) as advance_purchase_x_time_slot
--         FROM 
--           ((SELECT 
--           search_id,
--           id as provider_fare_id,
--           created_at,
--           flight_id, 
--           provider_code, 
--           CONCAT(provider_flight.provider_legs[0].provider_segments[0].departure_airport_code,":", ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code) as od,
--           CONCAT(flight_id,":",provider_flight.provider_legs[0].provider_segments[0].departure_airport_code,":", ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code) as key,
--           provider_flight.provider_legs[0].provider_segments[0].departure_airport_code as departure_airport_code,
--           ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code as arrival_airport_code,
--           provider_flight,
--           DATE_DIFF( DATE(SPLIT(provider_flight.provider_legs[0].provider_segments[0].departure_time, '+')[OFFSET(0)]),DATE(SPLIT(provider_flight.created_at, '+')[OFFSET(0)]),DAY) AS advance_purchase,
          
--           price.amount as price_amount,
--           price.currency_code as price_currency_code,
--           price.total_amount as price_total_amount,
--           FROM `wego-cloud.services_curiosity.provider_fares*`
--           -- WHERE _TABLE_SUFFIX BETWEEN "20240801" AND "20240802"
--           WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
--           -- FROM `wego-cloud.flights_cache_analysis.provider_fares_sample` 
--           ) as f

--           LEFT JOIN

--           (SELECT search.id as search_id, search.device_type as device_type, search.trip_type as trip_type,timestamp, FORMAT_DATETIME("%F %T", search.created_at) AS search_time, FORMAT_TIMESTAMP('%H', TIMESTAMP(timestamp)) AS time_slot,
--           -- FROM `services_curiosity.search_requests20240905`
--           FROM `services_curiosity.search_requests*`
--           -- WHERE _TABLE_SUFFIX BETWEEN "20240801" AND "20240802"
--           WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
--           ) as  r
--           ON f.search_id = r.search_id
--           )
--         WHERE
--         f.provider_code = 'd.wego.com'
--         AND ARRAY_LENGTH(f.provider_flight.provider_legs) = 1
--         AND f.advance_purchase <= 30
--         AND r.trip_type = 'ONEWAY'

--         ORDER BY
--         flight_id,
--         departure_airport_code,
--         arrival_airport_code,
--         search_time
--         )
--       )

--     -- WHERE 
--     -- key = 'XY105~1:RUH:GIZ'
--     )
--   WHERE is_changed = 1
--   GROUP BY od, advance_purchase
--   )
-- ORDER BY 
-- od,
-- advance_purchase,
-- total_changed_count desc
-- )

-- -- 2. SCHEDULED INSERT
DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE date_before_processing_date INT64 DEFAULT 1;
DECLARE date_window INT64 DEFAULT 13; -- i.e. If 12 day range, put 11 for window

EXECUTE IMMEDIATE "SELECT DATE_SUB(DATE(?), INTERVAL ? DAY)" INTO end_date USING processing_timestamp, date_before_processing_date;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO start_date USING end_date, date_window;


INSERT flights_cache_analysis.ttl_strategy

SELECT * EXCEPT (changed_array),
changed_array[OFFSET(0)] as p0_changed,
changed_array[OFFSET(5)] as p5_changed,
changed_array[OFFSET(10)] as p10_changed,
changed_array[OFFSET(15)] as p15_changed,
changed_array[OFFSET(20)] as p20_changed,
changed_array[OFFSET(25)] as p25_changed,
changed_array[OFFSET(30)] as p30_changed,
changed_array[OFFSET(35)] as p35_changed,
changed_array[OFFSET(40)] as p40_changed,
changed_array[OFFSET(45)] as p45_changed,
changed_array[OFFSET(50)] as p50_changed,
changed_array[OFFSET(55)] as p55_changed,
changed_array[OFFSET(60)] as p60_changed,
changed_array[OFFSET(65)] as p65_changed,
changed_array[OFFSET(70)] as p70_changed,
changed_array[OFFSET(75)] as p75_changed,
changed_array[OFFSET(80)] as p80_changed,
changed_array[OFFSET(85)] as p85_changed,
changed_array[OFFSET(90)] as p90_changed,
changed_array[OFFSET(95)] as p95_changed,
changed_array[OFFSET(100)] as p100_changed,
changed_array[OFFSET(75)] - changed_array[OFFSET(25)] as iqr_changed,
start_date,
end_date,
date_window + 1 as date_window,
processing_timestamp
FROM 
  (SELECT 
  od,
  advance_purchase,
  SUM(1) as total_changed_count, 
  APPROX_QUANTILES(CAST(IF(since_last_change_min != 0, since_last_change_min, NULL) AS INT64), 100) as changed_array,
  FROM 
    (SELECT od, advance_purchase, key,prev_key, 
    is_changed,
    IF(is_changed != 0, TIMESTAMP_DIFF(TIMESTAMP(search_time), TIMESTAMP(MAX(IF(is_changed != 0, search_time, NULL)) OVER (PARTITION BY key ORDER BY search_time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), MINUTE), NULL) as since_last_change_min,
    FROM 
      (SELECT *, 
      CASE  WHEN key = prev_key THEN CASE  WHEN total_amount!=prev_total_amount THEN 1 ELSE 0 END ELSE 0 END as is_changed,
      FROM 
        (SELECT  
        f.flight_id,
        f.departure_airport_code,
        f.arrival_airport_code,
        f.od,
        r.search_id,
        r.search_time as search_time,
        r.time_slot,
        r.timestamp AS timestamp,
        f.advance_purchase,
        f.price_total_amount AS total_amount,
        LAG(f.price_total_amount) OVER (ORDER BY flight_id, departure_airport_code, arrival_airport_code, search_time) as prev_total_amount,
        f.key as key,
        LAG(key) OVER (ORDER BY flight_id, departure_airport_code, arrival_airport_code, search_time) as prev_key,

        CONCAT(f.advance_purchase,"x",r.time_slot) as advance_purchase_x_time_slot
        FROM 
          ((SELECT 
          search_id,
          id as provider_fare_id,
          created_at,
          flight_id, 
          provider_code, 
          CONCAT(provider_flight.provider_legs[0].provider_segments[0].departure_airport_code,":", ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code) as od,
          CONCAT(flight_id,":",provider_flight.provider_legs[0].provider_segments[0].departure_airport_code,":", ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code) as key,
          provider_flight.provider_legs[0].provider_segments[0].departure_airport_code as departure_airport_code,
          ARRAY_REVERSE(provider_flight.provider_legs[0].provider_segments)[0].arrival_airport_code as arrival_airport_code,
          provider_flight,
          DATE_DIFF( DATE(SPLIT(provider_flight.provider_legs[0].provider_segments[0].departure_time, '+')[OFFSET(0)]),DATE(SPLIT(provider_flight.created_at, '+')[OFFSET(0)]),DAY) AS advance_purchase,
          
          price.amount as price_amount,
          price.currency_code as price_currency_code,
          price.total_amount as price_total_amount,
          FROM `wego-cloud.services_curiosity.provider_fares*`
          -- WHERE _TABLE_SUFFIX BETWEEN "20240801" AND "20240802"
          WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
          -- FROM `wego-cloud.flights_cache_analysis.provider_fares_sample` 
          ) as f

          LEFT JOIN

          (SELECT search.id as search_id, search.device_type as device_type, search.trip_type as trip_type,timestamp, FORMAT_DATETIME("%F %T", search.created_at) AS search_time, FORMAT_TIMESTAMP('%H', TIMESTAMP(timestamp)) AS time_slot,
          -- FROM `services_curiosity.search_requests20240905`
          FROM `services_curiosity.search_requests*`
          -- WHERE _TABLE_SUFFIX BETWEEN "20240801" AND "20240802"
          WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
          ) as  r
          ON f.search_id = r.search_id
          )
        WHERE
        f.provider_code = 'd.wego.com'
        AND ARRAY_LENGTH(f.provider_flight.provider_legs) = 1
        AND f.advance_purchase <= 30
        AND r.trip_type = 'ONEWAY'

        ORDER BY
        flight_id,
        departure_airport_code,
        arrival_airport_code,
        search_time
        )
      )

    -- WHERE 
    -- key = 'XY105~1:RUH:GIZ'
    )
  WHERE is_changed = 1
  GROUP BY od, advance_purchase
  )
ORDER BY 
od,
advance_purchase,
total_changed_count desc
{% endraw %}
