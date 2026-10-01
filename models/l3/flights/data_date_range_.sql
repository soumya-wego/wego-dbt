{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flight_sort_order_bqml
-- Destination: flight_sort_order_ml.data_date_range_  (unchanged)
-- Schedule   : every wed 02:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- ## FLIGHT SORT ORDER RANDOM FOREST

-- a. Manual Date Range
DECLARE null_proportion FLOAT64;
DECLARE start_date DATE DEFAULT "2024-02-13";
DECLARE end_date DATE DEFAULT "2024-02-13";
DECLARE secondary_start_date DATE;
DECLARE secondary_end_date DATE;
DECLARE start_date_string STRING;
DECLARE end_date_string STRING;
DECLARE secondary_start_date_string STRING;
DECLARE secondary_end_date_string STRING;
DECLARE start_date_suffix STRING;
DECLARE end_date_suffix STRING;
DECLARE secondary_start_date_suffix STRING;
DECLARE secondary_end_date_suffix STRING;
DECLARE processing_date_suffix STRING;
DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE processing_date DATE DEFAULT CURRENT_DATE();
DECLARE date_before_processing_date INT64 DEFAULT 1;
DECLARE date_windows_gap INT64 DEFAULT 0;
DECLARE date_window INT64 DEFAULT 92; -- i.e. If 12 day range, put 11 for window
DECLARE secondary_date_window INT64 DEFAULT 89; -- i.e. If 12 day range, put 11 for window
DECLARE i INT64 DEFAULT 0;
DECLARE model_count INT64;
DECLARE model_list ARRAY<STRING>;
DECLARE model_name STRING;

EXECUTE IMMEDIATE "SELECT DATE_SUB(DATE(?), INTERVAL ? DAY)" INTO end_date USING processing_timestamp, date_before_processing_date;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO start_date USING end_date, date_window;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO secondary_end_date USING start_date, date_windows_gap;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO secondary_start_date USING secondary_end_date, secondary_date_window;

SET start_date_string = CAST(start_date AS STRING);
SET end_date_string = CAST(end_date AS STRING);
SET secondary_start_date_string = CAST(secondary_start_date AS STRING);
SET secondary_end_date_string = CAST(secondary_end_date AS STRING);
SET start_date_suffix = FORMAT_DATE("%Y%m%d", start_date);
SET end_date_suffix = FORMAT_DATE("%Y%m%d", end_date);
SET secondary_start_date_suffix = FORMAT_DATE("%Y%m%d", secondary_start_date);
SET secondary_end_date_suffix = FORMAT_DATE("%Y%m%d", secondary_end_date);
SET processing_date_suffix = FORMAT_DATE("%Y%m%d", processing_date);

------- -1. Record Date Ranges ------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.data_date_range_%s AS (
SELECT 
@start_date AS start_date,
@end_date AS end_date, 
@date_window + 1 as date_window,
@secondary_start_date as secondary_start_date,
@secondary_end_date as secondary_end_date,
@secondary_date_window + 1 as secondary_date_window,
@processing_date AS processing_date
)
"""
, processing_date_suffix)
USING start_date AS start_date, 
end_date AS end_date, 
date_window AS date_window, 
secondary_start_date AS secondary_start_date, 
secondary_end_date AS secondary_end_date, 
secondary_date_window AS secondary_date_window, 
processing_date AS processing_date;

-- -- -- ---- 0. Generate Training Dataset ------
-- Approx 121GB Processing 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.training_data_%s AS (
SELECT *,
  0.5 * total_clicks_norm + 0.5 * ctr_norm AS clicks_ctr_score 
  FROM
  (SELECT *,
  SAFE_DIVIDE(total_clicks - MIN(total_clicks) OVER (), MAX(total_clicks) OVER () - MIN(total_clicks) OVER ()) as total_clicks_norm,
  SAFE_DIVIDE(ctr - MIN(ctr) OVER (), MAX(ctr) OVER () - MIN(ctr) OVER ()) as ctr_norm,
  FROM
    (SELECT 
    code,
    route,
    route_city,
    route_country,
    full_route,
    full_route_city,
    full_route_country,
    route_country_count,
    IF(route_country_count > 1, "International", "Domestic") as route_type,
    route_direct,
    route_trip_type,
    route_total_legs,
    route_total_segments,
    leg_full_route,
    leg_full_route_city,
    leg_full_route_country,
    leg_route_direct,
    leg_first_departure_airport_code,
    leg_first_arrival_airport_code,
    legs_order,
    leg_total_segments,
    leg_in_air_min,
    leg_time_min,
    leg_min_stop_over,
    leg_max_stop_over,
    leg_min_departure_time,
    leg_min_departure_time_local,
    leg_min_departure_time_local_hour,
    leg_max_arrival_time,
    leg_max_arrival_time_local,
    leg_max_arrival_time_local_hour,
    IF(distinct_airline_count > 1, "Multiple", "Single") AS multiple_airlines,
    distinct_airline_count,
    IF(distinct_alliance_count > 1, "Multiple", "Single") AS multiple_alliances,
    distinct_alliance_count,
    lcc_indicator,
    CASE WHEN distinct_alliance_count > 1 AND lcc_indicator = 1 THEN "Mixed"
    WHEN distinct_alliance_count = 1 AND lcc_indicator = 1 THEN "Pure LCC"
    WHEN distinct_alliance_count = 1 AND lcc_indicator = 0 THEN "Pure Normal Airline"
    END AS alliance_cat,
    airline_code_0,
    airline_rating_0,
    aircraft_code_0,
    aircraft_rating_0,
    segment_duration_min_0,
    airline_code_1,
    airline_rating_1,
    aircraft_code_1,
    aircraft_rating_1,
    segment_duration_min_1,
    airline_code_2,
    airline_rating_2,
    aircraft_code_2,
    aircraft_rating_2,
    segment_duration_min_2,
    airline_code_3,
    airline_rating_3,
    aircraft_code_3,
    aircraft_rating_3,
    segment_duration_min_3,
    airline_code_4,
    airline_rating_4,
    aircraft_code_4,
    aircraft_rating_4,
    segment_duration_min_4,
    airline_code_5,
    airline_rating_5,
    aircraft_code_5,
    aircraft_rating_5,
    segment_duration_min_5,
    -- 6 Existing Variables
    leg_min_departure_time_local_min,
    leg_max_arrival_time_local_min,
    leg_in_air_hours,
    leg_time_hours,
    leg_min_stop_over_hours,
    leg_max_stop_over_hours,
    -- stop_count_score,
    -- duration_score,
    -- stop_over_duration_min_score,
    -- stop_over_duration_max_score,
    -- departure_time_score,
    -- arrival_time_score,
    leg_experience_score_calculated,
    leg_experience_score,
    itinerary_experience_score,
    aircraft_rating_score,
    airline_rating_score,
    price_amount_usd,
    SAFE_DIVIDE(price_amount_usd, route_total_legs) as leg_price_amount_usd,
    SAFE_DIVIDE(price_amount_usd, route_average_price) AS price_ratio,
    total_impressions,
    min_impression_position,
    avg_impression_position,
    max_impression_position,
    pct_labeled_best,
    pct_labeled_cheapest,
    fastest_leg_time,
    average_layover_time,
    SAFE_DIVIDE(leg_time_min, fastest_leg_time) AS leg_time_ratio,
    total_searches,
    IFNULL(total_clicks,0) AS total_clicks,
    total_conversions,
    IFNULL(100*SAFE_DIVIDE(total_clicks, total_searches),0) AS CTR,
    100*SAFE_DIVIDE(total_conversions, total_clicks) AS CVR,
    total_revenue,
    total_gmv,
    100*IFNULL(SAFE_DIVIDE(total_clicks, total_searches), 0) + 3 * IFNULL(SAFE_DIVIDE(total_conversions, total_clicks), 0) AS itinerary_score,
    leg_distance_km,
    RANGE_BUCKET(leg_distance_km,[1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 15000, 20000]) as leg_distance_bucket
    FROM
      (SELECT itinerary_info.* EXCEPT (leg_experience_score,itinerary_experience_score), 
      

      -- COMPONENTS OF LEG EXPERIENCE SCORE (Calculation Verification) --
      -- (leg_total_segments - 1) * 100 as stop_count_score,
      -- IF(leg_time_hours < 4,1,0) * 0
      -- + IF(leg_time_hours >= 4,1,0) * (leg_time_hours - 4.0) * 4 as duration_score,
      -- IF(leg_min_stop_over_hours > 2,1,0) * 0 
      -- + IF(leg_min_stop_over_hours <= 2 AND leg_min_stop_over_hours > 1,1,0) * (2 - leg_min_stop_over_hours) * 10 -- Short Stopover Penalty
      -- + IF(leg_min_stop_over_hours <= 1 AND leg_min_stop_over_hours > 0,1,0) * ((1 - leg_min_stop_over_hours) * 30 + (2 - leg_min_stop_over_hours) * 10) as stop_over_duration_min_score,
      -- IF(leg_max_stop_over_hours < 4,1,0) * 0 
      -- + IF(leg_max_stop_over_hours >= 4,1,0) * (leg_max_stop_over_hours - 4) * 10 as stop_over_duration_max_score,
      -- IF(leg_min_departure_time_local_min < 6 * 60,1,0) * 20 as departure_time_score,
      -- IF(leg_max_arrival_time_local_min > 22 * 60,1,0) * 10 as arrival_time_score,

      -- OVERALL CALCULATION OF LEG EXPERIENCE SCORE --
      1000 
      -(
      -- STOP COUNT SCORE
      (leg_total_segments - 1) * 100 -- Stopover Penalty
      -- DURATION SCORE 
      + IF(leg_time_hours < 4,1,0) * 0
      + IF(leg_time_hours >= 4,1,0) * (leg_time_hours - 4.0) * 4
      -- -- STOP OVER DURATION MIN SCORE
      + IF(leg_min_stop_over_hours > 2,1,0) * 0 
      + IF(leg_min_stop_over_hours <= 2 AND leg_min_stop_over_hours > 1,1,0) * (2 - leg_min_stop_over_hours) * 10 -- Short Stopover Penalty
      + IF(leg_min_stop_over_hours <= 1 AND leg_min_stop_over_hours > 0,1,0) * ((1 - leg_min_stop_over_hours) * 30 + (2 - leg_min_stop_over_hours) * 10)
      -- STOP OVER DURATION MAX SCORE
      + IF(leg_max_stop_over_hours < 4,1,0) * 0 
      + IF(leg_max_stop_over_hours >= 4,1,0) * (leg_max_stop_over_hours - 4) * 10
      -- DEPARTURE TIME SCORE
      + IF(leg_min_departure_time_local_min < 6 * 60,1,0) * 20
      -- ARRIVAL TIME SCORE
      + IF(leg_max_arrival_time_local_min > 22 * 60,1,0) * 10
      ) as leg_experience_score_calculated,
      leg_experience_score,

      itinerary_experience_score,

      -- AIRCRAFT RATING SCORE
      IF(leg_total_segments >= 1,1,0) * IFNULL((IFNULL(aircraft_rating_0,3) - 5) * SAFE_DIVIDE(segment_duration_min_0,60),0) * 2
      + IF(leg_total_segments >= 2,1,0) * IFNULL((IFNULL(aircraft_rating_1,3) - 5) * SAFE_DIVIDE(segment_duration_min_1,60),0) * 2
      + IF(leg_total_segments >= 3,1,0) * IFNULL((IFNULL(aircraft_rating_2,3) - 5) * SAFE_DIVIDE(segment_duration_min_2,60),0) * 2
      + IF(leg_total_segments >= 4,1,0) * IFNULL((IFNULL(aircraft_rating_3,3) - 5) * SAFE_DIVIDE(segment_duration_min_3,60),0) * 2
      + IF(leg_total_segments >= 5,1,0) * IFNULL((IFNULL(aircraft_rating_4,3) - 5) * SAFE_DIVIDE(segment_duration_min_4,60),0) * 2
      + IF(leg_total_segments >= 6,1,0) * IFNULL((IFNULL(aircraft_rating_5,3) - 5) * SAFE_DIVIDE(segment_duration_min_5,60),0) * 2
      AS aircraft_rating_score,

      -- AIRLINE RATING SCORE
      IF(leg_total_segments >= 1, 1, 0) * IFNULL((IFNULL(airline_rating_0,3) - 5) * SAFE_DIVIDE(segment_duration_min_0, 60), 0) * 2
      + IF(leg_total_segments >= 2, 1, 0) * IFNULL((IFNULL(airline_rating_1,3) - 5) * SAFE_DIVIDE(segment_duration_min_1, 60), 0) * 2
      + IF(leg_total_segments >= 3, 1, 0) * IFNULL((IFNULL(airline_rating_2,3) - 5) * SAFE_DIVIDE(segment_duration_min_2, 60), 0) * 2
      + IF(leg_total_segments >= 4, 1, 0) * IFNULL((IFNULL(airline_rating_3,3) - 5) * SAFE_DIVIDE(segment_duration_min_3, 60), 0) * 2
      + IF(leg_total_segments >= 5, 1, 0) * IFNULL((IFNULL(airline_rating_4,3) - 5) * SAFE_DIVIDE(segment_duration_min_4, 60), 0) * 2
      + IF(leg_total_segments >= 6, 1, 0) * IFNULL((IFNULL(airline_rating_5,3) - 5) * SAFE_DIVIDE(segment_duration_min_5, 60), 0) * 2
      AS airline_rating_score,

      performance_info.* EXCEPT (itinerary_id),
      search_info.* EXCEPT (first_departure_airport_code, first_arrival_airport_code),
      total_impressions,
      min_impression_position,
      avg_impression_position,
      max_impression_position,
      pct_labeled_best,
      pct_labeled_cheapest,
      avg(price_amount_usd) over (partition by full_route, route_trip_type) AS route_average_price,

      FROM
        (SELECT *,
        IF(route_total_legs < route_total_segments, "Indirect", "Direct") as route_direct,
        IF(leg_total_segments > 1, "Indirect", "Direct") as leg_route_direct,
        MAX(IF(legs_order = 0, CONCAT(leg_first_departure_airport_code,"-",leg_first_arrival_airport_code),NULL)) OVER (PARTITION BY code) as route,
        MAX(IF(legs_order = 0, CONCAT(leg_first_departure_location_code,"-",leg_first_arrival_location_code),NULL)) OVER (PARTITION BY code) as route_city,
        MAX(IF(legs_order = 0, CONCAT(leg_first_departure_country_code,"-",leg_first_arrival_country_code),NULL)) OVER (PARTITION BY code) as route_country,
        IF(route_total_legs > 1,"Roundtrip","Oneway") as route_trip_type, 
        MAX(IF(legs_order = 0, leg_first_departure_airport_code, NULL)) OVER (PARTITION BY code) as route_first_departure_airport_code,
        MAX(IF(legs_order = 0, leg_first_arrival_airport_code, NULL)) OVER (PARTITION BY code) as route_first_arrival_airport_code,
        (SELECT COUNT(DISTINCT countries) FROM UNNEST(SPLIT(REPLACE(full_route_country,"=","-"),"-")) as countries) as route_country_count
        FROM
          (SELECT *, 
          STRING_AGG(leg_full_route,"=") OVER (PARTITION BY code ORDER BY legs_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as full_route,
          STRING_AGG(leg_full_route_city,"=") OVER (PARTITION BY code ORDER BY legs_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as full_route_city,
          STRING_AGG(leg_full_route_country,"=") OVER (PARTITION BY code ORDER BY legs_order ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as full_route_country,
          MAX(legs_order) OVER (PARTITION BY code) + 1 as route_total_legs,
          SUM(leg_total_segments) OVER (PARTITION BY code) as route_total_segments,
          EXTRACT(HOUR FROM leg_min_departure_time_local) * 60 + EXTRACT(MINUTE FROM leg_min_departure_time_local) as leg_min_departure_time_local_min,
          EXTRACT(HOUR FROM leg_min_departure_time_local) as leg_min_departure_time_local_hour,
          EXTRACT(HOUR FROM leg_max_arrival_time_local) * 60 + EXTRACT(MINUTE FROM leg_max_arrival_time_local) as leg_max_arrival_time_local_min,
          EXTRACT(HOUR FROM leg_max_arrival_time_local) as leg_max_arrival_time_local_hour,
          SAFE_DIVIDE(leg_in_air_min,60) as leg_in_air_hours,
          SAFE_DIVIDE(leg_time_min,60) as leg_time_hours, 
          SAFE_DIVIDE(leg_min_stop_over,60) as leg_min_stop_over_hours,
          SAFE_DIVIDE(leg_max_stop_over,60) as leg_max_stop_over_hours,
          MIN(leg_time_min) OVER (partition by leg_full_route, leg_total_segments) as fastest_leg_time,
          SAFE_DIVIDE((leg_time_min - leg_in_air_min),(leg_total_segments - 1)) AS average_layover_time
            FROM
            (-- Get components of experience score at legs level
            SELECT 
            code,
            legs_order,
            STRING_AGG(IF(overall_legs_order > 1, arrival_airport_code,CONCAT(departure_airport_code,"-",arrival_airport_code)),"-" ORDER BY overall_order) as leg_full_route,
            STRING_AGG(IF(overall_legs_order > 1, arrival_location_code,CONCAT(departure_location_code,"-",arrival_location_code)),"-" ORDER BY overall_order) as leg_full_route_city,
            STRING_AGG(IF(overall_legs_order > 1, arrival_country_code,CONCAT(departure_country_code,"-",arrival_country_code)),"-" ORDER BY overall_order) as leg_full_route_country,
            MAX(segments_order) + 1 as leg_total_segments,
            SUM(in_air_min) as leg_in_air_min,
            MAX(leg_time_min) as leg_time_min,
            IFNULL(MIN(layover_time),0) as leg_min_stop_over,
            IFNULL(MAX(layover_time),0) as leg_max_stop_over,
            MIN(IF(overall_legs_order = 1, departure_airport_code, NULL)) as leg_first_departure_airport_code,
            MIN(IF(overall_legs_order_reverse = 1, arrival_airport_code, NULL)) as leg_first_arrival_airport_code,
            MIN(IF(overall_legs_order = 1, departure_location_code, NULL)) as leg_first_departure_location_code,
            MIN(IF(overall_legs_order_reverse = 1, arrival_location_code, NULL)) as leg_first_arrival_location_code,
            MIN(IF(overall_legs_order = 1, departure_country_code, NULL)) as leg_first_departure_country_code,
            MIN(IF(overall_legs_order_reverse = 1, arrival_country_code, NULL)) as leg_first_arrival_country_code,
            MIN(IF(overall_legs_order = 1, departure_time, NULL)) as leg_min_departure_time,
            MIN(IF(overall_legs_order = 1, DATETIME(departure_time, departure_timezone), NULL)) as leg_min_departure_time_local,
            MAX(IF(overall_legs_order_reverse = 1, arrival_time, NULL)) as leg_max_arrival_time,
            MAX(IF(overall_legs_order_reverse = 1, DATETIME(arrival_time, arrival_timezone), NULL)) as leg_max_arrival_time_local,
            MAX(experience_score) as leg_experience_score,
            MAX(itinerary_experience_score) as itinerary_experience_score,
            MAX(airline_code_0) as airline_code_0,
            MAX(airline_rating_0) as airline_rating_0,
            MAX(aircraft_code_0) as aircraft_code_0,
            MAX(aircraft_rating_0) as aircraft_rating_0,
            MAX(segment_duration_min_0) as segment_duration_min_0,
            MAX(airline_code_1) as airline_code_1,
            MAX(airline_rating_1) as airline_rating_1,
            MAX(aircraft_code_1) as aircraft_code_1,
            MAX(aircraft_rating_1) as aircraft_rating_1,
            MAX(segment_duration_min_1) as segment_duration_min_1,
            MAX(airline_code_2) as airline_code_2,
            MAX(airline_rating_2) as airline_rating_2,
            MAX(aircraft_code_2) as aircraft_code_2,
            MAX(aircraft_rating_2) as aircraft_rating_2,
            MAX(segment_duration_min_2) as segment_duration_min_2,
            MAX(airline_code_3) as airline_code_3,
            MAX(airline_rating_3) as airline_rating_3,
            MAX(aircraft_code_3) as aircraft_code_3,
            MAX(aircraft_rating_3) as aircraft_rating_3,
            MAX(segment_duration_min_3) as segment_duration_min_3,
            MAX(airline_code_4) as airline_code_4,
            MAX(airline_rating_4) as airline_rating_4,
            MAX(aircraft_code_4) as aircraft_code_4,
            MAX(aircraft_rating_4) as aircraft_rating_4,
            MAX(segment_duration_min_4) as segment_duration_min_4,
            MAX(airline_code_5) as airline_code_5,
            MAX(airline_rating_5) as airline_rating_5,
            MAX(aircraft_code_5) as aircraft_code_5,
            MAX(aircraft_rating_5) as aircraft_rating_5,
            MAX(segment_duration_min_5) as segment_duration_min_5,
            MAX(price_amount_usd) as price_amount_usd,
            MAX(distinct_airline_count) as distinct_airline_count,
            MAX(distinct_alliance_count) as distinct_alliance_count,
            MAX(lcc_indicator) as lcc_indicator,
            SUM(segment_distance_km) as leg_distance_km
            FROM
              (SELECT * EXCEPT (airline_table_code, aircraft_table_code),
              COUNT(DISTINCT a.airline_code) OVER (PARTITION BY code, legs_order) as distinct_airline_count,
              COUNT(DISTINCT Alliance) OVER (PARTITION BY code, legs_order) as distinct_alliance_count,
              COUNT(DISTINCT IF(lcc_alliance = "lcc", lcc_alliance, NULL)) OVER (PARTITION BY code, legs_order) as lcc_indicator,
              CASE
              WHEN g.departure_latitude IS NOT NULL AND g.departure_longitude IS NOT NULL
              AND h.arrival_latitude IS NOT NULL AND h.arrival_longitude IS NOT NULL
              THEN CAST(ST_DISTANCE(
              ST_GEOGPOINT(g.departure_longitude, g.departure_latitude),
              ST_GEOGPOINT(h.arrival_longitude, h.arrival_latitude)
              ) / 1000 AS INT64)
              ELSE NULL
              END AS segment_distance_km,
              FROM
              (SELECT t.code as code,
              legs_unnested.order as legs_order, 
              segments_unnested.order as segments_order, 
              segments_unnested.order as segments_order_pivot, 
              segments_unnested.departure_airport.code as departure_airport_code,
              segments_unnested.arrival_airport.code as arrival_airport_code,
              segments_unnested.airline.code as airline_code,
              segments_unnested.departure_time as departure_time,
              segments_unnested.departure_timezone as departure_timezone,
              segments_unnested.arrival_time as arrival_time,
              segments_unnested.arrival_timezone as arrival_timezone,
              segments_unnested.aircraft.code as aircraft_code,
              ROW_NUMBER () OVER (PARTITION BY code ,legs_unnested.order ORDER BY segments_unnested.order) AS overall_legs_order,
              ROW_NUMBER () OVER (PARTITION BY code ,legs_unnested.order ORDER BY segments_unnested.order DESC) AS overall_legs_order_reverse,
              ROW_NUMBER () OVER (PARTITION BY code ORDER BY legs_unnested.order, segments_unnested.order) AS overall_order,
              ROW_NUMBER () OVER (PARTITION BY code ORDER BY legs_unnested.order DESC, segments_unnested.order DESC) AS overall_order_reverse,
              -- LAG(segments_unnested.arrival_time) OVER (PARTITION BY code, legs_unnested.order ORDER BY legs_unnested.order, segments_unnested.order) as arrival_time_lag,
              TIMESTAMP_DIFF(MAX(segments_unnested.arrival_time) OVER (PARTITION BY code, legs_unnested.order),
              MIN(segments_unnested.departure_time) OVER (PARTITION BY code, legs_unnested.order)
              , MINUTE) AS leg_time_min,
              TIMESTAMP_DIFF(segments_unnested.arrival_time, segments_unnested.departure_time, MINUTE) AS in_air_min,
              TIMESTAMP_DIFF(segments_unnested.arrival_time, segments_unnested.departure_time, MINUTE) AS in_air_min_pivot,
              TIMESTAMP_DIFF(segments_unnested.departure_time, LAG(segments_unnested.arrival_time) OVER (PARTITION BY code, legs_unnested.order ORDER BY legs_unnested.order, segments_unnested.order), MINUTE) AS layover_time,
              legs_unnested.experience_score as experience_score,
              t.experience_score as itinerary_experience_score
              FROM
                (SELECT *,
                ROW_NUMBER () OVER (PARTITION BY code) as unique_code
                -- FROM `wego-cloud.flight_sort_order_ml.trips_data` 
                -- FROM `wego-cloud.services_curiosity.trips*`
                -- WHERE _TABLE_SUFFIX BETWEEN 
                -- WHERE _TABLE_SUFFIX = "20240312"
                FROM 
                  (SELECT * FROM `wego-cloud.flight_popular_itineraries.clicked_trips`
                  WHERE DATE(created_at) BETWEEN "%s" AND "%s"

                  UNION ALL 

                  SELECT * FROM
                    (SELECT * FROM `wego-cloud.flight_popular_itineraries.non_clicked_trips` 
                    WHERE DATE(created_at) BETWEEN "%s" AND "%s"
                    LIMIT 2000000 -- Limit as too many itineraries non-clicked
                    ) 
                  )
                ) as t,
              UNNEST(legs) as legs_unnested, 
              UNNEST(legs_unnested.segments) as segments_unnested
              WHERE unique_code = 1
              -- AND code = "3L124~4=QR1055~3-QR536~4"
              ) as a

              LEFT JOIN
              (SELECT itinerary_id,  
              SUM(avg_price_amount_usd * rates_count) / sum(rates_count) AS price_amount_usd,  -- cannot just use avg: avg_price already aggreagted and the rates used to calculate it is diff
              MIN(min_price_amount_usd) AS min_price_amount_usd,
              MAX(max_price_amount_usd) AS max_price_amount_usd,
              FROM
                (SELECT *
                FROM `wego-cloud.flight_popular_itineraries.clicked_fares` 
                WHERE DATE(created_at) BETWEEN "%s" AND "%s"

                UNION ALL 

                SELECT *, 
                FROM `wego-cloud.flight_popular_itineraries.non_clicked_fares` 
                WHERE DATE(created_at) BETWEEN "%s" AND "%s"
                  
                -- SELECT SPLIT(trip_id, ":")[SAFE_OFFSET(1)] as itinerary_id, 
                -- ROW_NUMBER() OVER (PARTITION BY SPLIT(trip_id, ":")[SAFE_OFFSET(1)] ORDER BY created_at DESC) as rn,
                -- price.amount as price_amount,
                -- price.currency_code as price_currency_code,
                -- price.amount_usd as price_amount_usd
                -- FROM `wego-cloud.services_curiosity.fares*`
                -- WHERE _TABLE_SUFFIX BETWEEN 
                )
              GROUP BY 1
              -- WHERE rn = 1
              -- SELECT itinerary_id, price_amount_usd 
              -- FROM `wego-cloud.flight_sort_order_ml.rates_data` 
              ) as x

              ON a.code = x.itinerary_id

              LEFT JOIN

              (SELECT code as aircraft_table_code, MAX(star_rating) as aircraft_rating
              FROM `wego-cloud.flights.aircrafts`
              GROUP BY 1) as b

              ON a.aircraft_code = b.aircraft_table_code

              LEFT JOIN

              (SELECT code as airline_table_code, MAX(rating) as airline_rating, MAX(alliance) as lcc_alliance
              FROM `wego-cloud.flights.airlines`
              GROUP BY 1) as c

              ON a.airline_code = c.airline_table_code

              LEFT JOIN

              (SELECT _2_letter_code, Alliance 
              FROM `wego-cloud.analysis.flights_alliances` ) as d

              ON c.airline_table_code = d._2_letter_code

              LEFT JOIN

              (SELECT code as airport_code, location_id 
              FROM `wego-cloud.place_services.airports`) as e

              ON a.departure_airport_code = e.airport_code

              LEFT JOIN

              (SELECT code as airport_code, location_id 
              FROM `wego-cloud.place_services.airports`) as f

              ON a.arrival_airport_code = f.airport_code
              
              LEFT JOIN

              (SELECT id as location_id, code as departure_location_code, base_name as departure_location_name, country_id,
              latitude as departure_latitude, longitude as departure_longitude 
              FROM `wego-cloud.place_services.locations`) as g

              ON e.location_id = g.location_id

              LEFT JOIN

              (SELECT id as location_id, code as arrival_location_code, base_name as arrival_location_name, country_id,
              latitude as arrival_latitude, longitude as arrival_longitude
              FROM `wego-cloud.place_services.locations`) as h

              ON f.location_id = h.location_id
                
              LEFT JOIN

              (SELECT id as country_id, code as departure_country_code, base_name as departure_country_name
              FROM `wego-cloud.place_services.countries`) as i

              ON g.country_id = i.country_id

              LEFT JOIN

              (SELECT id as country_id, code as arrival_country_code, base_name as arrival_country_name
              FROM `wego-cloud.place_services.countries`) as j

              ON h.country_id = j.country_id

              ) PIVOT 
              (MAX(airline_code) AS airline_code,
              MAX(airline_rating) AS airline_rating,  
              MAX(aircraft_code) AS aircraft_code,
              MAX(aircraft_rating) AS aircraft_rating,
              MAX(in_air_min_pivot) AS segment_duration_min
              FOR segments_order_pivot IN (0,1,2,3,4,5))
            GROUP BY 1,2
            )
          ) 
        ) as itinerary_info


        LEFT JOIN

        (SELECT SPLIT(trip_id, ":")[SAFE_OFFSET(1)] as itinerary_id, 
        SUM(1) as total_clicks,
        COUNTIF(conversions_tracked > 0 ) as total_conversions,
        SUM(booking_value_usd) as total_gmv,
        SUM(revenue_in_usd) as total_revenue
        FROM `wego-cloud.wego_analytics.flights_clicks` 
        WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
        GROUP BY 1
        ORDER BY 2 DESC) as performance_info

        ON itinerary_info.code = performance_info.itinerary_id


        LEFT JOIN

        (SELECT first_departure_airport_code, first_arrival_airport_code, trip_type, SUM(1) as total_searches
        FROM `wego-cloud.wego_analytics.flights_searches` 
        WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
        GROUP BY 1,2,3) as search_info

        ON itinerary_info.route_first_departure_airport_code = search_info.first_departure_airport_code
        AND itinerary_info.route_first_arrival_airport_code = search_info.first_arrival_airport_code
        AND LOWER(itinerary_info.route_trip_type) = search_info.trip_type

        LEFT JOIN 
        
        (SELECT SPLIT(o.id,":")[SAFE_OFFSET(1)] AS imp_code,
        COUNT(*) as total_impressions,
        MIN(o.order) as min_impression_position,
        AVG(o.order) as avg_impression_position,
        MAX(o.order) as max_impression_position,
        100 * SAFE_DIVIDE(COUNTIF(CONTAINS_SUBSTR(LOWER(IFNULL(o.order_label, '')), 'best')),COUNT(*)) AS pct_labeled_best,
        100 * SAFE_DIVIDE(COUNTIF(CONTAINS_SUBSTR(LOWER(IFNULL(o.order_label, '')), 'cheapest')),COUNT(*)) AS pct_labeled_cheapest
        FROM
        (SELECT *, MAX(created_at) OVER (PARTITION BY search_id) AS latest_pageload_time
        FROM `wego-cloud.services_genzo.impressions_logs*`
        WHERE _TABLE_SUFFIX BETWEEN "%s" AND "%s" -- Adjust the date range as needed
        AND impression.page IN ("flights_search_results", "flight_search_results")
        -- AND device.app LIKE "%%WEB%%"
        AND impression.trigger = "page_load") il,
        UNNEST(objects) o
        WHERE il.created_at = il.latest_pageload_time
        AND o.itinerary_category = "meta"
        GROUP BY imp_code
        ) AS relevant_imps
        
        ON itinerary_info.code = relevant_imps.imp_code
      )
    ) 
  )    
  ORDER BY total_clicks DESC, code, legs_order
)
"""
, processing_date_suffix,
start_date_string, end_date_string, 
start_date_string, end_date_string,
start_date_string, end_date_string, 
start_date_string, end_date_string,
start_date_string, end_date_string, 
start_date_string, end_date_string,
start_date_suffix, end_date_suffix
);


------------------- MODEL 1: Experience Score Matching -------------------
-- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT 
ROUND(SAFE_DIVIDE(cases_not_null, cases_null), 10) AS scaling_factor 
FROM
  (SELECT 
  COUNTIF(leg_experience_score != 1000) AS cases_null,
  COUNTIF(leg_experience_score = 1000) AS cases_not_null,
  SUM(1) AS total_cases
  FROM 
  `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- WHERE 
  -- avg_impression_position <= (
  --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
  --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- )
  ) 
""" 
, processing_date_suffix, processing_date_suffix)
INTO null_proportion;

-- --- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE leg_experience_score != 1000
-- AND avg_impression_position <= (
--   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
--   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE leg_experience_score = 1000 
-- AND avg_impression_position <= (
-- SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- -- -- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `flight_sort_order_ml.rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['leg_experience_score']) AS
SELECT
route_trip_type,
leg_total_segments,
leg_time_hours,
leg_min_stop_over_hours,
leg_max_stop_over_hours,
leg_min_departure_time_local_min,
leg_max_arrival_time_local_min,
-- aircraft_rating_0,
-- aircraft_rating_1,
-- aircraft_rating_2,
-- aircraft_rating_3,
-- aircraft_rating_4,
-- aircraft_rating_5,
-- airline_rating_0,
-- airline_rating_1,
-- airline_rating_2,
-- airline_rating_3,
-- airline_rating_4,
-- airline_rating_5,
-- segment_duration_min_0,
-- segment_duration_min_1,
-- segment_duration_min_2,
-- segment_duration_min_3,
-- segment_duration_min_4,
-- segment_duration_min_5,
-- price_amount_usd as price_amount_usd,
leg_price_amount_usd,
IFNULL(leg_experience_score,0) as leg_experience_score
FROM `flight_sort_order_ml.training_data_downsample_%s`;
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`flight_sort_order_ml.rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `flight_sort_order_ml.rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 6. Model Global Explain -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.rf_model_global_explain_%s AS (
-- SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `flight_sort_order_ml.rf_model_%s`)
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);

-- -- -- -- --- 7. Individual Record Explainability (Optional) -----
-- -- -- -- CREATE OR REPLACE TABLE flight_sort_order_ml.training_data_explain_predict AS (
-- -- -- -- SELECT
-- -- -- -- -- input.*,
-- -- -- -- -- predoutput.*
-- -- -- -- predoutput.* 
-- -- -- -- -- predoutput.predicted_total_itinerary_card_clicks,
-- -- -- -- -- predoutput.total_itinerary_card_clicks_probs

-- -- -- -- FROM
-- -- -- -- ML.EXPLAIN_PREDICT(MODEL `flight_sort_order_ml.rf_model`,
-- -- -- -- (
-- -- -- -- SELECT
-- -- -- -- *
-- -- -- -- -- leg_total_segments,
-- -- -- -- -- leg_time_hours,
-- -- -- -- -- leg_min_stop_over_hours,
-- -- -- -- -- leg_max_stop_over_hours,
-- -- -- -- -- leg_min_departure_time_local_min,
-- -- -- -- -- leg_max_arrival_time_local_min,
-- -- -- -- -- aircraft_rating_0,
-- -- -- -- -- aircraft_rating_1,
-- -- -- -- -- aircraft_rating_2,
-- -- -- -- -- aircraft_rating_3,
-- -- -- -- -- aircraft_rating_4,
-- -- -- -- -- aircraft_rating_5,
-- -- -- -- -- airline_rating_0,
-- -- -- -- -- airline_rating_1,
-- -- -- -- -- airline_rating_2,
-- -- -- -- -- airline_rating_3,
-- -- -- -- -- airline_rating_4,
-- -- -- -- -- airline_rating_5,
-- -- -- -- -- segment_duration_min_0,
-- -- -- -- -- segment_duration_min_1,
-- -- -- -- -- segment_duration_min_2,
-- -- -- -- -- segment_duration_min_3,
-- -- -- -- -- segment_duration_min_4,
-- -- -- -- -- segment_duration_min_5,
-- -- -- -- -- IFNULL(total_clicks,0) as total_clicks
-- -- -- -- FROM `flight_sort_order_ml.training_data_downsample`
-- -- -- -- WHERE total_clicks IS NOT NULL
-- -- -- -- ORDER BY total_clicks DESC
-- -- -- -- LIMIT 500
-- -- -- -- ),
-- -- -- -- STRUCT(TRUE AS approx_feature_contrib))predoutput join `flight_sort_order_ml.training_data_downsample` input 
-- -- -- -- ON predoutput.code=input.code 
-- -- -- -- AND predoutput.legs_order=input.legs_order
-- -- -- -- )


-- -- 8. Productionise it in Vertex AI
EXECUTE IMMEDIATE FORMAT("""
ALTER MODEL flight_sort_order_ml.rf_model_%s 
SET OPTIONS (vertex_ai_model_id='flight_sort_order_ml_rf_model_%s',
labels=[('model_type', 'flight_sort_order')]
)
"""
, processing_date_suffix, processing_date_suffix);


-- -- -- -- DROP MODEL IF EXISTS flight_sort_order_ml.rf_model20240213

-- ------------------- MODEL 2: Airline Route Features -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT 
ROUND(SAFE_DIVIDE(cases_not_null, cases_null), 10) AS scaling_factor 
FROM
  (SELECT 
  COUNTIF(total_clicks = 0) AS cases_null,
  COUNTIF(total_clicks != 0) AS cases_not_null,
  SUM(1) AS total_cases
  FROM 
  `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- WHERE 
  -- avg_impression_position <= (
  --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
  --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- )
  ) 
""" 
, processing_date_suffix, processing_date_suffix)
INTO null_proportion;

--- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.route_airline_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE total_clicks = 0 
-- AND avg_impression_position <= (
--   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
--   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE total_clicks != 0 
-- AND avg_impression_position <= (
-- SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- -- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `flight_sort_order_ml.route_airline_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_clicks']) AS
SELECT
leg_first_departure_airport_code,
leg_first_arrival_airport_code,
airline_code_0,
airline_code_1,
airline_code_2,
airline_code_3,
airline_code_4,
route_trip_type,
leg_total_segments,
leg_time_hours,
leg_min_stop_over_hours,
leg_max_stop_over_hours,
leg_min_departure_time_local_min,
leg_max_arrival_time_local_min,
leg_price_amount_usd,
IFNULL(total_clicks,0) as total_clicks
FROM `flight_sort_order_ml.route_airline_training_data_downsample_%s`;
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.route_airline_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`flight_sort_order_ml.route_airline_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.route_airline_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `flight_sort_order_ml.route_airline_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 6. Model Global Explain -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.route_airline_rf_model_global_explain_%s AS (
-- SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `flight_sort_order_ml.route_airline_rf_model_%s`)
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);

-- -- -- --- 7. Individual Record Explainability (Optional) -----
-- -- -- CREATE OR REPLACE TABLE flight_sort_order_ml.route_airline_training_data_explain_predict AS (
-- -- -- SELECT
-- -- -- -- input.*,
-- -- -- -- predoutput.*
-- -- -- predoutput.* 
-- -- -- -- predoutput.predicted_total_itinerary_card_clicks,
-- -- -- -- predoutput.total_itinerary_card_clicks_probs

-- -- -- FROM
-- -- -- ML.EXPLAIN_PREDICT(MODEL `flight_sort_order_ml.route_airline_rf_model`,
-- -- -- (
-- -- -- SELECT
-- -- -- *
-- -- -- -- leg_total_segments,
-- -- -- -- leg_time_hours,
-- -- -- -- leg_min_stop_over_hours,
-- -- -- -- leg_max_stop_over_hours,
-- -- -- -- leg_min_departure_time_local_min,
-- -- -- -- leg_max_arrival_time_local_min,
-- -- -- -- aircraft_rating_0,
-- -- -- -- aircraft_rating_1,
-- -- -- -- aircraft_rating_2,
-- -- -- -- aircraft_rating_3,
-- -- -- -- aircraft_rating_4,
-- -- -- -- aircraft_rating_5,
-- -- -- -- airline_rating_0,
-- -- -- -- airline_rating_1,
-- -- -- -- airline_rating_2,
-- -- -- -- airline_rating_3,
-- -- -- -- airline_rating_4,
-- -- -- -- airline_rating_5,
-- -- -- -- segment_duration_min_0,
-- -- -- -- segment_duration_min_1,
-- -- -- -- segment_duration_min_2,
-- -- -- -- segment_duration_min_3,
-- -- -- -- segment_duration_min_4,
-- -- -- -- segment_duration_min_5,
-- -- -- -- IFNULL(total_clicks,0) as total_clicks
-- -- -- FROM `flight_sort_order_ml.training_data_downsample`
-- -- -- WHERE total_clicks IS NOT NULL
-- -- -- ORDER BY total_clicks DESC
-- -- -- LIMIT 500
-- -- -- ),
-- -- -- STRUCT(TRUE AS approx_feature_contrib))predoutput join `flight_sort_order_ml.training_data_downsample` input 
-- -- -- ON predoutput.code=input.code 
-- -- -- AND predoutput.legs_order=input.legs_order
-- -- -- )


-- -- 8. Productionise it in Vertex AI
EXECUTE IMMEDIATE FORMAT("""
ALTER MODEL flight_sort_order_ml.route_airline_rf_model_%s 
SET OPTIONS (vertex_ai_model_id='flight_sort_order_ml_route_airline_rf_model_%s',
labels=[('model_type', 'flight_sort_order_route_airline')]
)
"""
, processing_date_suffix, processing_date_suffix);


-- -- -- -- DROP MODEL IF EXISTS flight_sort_order_ml.rf_model20240213


-- ------------------- MODEL 3A: CTR Target Metric -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT 
ROUND(SAFE_DIVIDE(cases_not_null, cases_null), 10) AS scaling_factor 
FROM
  (SELECT 
  COUNTIF(ctr = 0) AS cases_null,
  COUNTIF(ctr != 0) AS cases_not_null,
  SUM(1) AS total_cases
  FROM 
  `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- WHERE 
  -- avg_impression_position <= (
  --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
  --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- )
  WHERE (total_clicks = 0 OR total_clicks > 2)
  AND CTR <= 100 
  ) 
""" 
, processing_date_suffix, processing_date_suffix)
INTO null_proportion;

--- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.ctr_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE ctr = 0 
-- AND avg_impression_position <= (
--   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
--   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE total_clicks > 2 AND CTR <= 100
-- AND avg_impression_position <= (
-- SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- -- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `flight_sort_order_ml.ctr_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['ctr']) AS
SELECT
leg_first_departure_airport_code,
leg_first_arrival_airport_code,
airline_code_0,
airline_code_1,
airline_code_2,
airline_code_3,
airline_code_4,
route_trip_type,
leg_total_segments,
leg_time_hours,
leg_min_stop_over_hours,
leg_max_stop_over_hours,
leg_min_departure_time_local_min,
leg_max_arrival_time_local_min,
leg_price_amount_usd,
IFNULL(ctr,0) as ctr
FROM `flight_sort_order_ml.ctr_training_data_downsample_%s`;
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.ctr_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`flight_sort_order_ml.ctr_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.ctr_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `flight_sort_order_ml.ctr_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.ctr_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `flight_sort_order_ml.ctr_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

-- -- -- --- 7. Individual Record Explainability (Optional) -----
-- -- -- CREATE OR REPLACE TABLE flight_sort_order_ml.ctr_training_data_explain_predict AS (
-- -- -- SELECT
-- -- -- -- input.*,
-- -- -- -- predoutput.*
-- -- -- predoutput.* 
-- -- -- -- predoutput.predicted_total_itinerary_card_clicks,
-- -- -- -- predoutput.total_itinerary_card_clicks_probs

-- -- -- FROM
-- -- -- ML.EXPLAIN_PREDICT(MODEL `flight_sort_order_ml.ctr_rf_model`,
-- -- -- (
-- -- -- SELECT
-- -- -- *
-- -- -- -- leg_total_segments,
-- -- -- -- leg_time_hours,
-- -- -- -- leg_min_stop_over_hours,
-- -- -- -- leg_max_stop_over_hours,
-- -- -- -- leg_min_departure_time_local_min,
-- -- -- -- leg_max_arrival_time_local_min,
-- -- -- -- aircraft_rating_0,
-- -- -- -- aircraft_rating_1,
-- -- -- -- aircraft_rating_2,
-- -- -- -- aircraft_rating_3,
-- -- -- -- aircraft_rating_4,
-- -- -- -- aircraft_rating_5,
-- -- -- -- airline_rating_0,
-- -- -- -- airline_rating_1,
-- -- -- -- airline_rating_2,
-- -- -- -- airline_rating_3,
-- -- -- -- airline_rating_4,
-- -- -- -- airline_rating_5,
-- -- -- -- segment_duration_min_0,
-- -- -- -- segment_duration_min_1,
-- -- -- -- segment_duration_min_2,
-- -- -- -- segment_duration_min_3,
-- -- -- -- segment_duration_min_4,
-- -- -- -- segment_duration_min_5,
-- -- -- -- IFNULL(total_clicks,0) as total_clicks
-- -- -- FROM `flight_sort_order_ml.training_data_downsample`
-- -- -- WHERE total_clicks IS NOT NULL
-- -- -- ORDER BY total_clicks DESC
-- -- -- LIMIT 500
-- -- -- ),
-- -- -- STRUCT(TRUE AS approx_feature_contrib))predoutput join `flight_sort_order_ml.training_data_downsample` input 
-- -- -- ON predoutput.code=input.code 
-- -- -- AND predoutput.legs_order=input.legs_order
-- -- -- )


-- -- 8. Productionise it in Vertex AI
EXECUTE IMMEDIATE FORMAT("""
ALTER MODEL flight_sort_order_ml.ctr_rf_model_%s 
SET OPTIONS (vertex_ai_model_id='flight_sort_order_ml_ctr_rf_model_%s',
labels=[('model_type', 'flight_sort_order_ctr')]
)
"""
, processing_date_suffix, processing_date_suffix);


-- -- -- -- DROP MODEL IF EXISTS flight_sort_order_ml.rf_model20240213





-- -- ------------------- MODEL 3: Adding Features -------------------
-- -- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
-- EXECUTE IMMEDIATE FORMAT(
-- """
-- SELECT 
-- ROUND(SAFE_DIVIDE(cases_not_null, cases_null), 10) AS scaling_factor 
-- FROM
--   (SELECT 
--   COUNTIF(total_clicks = 0) AS cases_null,
--   COUNTIF(total_clicks != 0) AS cases_not_null,
--   SUM(1) AS total_cases
--   FROM 
--   `wego-cloud.flight_sort_order_ml.training_data_%s`
--   -- WHERE 
--   -- avg_impression_position <= (
--   --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
--   --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
--   -- )
--   ) 
-- """ 
-- , processing_date_suffix, processing_date_suffix)
-- INTO null_proportion;

-- --- 2. Downsample to 50:50 Not Null:Null Cases -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.add_training_data_downsample_%s AS (
-- SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- WHERE total_clicks = 0 
-- -- AND avg_impression_position <= (
-- --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- -- )
-- AND RAND() < @null_proportion -- * 0.2/(1-0.2)

-- UNION ALL 

-- SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- WHERE total_clicks != 0 
-- -- AND avg_impression_position <= (
-- -- SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- -- FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- -- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
-- USING null_proportion as null_proportion;


-- -- -- ----- 3. Train Random Forest Model -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE MODEL `flight_sort_order_ml.add_rf_model_%s`
-- OPTIONS
-- ( model_type='RANDOM_FOREST_REGRESSOR',
--   ENABLE_GLOBAL_EXPLAIN = TRUE,
--   input_label_cols=['total_clicks']) AS
-- SELECT
-- route_trip_type,
-- leg_first_departure_airport_code,
-- leg_first_arrival_airport_code,
-- leg_total_segments,
-- leg_time_hours,
-- leg_min_stop_over_hours,
-- leg_max_stop_over_hours,
-- leg_min_departure_time_local_min,
-- leg_max_arrival_time_local_min,
-- aircraft_rating_0,
-- aircraft_rating_1,
-- aircraft_rating_2,
-- aircraft_rating_3,
-- aircraft_rating_4,
-- -- aircraft_rating_5,
-- airline_rating_0,
-- airline_rating_1,
-- airline_rating_2,
-- airline_rating_3,
-- airline_rating_4,
-- -- airline_rating_5,
-- segment_duration_min_0,
-- segment_duration_min_1,
-- segment_duration_min_2,
-- segment_duration_min_3,
-- segment_duration_min_4,
-- -- segment_duration_min_5,
-- -- price_amount_usd as price_amount_usd,
-- leg_price_amount_usd,
-- IFNULL(total_clicks,0) as total_clicks
-- FROM `flight_sort_order_ml.add_training_data_downsample_%s`;
-- """
-- , processing_date_suffix, processing_date_suffix);

-- -- ----- 4. Model Evaluation Metrics -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.add_rf_model_evaluation_%s AS (
-- SELECT * FROM
-- ML.EVALUATE(MODEL`flight_sort_order_ml.add_rf_model_%s`)
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);

-- --- 5. Model Feature Importance -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.add_rf_model_feature_importance_%s AS (
-- SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `flight_sort_order_ml.add_rf_model_%s`)
-- ORDER BY importance_gain DESC
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);

-- ----- 6. Model Global Explain -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE flight_sort_order_ml.add_rf_model_global_explain_%s AS (
-- SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `flight_sort_order_ml.add_rf_model_%s`)
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);

-- -- -- --- 7. Individual Record Explainability (Optional) -----
-- -- -- CREATE OR REPLACE TABLE flight_sort_order_ml.add_training_data_explain_predict AS (
-- -- -- SELECT
-- -- -- -- input.*,
-- -- -- -- predoutput.*
-- -- -- predoutput.* 
-- -- -- -- predoutput.predicted_total_itinerary_card_clicks,
-- -- -- -- predoutput.total_itinerary_card_clicks_probs

-- -- -- FROM
-- -- -- ML.EXPLAIN_PREDICT(MODEL `flight_sort_order_ml.add_rf_model`,
-- -- -- (
-- -- -- SELECT
-- -- -- *
-- -- -- -- leg_total_segments,
-- -- -- -- leg_time_hours,
-- -- -- -- leg_min_stop_over_hours,
-- -- -- -- leg_max_stop_over_hours,
-- -- -- -- leg_min_departure_time_local_min,
-- -- -- -- leg_max_arrival_time_local_min,
-- -- -- -- aircraft_rating_0,
-- -- -- -- aircraft_rating_1,
-- -- -- -- aircraft_rating_2,
-- -- -- -- aircraft_rating_3,
-- -- -- -- aircraft_rating_4,
-- -- -- -- aircraft_rating_5,
-- -- -- -- airline_rating_0,
-- -- -- -- airline_rating_1,
-- -- -- -- airline_rating_2,
-- -- -- -- airline_rating_3,
-- -- -- -- airline_rating_4,
-- -- -- -- airline_rating_5,
-- -- -- -- segment_duration_min_0,
-- -- -- -- segment_duration_min_1,
-- -- -- -- segment_duration_min_2,
-- -- -- -- segment_duration_min_3,
-- -- -- -- segment_duration_min_4,
-- -- -- -- segment_duration_min_5,
-- -- -- -- IFNULL(total_clicks,0) as total_clicks
-- -- -- FROM `flight_sort_order_ml.training_data_downsample`
-- -- -- WHERE total_clicks IS NOT NULL
-- -- -- ORDER BY total_clicks DESC
-- -- -- LIMIT 500
-- -- -- ),
-- -- -- STRUCT(TRUE AS approx_feature_contrib))predoutput join `flight_sort_order_ml.training_data_downsample` input 
-- -- -- ON predoutput.code=input.code 
-- -- -- AND predoutput.legs_order=input.legs_order
-- -- -- )


-- -- -- 8. Productionise it in Vertex AI
-- EXECUTE IMMEDIATE FORMAT("""
-- ALTER MODEL flight_sort_order_ml.add_rf_model_%s 
-- SET OPTIONS (vertex_ai_model_id='flight_sort_order_ml_add_rf_model_%s',
-- labels=[('model_type', 'flight_sort_order_add')]
-- )
-- """
-- , processing_date_suffix, processing_date_suffix);


-- -- -- -- DROP MODEL IF EXISTS flight_sort_order_ml.add_rf_model20240213



-- ------------------- MODEL 4: Oneway Only -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT 
ROUND(SAFE_DIVIDE(cases_not_null, cases_null), 10) AS scaling_factor 
FROM
  (SELECT 
  COUNTIF(total_clicks = 0) AS cases_null,
  COUNTIF(total_clicks != 0) AS cases_not_null,
  SUM(1) AS total_cases
  FROM 
  `wego-cloud.flight_sort_order_ml.training_data_%s`
  WHERE route_trip_type = "Oneway"
  -- WHERE 
  -- avg_impression_position <= (
  --   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
  --   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
  -- )
  ) 
""" 
, processing_date_suffix, processing_date_suffix)
INTO null_proportion;

--- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.oneway_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE total_clicks = 0 
AND route_trip_type = "Oneway"
-- AND avg_impression_position <= (
--   SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
--   FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
WHERE total_clicks != 0 
AND route_trip_type = "Oneway"
-- AND avg_impression_position <= (
-- SELECT APPROX_QUANTILES(avg_impression_position, 100)[SAFE_OFFSET(25)] 
-- FROM `wego-cloud.flight_sort_order_ml.training_data_%s`
-- )
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- -- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `flight_sort_order_ml.oneway_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_clicks']) AS
SELECT
leg_first_departure_airport_code,
leg_first_arrival_airport_code,
airline_code_0,
airline_code_1,
airline_code_2,
airline_code_3,
airline_code_4,
route_trip_type,
leg_total_segments,
leg_time_hours,
leg_min_stop_over_hours,
leg_max_stop_over_hours,
leg_min_departure_time_local_min,
leg_max_arrival_time_local_min,
leg_price_amount_usd,
IFNULL(total_clicks,0) as total_clicks
FROM `flight_sort_order_ml.oneway_training_data_downsample_%s`;
"""
, processing_date_suffix, processing_date_suffix);

-- ----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.oneway_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`flight_sort_order_ml.oneway_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE flight_sort_order_ml.oneway_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `flight_sort_order_ml.oneway_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);


-- -- 8. Productionise it in Vertex AI
EXECUTE IMMEDIATE FORMAT("""
ALTER MODEL flight_sort_order_ml.oneway_rf_model_%s 
SET OPTIONS (vertex_ai_model_id='flight_sort_order_ml_oneway_rf_model_%s',
labels=[('model_type', 'flight_sort_order_oneway')]
)
"""
, processing_date_suffix, processing_date_suffix);




------------------------ DELETE OLDER MODELS ------------------------
---------- Maintain the past 30 models, delete the rest -------------
SET (model_list, model_count) = (
SELECT AS STRUCT ARRAY_AGG(table_id) as model_list, COUNT(*) as model_count
FROM (
  SELECT * FROM
  (SELECT *, ROW_NUMBER() OVER (ORDER BY creation_time DESC) AS rn
  FROM `wego-cloud.flight_sort_order_ml.__TABLES__`
  WHERE type = 4
  AND table_id LIKE 'rf_model_%' AND table_id NOT IN ("rf_model_sj","rf_model_price")
  )
  WHERE rn > 30
  ORDER BY creation_time
  )
);  

LOOP
  IF i >= model_count THEN
    LEAVE;
  END IF;
    SET model_name = model_list[OFFSET(i)];
    --RAISE USING MESSAGE = FORMAT("""Model to drop: %s""", model_name);
    EXECUTE IMMEDIATE FORMAT("""DROP MODEL `wego-cloud.flight_sort_order_ml.%s`""", model_name);
    SET i = i + 1;
END LOOP;
{% endraw %}
