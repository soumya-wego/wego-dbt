{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_price_trends_history
-- Destination: flights_price_guidance.min_price_history  (unchanged)
-- Schedule   : every day 11:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
DECLARE start_date DATE; 
DECLARE end_date DATE;
DECLARE processing_date DATE;
DECLARE date_shard_suffix STRING;
DECLARE click_rank INT64 DEFAULT 20;
DECLARE today_date DATE DEFAULT CURRENT_DATE();
DECLARE date_before_current_date INT64 DEFAULT 1;
-- look 60 days in the past
DECLARE date_range INT64 DEFAULT 60;

SET processing_date = today_date;
SET date_shard_suffix = FORMAT_DATE("%Y%m%d", processing_date);
SET end_date = DATE_SUB(today_date, INTERVAL date_before_current_date DAY);
SET start_date = DATE_SUB(today_date, INTERVAL date_before_current_date + date_range DAY);



CREATE OR REPLACE TABLE flights_price_guidance.min_price_history
PARTITION BY processing_date
CLUSTER BY trip_type, route, departure_date
AS

-- INSERT INTO flights_price_guidance.min_price_history (date_shard, route, trip_type, cabin, departure_date, airline_type, stops, daily_min, start_date, end_date, processing_date)


WITH 
airport_metadata AS (
    SELECT airport, airport_meta.location_id, city_code, location_name, latitude, longitude, location_meta.country_id, country_code, country_name
    FROM
    (SELECT code AS airport, location_id FROM `wego-cloud.place_services.airports`) AS airport_meta
    LEFT JOIN
    (SELECT id, code AS city_code, base_name AS location_name FROM `wego-cloud.place_services.locations`) AS city_meta 
    ON airport_meta.location_id = city_meta.id
    LEFT JOIN
    (SELECT id, latitude AS latitude, longitude AS longitude, country_id FROM `wego-cloud.place_services.locations`) AS location_meta 
    ON city_meta.id = location_meta.id
    LEFT JOIN
    (SELECT id AS country_id, code AS country_code, base_name AS country_name FROM `wego-cloud.place_services.countries`) AS country_meta 
    ON country_meta.country_id = location_meta.country_id
), 

holidays_data AS
(SELECT site_code,
  IF(rn=1, INITCAP(REPLACE(key, '_', ' ')), NULL) as holiday_name,
  IF(rn=1, date, NULL) as holiday_start,
  IF(rn=1, DATE_ADD(date, INTERVAL length-1 DAY), NULL) as holiday_end
  -- IF(rn=1, length, NULL) as holiday_length
  FROM
  (SELECT *,
    ROW_NUMBER() over (partition by site_code, cumulative_grp) as rn,
    count(*) over (partition by site_code, cumulative_grp) as length
    FROM
    (SELECT *,
      -- id for each grp of consecutive holidays
      sum(date_diff) OVER (PARTITION BY site_code ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_grp,
      FROM
      (SELECT * EXCEPT(date_diff), IF(date_diff is null, 0 ,date_diff) as date_diff
        FROM
        (SELECT *, DATE_DIFF(date, date_lag, DAY) - 1 as date_diff
          FROM (SELECT *, lag(date) OVER (PARTITION BY site_code ORDER BY date) as date_lag FROM
          (SELECT * EXCEPT(created_at, updated_at, date), DATE(date) as date
          FROM `wego-cloud.place_services.public_holidays`)))) as a
    LEFT JOIN `wego-cloud.place_services.holiday_keys` as b
    ON b.id = a.holiday_key_id))
    WHERE rn=1),

airport_holidays AS
(SELECT *,
  CASE
    WHEN date < holiday_start
      THEN DATE_DIFF(holiday_start, date , DAY)
    WHEN date > holiday_end
      THEN DATE_DIFF(holiday_end, date, DAY)
    ELSE
      0
  END as days_to_hol
  FROM
  (SELECT
  *,
  FROM (SELECT
    airport, country_name, holiday_name, holiday_start, holiday_end
    FROM airport_metadata a
    LEFT JOIN holidays_data h1
    ON a.country_code = h1.site_code
  ),
  UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(holiday_start, INTERVAL 7 DAY), DATE_ADD(holiday_end, INTERVAL 3 DAY))) AS date)),

aggregated_data AS
(SELECT *,
   ROW_NUMBER() OVER (PARTITION BY provider_airline_clicks_rank) as provider_airline_rn
    FROM
    (SELECT *,
      DENSE_RANK() OVER (ORDER BY provider_airline_clicks DESC) as provider_airline_clicks_rank
      FROM
      (SELECT *,
        SUM(total_clicks) OVER (PARTITION BY provider_code, airline) as provider_airline_clicks
        FROM
        (SELECT *,
          IF(rn = 1, clicks, NULL) as total_clicks,
          FROM
          (SELECT i.*,
            clicks,
            ROW_NUMBER() OVER (PARTITION BY i.airline, i.route, i.trip_type, i.provider_code) as rn
            FROM
            (SELECT a.*,
              h1.holiday_name as departure_holiday,
              h1.holiday_start as departure_holiday_start,
              h1.holiday_end as departure_holiday_end,
              h1.days_to_hol as departure_days_to_hol,
              h2.holiday_name as arrival_holiday,
              h2.holiday_start as arrival_holiday_start,
              h2.holiday_end as arrival_holiday_end,
              h2.days_to_hol as arrival_days_to_hol
              FROM
              (SELECT
                date_shard,
                CASE
                  WHEN CONTAINS_SUBSTR(provider_code,"wego.com")
                    THEN "wego.com"
                  ELSE
                    provider_code
                  END as provider_code,
                lead_time,
                departure_code,
                arrival_code,
                route,
                month,
                day,
                year,
                DATE(CONCAT(year, "-", month, "-", day)) as departure_date,
                day_of_week,
                IF (airline_count > 1, "mixed", name_en) as airline,
                CASE
                  WHEN airline_count > 1 
                    THEN "mixed"
                  WHEN alliance = "lcc" OR name_en in ("AJet")
                    THEN "LCC"
                  ELSE 
                    "FSC"
                END as airline_type,
                cabin,
                INITCAP(trip_type) as trip_type,
                stops,
                SD,
                mean,
                min, 
                max,
                n
                FROM
                (SELECT * from  `wego-cloud.flights_price_guidance.price_trends_data_7`
                WHERE DATE(date_shard) BETWEEN start_date AND end_date
                AND DATE(CONCAT(year, "-", month, "-", day)) >= CURRENT_DATE()
                -- AND route = "KWI-CAI"
                -- AND trip_type = "oneway"
                ) as c
                LEFT JOIN `wego-cloud.flights.airlines`  as d
                ON c.airline = d.code

                ) as a
                  LEFT JOIN airport_holidays h1
                  ON departure_date = h1.date AND departure_code = h1.airport
                  LEFT JOIN airport_holidays h2
                  ON departure_date = h2.date AND arrival_code = h2.airport
                ) as i
              LEFT JOIN
                (SELECT trip_type, provider_code, route, name_en as airline, clicks
                  FROM
                    (SELECT INITCAP(trip_type) as trip_type, provider_code, airline, CONCAT(departure_airport_code, "-", arrival_airport_code) as route,
                      count(*) as clicks
                      FROM `wego-cloud.wego_analytics.flights_clicks`
                      WHERE DATE(_PARTITIONDATE) BETWEEN start_date AND end_date
                    GROUP BY
                    1,2,3,4) as a
                    LEFT JOIN `wego-cloud.flights.airlines`  as b
                    ON a.airline = b.code
                  where name_en is not null)
                as j
               ON i.route = j.route AND i.trip_type = j.trip_type AND i.provider_code = j.provider_code AND i.airline = j.airline
            )))))


    SELECT 
      date_shard, route, trip_type, cabin, departure_date, stops, airline_type,
      MIN(min) as daily_min, 
      start_date,
      end_date,
      processing_date, 
    FROM (
      SELECT  
        date_shard, 
        lead_time,
        route,
        trip_type, 
        cabin,
        stops,
        departure_date,
        airline,
        airline_type,
        mean, 
        min, 
        max
      FROM aggregated_data 
      WHERE provider_airline_clicks_rank <= click_rank
        AND provider_code != "travrun.com"
        AND SD > 0
    )
    GROUP BY route, trip_type, cabin, departure_date, stops, airline_type, date_shard

-- SELECT distinct airline, airline_type FROM 
-- aggregated_data
{% endraw %}
