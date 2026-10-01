{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_bow_rate_cache_ttl_metrics
-- Destination: hotels_bow_rates_analysis.ttl_strategy_metrics  (unchanged)
-- Schedule   : every mon,thu 02:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE date_before_processing_date INT64 DEFAULT 1;
DECLARE date_window INT64 DEFAULT 0; -- i.e. If 12 day range, put 11 for window


EXECUTE IMMEDIATE "SELECT DATE_SUB(DATE(?), INTERVAL ? DAY)" INTO end_date USING processing_timestamp, date_before_processing_date;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO start_date USING end_date, date_window;

--- INITIALISATION ---
-- CREATE OR REPLACE TABLE hotels_bow_rates_analysis.ttl_strategy_metrics AS (
-- WITH hotel_details as
-- (SELECT CAST(hotels.id AS STRING) as hotel_id, name_en as hotel_name, countries.base_name as country, countries.code as country_code,
-- locations.base_name as city, city_code, CAST(hotels.brand_id AS STRING) as brand_id, brand.brand_name,
-- CAST(hotels.property_type_id AS STRING) as property_type_id, property_type_name, CAST(brand.chain_id AS STRING) as chain_id, chain.chain_name, reviews_score, reviews_count, IFNULL(star,0) as star
-- FROM
-- (SELECT id, brand_id, property_type_id, name_en, city_code, star
-- FROM `wego-cloud.hotel_services.hotels` ) as hotels

-- LEFT JOIN

-- (SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
-- FROM `wego-cloud.hotel_services.brands`) as brand
-- ON hotels.brand_id = brand.brand_id

-- LEFT JOIN

-- (SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
-- FROM `wego-cloud.hotel_services.chains`) as chain
-- ON chain.chain_id = brand.chain_id

-- LEFT JOIN

-- (SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
-- FROM `hotel_services.property_types`) as property_type
-- on property_type.property_type_id = hotels.property_type_id

-- LEFT JOIN

-- (SELECT hotel_id, MAX(score) as reviews_score, MAX(count) as reviews_count
-- FROM hotel_services.reviews
-- WHERE reviewer_group='ALL' GROUP BY hotel_id) AS reviews
-- on reviews.hotel_id = hotels.id

-- LEFT JOIN

-- (SELECT id, base_name, code, country_id
-- FROM `wego-cloud.place_services.locations`) as locations

-- ON hotels.city_code = locations.code

-- LEFT JOIN

-- (SELECT id, base_name, code
-- FROM `wego-cloud.place_services.countries`) as countries

-- ON locations.country_id = countries.id
-- )

-- SELECT *, 
-- -- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(min_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(min_since_last_change_min AS INT64),60)," mins") as min_since_last_change,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(average,60)) AS INT64)," hrs ",MOD(CAST(average AS INT64),60)," mins") as average_str,
-- -- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(max_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(max_since_last_change_min AS INT64),60)," mins") as max_since_last_change,
-- -- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(mode_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(mode_since_last_change_min AS INT64),60)," mins") as mode_since_last_change,


-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p0,60)) AS INT64)," hrs ",MOD(CAST(p0 AS INT64),60)," mins") as p0_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p5,60)) AS INT64)," hrs ",MOD(CAST(p5 AS INT64),60)," mins") as p5_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p10,60)) AS INT64)," hrs ",MOD(CAST(p10 AS INT64),60)," mins") as p10_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p15,60)) AS INT64)," hrs ",MOD(CAST(p15 AS INT64),60)," mins") as p15_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p20,60)) AS INT64)," hrs ",MOD(CAST(p20 AS INT64),60)," mins") as p20_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p25,60)) AS INT64)," hrs ",MOD(CAST(p25 AS INT64),60)," mins") as p25_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p30,60)) AS INT64)," hrs ",MOD(CAST(p30 AS INT64),60)," mins") as p30_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p35,60)) AS INT64)," hrs ",MOD(CAST(p35 AS INT64),60)," mins") as p35_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p40,60)) AS INT64)," hrs ",MOD(CAST(p40 AS INT64),60)," mins") as p40_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p45,60)) AS INT64)," hrs ",MOD(CAST(p45 AS INT64),60)," mins") as p45_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p50,60)) AS INT64)," hrs ",MOD(CAST(p50 AS INT64),60)," mins") as p50_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p55,60)) AS INT64)," hrs ",MOD(CAST(p55 AS INT64),60)," mins") as p55_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p60,60)) AS INT64)," hrs ",MOD(CAST(p60 AS INT64),60)," mins") as p60_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p65,60)) AS INT64)," hrs ",MOD(CAST(p65 AS INT64),60)," mins") as p65_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p70,60)) AS INT64)," hrs ",MOD(CAST(p70 AS INT64),60)," mins") as p70_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p75,60)) AS INT64)," hrs ",MOD(CAST(p75 AS INT64),60)," mins") as p75_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p80,60)) AS INT64)," hrs ",MOD(CAST(p80 AS INT64),60)," mins") as p80_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p85,60)) AS INT64)," hrs ",MOD(CAST(p85 AS INT64),60)," mins") as p85_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p90,60)) AS INT64)," hrs ",MOD(CAST(p90 AS INT64),60)," mins") as p90_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p95,60)) AS INT64)," hrs ",MOD(CAST(p95 AS INT64),60)," mins") as p95_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p100,60)) AS INT64)," hrs ",MOD(CAST(p100 AS INT64),60)," mins") as p100_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(iqr,60)) AS INT64)," hrs ",MOD(CAST(iqr AS INT64),60)," mins") as iqr_str,

-- start_date,
-- end_date,
-- date_window + 1 as date_window,
-- processing_timestamp

-- FROM
--   (SELECT 
--   device,
--   supplier_name, 
--   leadtime,
--   leadtime_start,
--   leadtime_end,
--   site_code, 
--   board_basis, 
--   supplier_channel,
--   refund_term,
--   length_of_stay,
--   length_of_stay_start,
--   length_of_stay_end,
--   weekend_ind,
--   star,
--   -- public_holiday_ind,

--   COUNT(DISTINCT block_no) as unique_rate_combinations,
--   SUM(1) as total_rates_count, 
--   SUM(IF(rn = 1,1,0)) as starting_rates_count, 
--   SUM(IF(price_change != 0,1,0)) as total_changed_rates, 
--   SUM(IF(price_change = 0,1,0)) as total_unchanged_rates, 
--   ROUND(100*SAFE_DIVIDE(SUM(IF(price_change != 0, 1,0)),SUM(IF(rn != 1,1,0)) )) as pct_changed_rates,
--   ROUND(100*SAFE_DIVIDE(SUM(IF(price_change = 0, 1,0)),SUM(IF(rn != 1,1,0)) )) as pct_unchanged_rates,

--   ---- PRESENTATION FORMATTING ----
--   -- FORMAT("%'d", COUNT(DISTINCT block_no)) as unique_rate_combinations,
--   -- FORMAT("%'d", SUM(1)) as total_rates_count, 
--   -- FORMAT("%'d", SUM(IF(rn = 1,1,0))) as starting_rates_count, 
--   -- FORMAT("%'d",SUM(IF(price_change != 0,1,0))) as total_changed_rates, 
--   -- FORMAT("%'d",SUM(IF(price_change = 0,1,0))) as total_unchanged_rates, 
--   -- FORMAT('%s%%',CAST(ROUND(100*SAFE_DIVIDE(SUM(IF(price_change != 0, 1,0)),SUM(IF(rn != 1,1,0)) ))AS STRING)) as pct_changed_rates,
--   -- FORMAT('%s%%',CAST(ROUND(100*SAFE_DIVIDE(SUM(IF(price_change = 0, 1,0)),SUM(IF(rn != 1,1,0)) ))AS STRING)) as pct_unchanged_rates,


--   -- -- MIN(IF(since_last_change_min != 0, since_last_change_min, NULL)) as min_since_last_change_min,
--   -- AVG(IF(since_last_change_min != 0, since_last_change_min, NULL)) as average,
--   -- -- MAX(IF(since_last_change_min != 0, since_last_change_min, NULL)) as max_since_last_change_min, 
--   -- -- APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)].value as mode_since_last_change_min,
--   -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),4)[SAFE_OFFSET(1)] as q1_since_last_change_min,
--   -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)] as median_since_last_change_min,
--   -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),4)[SAFE_OFFSET(3)] as q3_since_last_change_min,
--   -- -- COALESCE(APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(0)].value,APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)].value) as mode_since_last_change_min,
--   -- MAX(p0) as p0,
--   -- MAX(p5) as p5,
--   -- MAX(p10) as p10,
--   -- MAX(p15) as p15,
--   -- MAX(p20) as p20,
--   -- MAX(p25) as p25,
--   -- MAX(p30) as p30,
--   -- MAX(p35) as p35,
--   -- MAX(p40) as p40,
--   -- MAX(p45) as p45,
--   -- MAX(p50) as p50,
--   -- MAX(p55) as p55,
--   -- MAX(p60) as p60,
--   -- MAX(p65) as p65,
--   -- MAX(p70) as p70,
--   -- MAX(p75) as p75,
--   -- MAX(p80) as p80,
--   -- MAX(p85) as p85,
--   -- MAX(p90) as p90,
--   -- MAX(p95) as p95,
--   -- MAX(p100) as p100,
--   -- MAX(p75)-MAX(p25) as iqr
--   FROM
--   (SELECT *,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.00) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p0,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.05) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p5,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.10) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p10,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.15) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p15,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.20) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p20,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.25) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p25,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.30) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p30,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.35) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p35,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.40) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p40,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.45) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p45,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.50) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p50,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.55) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p55,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.60) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p60,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.65) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p65,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.70) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p70,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.75) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p75,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.80) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p80,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.85) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p85,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.90) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p90,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.95) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p95,
--   -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 1.00) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p100

--   FROM (
--   SELECT * EXCEPT (leadtime, weekend_ind, length_of_stay),
--   CASE WHEN leadtime = 0 THEN "01. Same day"
--   WHEN leadtime = 1 THEN "02. Next day"
--   WHEN leadtime >= 2 AND leadtime <= 7 THEN "03. 2-7 days"
--   WHEN leadtime >= 8 AND leadtime <= 14 THEN "04. 8-14 days"
--   WHEN leadtime >= 15 AND leadtime <= 30 THEN "05. 15-30 days"
--   WHEN leadtime >= 31 AND leadtime <= 60 THEN "06. 31-60 days"
--   WHEN leadtime >= 61 AND leadtime <= 90 THEN "07. 61-90 days"
--   WHEN leadtime >= 91 AND leadtime <= 120 THEN "08. 91-120 days"
--   WHEN leadtime >= 121 AND leadtime <= 150 THEN "09. 121-150 days"
--   WHEN leadtime >= 151 AND leadtime <= 180 THEN "10. 151-180 days"
--   WHEN leadtime > 180 THEN "11. Over 180 days" END as leadtime,
  
--   CASE WHEN leadtime = 0 THEN 0
--   WHEN leadtime = 1 THEN 1
--   WHEN leadtime >= 2 AND leadtime <= 7 THEN 2
--   WHEN leadtime >= 8 AND leadtime <= 14 THEN 8
--   WHEN leadtime >= 15 AND leadtime <= 30 THEN 15
--   WHEN leadtime >= 31 AND leadtime <= 60 THEN 31
--   WHEN leadtime >= 61 AND leadtime <= 90 THEN 61
--   WHEN leadtime >= 91 AND leadtime <= 120 THEN 91
--   WHEN leadtime >= 121 AND leadtime <= 150 THEN 121
--   WHEN leadtime >= 151 AND leadtime <= 180 THEN 151
--   WHEN leadtime > 180 THEN 181 END as leadtime_start,

--   CASE WHEN leadtime = 0 THEN 1
--   WHEN leadtime = 1 THEN 2
--   WHEN leadtime >= 2 AND leadtime <= 7 THEN 8
--   WHEN leadtime >= 8 AND leadtime <= 14 THEN 15
--   WHEN leadtime >= 15 AND leadtime <= 30 THEN 31
--   WHEN leadtime >= 31 AND leadtime <= 60 THEN 61
--   WHEN leadtime >= 61 AND leadtime <= 90 THEN 91
--   WHEN leadtime >= 91 AND leadtime <= 120 THEN 121
--   WHEN leadtime >= 121 AND leadtime <= 150 THEN 151
--   WHEN leadtime >= 151 AND leadtime <= 180 THEN 181
--   WHEN leadtime > 180 THEN 100000 END as leadtime_end,

--   CASE WHEN length_of_stay = 1 THEN "01. 1 Day"
--   WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN "02. 2-5 Days"
--   WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN "03. 6-14 Days"
--   WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN "04. 15-21 Days"
--   WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN "05. 22-31 Days"
--   WHEN length_of_stay >= 32 THEN "06. >= 32 Days"
--   END as length_of_stay,

--   CASE WHEN length_of_stay = 1 THEN 1
--   WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN 2
--   WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN 6
--   WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN 15
--   WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN 22
--   WHEN length_of_stay >= 32 THEN 32
--   END as length_of_stay_start,

--   CASE WHEN length_of_stay = 1 THEN 2
--   WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN 6
--   WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN 15
--   WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN 22
--   WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN 32
--   WHEN length_of_stay >= 32 THEN 100000
--   END as length_of_stay_end,


--   IF(weekend_ind > 0, 1, 0) as weekend_ind,
--   -- IF(public_holiday_ind > 0, 1, 0) as public_holiday_ind,

--   MAX(IF(price_change != 0 OR rn = 1, timestamp, NULL)) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as since_last_change_timestamp,
--   IF(price_change != 0, TIMESTAMP_DIFF(timestamp, MAX(IF(price_change != 0 OR rn = 1, timestamp, NULL)) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), MINUTE), NULL) as since_last_change_min
--   FROM
--     (SELECT
--     CONCAT(wego_hotel_id, " ", currency_code, " ", check_in_date, " ", check_out_date, " ", supplier_name, " ", room_type_name, " ", board_basis) as block_combination,
--     DENSE_RANK() OVER (ORDER BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date)) as block_no,
--     COUNT(1) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rate_count,
--     ROW_NUMBER() OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) AS rn,

--     IFNULL(TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room), MINUTE),0) as time_diff_min,
--     LAG(timestamp) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) as previous_timestamp,
--     timestamp,
--     base_price_amount_room,
--     LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) as previous_base_price_amount_room,
--     MIN(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_base_price_amount_room,
--     MAX(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_base_price_amount_room,
--     ROUND(base_price_amount_room-LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room),2) as price_change,
--     ROUND(100*SAFE_DIVIDE(base_price_amount_room-LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room),base_price_amount_room),1) as pct_change,
--     * EXCEPT (timestamp, base_price_amount_room),
--     EXTRACT(HOUR FROM timestamp) AS timestamp_hour,
--     EXTRACT(MINUTE FROM timestamp) AS timestamp_minute,
--     EXTRACT(SECOND FROM timestamp) AS timestamp_second,
--     EXTRACT(DAYOFWEEK FROM timestamp) AS timestamp_day_of_week,
--     IF(EXTRACT(DAYOFWEEK FROM timestamp) IN (1,7),1,0) AS timestamp_weekend,
--     DATE_DIFF(DATE(check_in_date), DATE(timestamp), DAY) as leadtime,
--     EXTRACT(DAY FROM DATE(check_in_date)) AS check_in_date_day,
--     EXTRACT(MONTH FROM DATE(check_in_date)) AS check_in_date_month,
--     EXTRACT(DAYOFWEEK FROM DATE(check_in_date)) AS check_in_date_day_of_week,
--     IF(EXTRACT(DAYOFWEEK FROM DATE(check_in_date)) IN (1,7),1,0) AS check_in_date_weekend,
--     DATE_DIFF(DATE(check_out_date), DATE(check_in_date), DAY) as length_of_stay,
--     -- (SELECT
--     -- COUNT(DISTINCT holiday_date)
--     -- -- STRUCT(holiday_date, id)
--     -- FROM
--     -- UNNEST(GENERATE_DATE_ARRAY(DATE(check_in_date), DATE(check_out_date), INTERVAL 1 DAY)) as date_list
--     -- JOIN
--     -- (SELECT  DATE(date) as holiday_date, MAX(site_code) as site_code
--     -- FROM `wego-cloud.place_services.public_holidays`
--     -- GROUP BY 1) as b
--     -- ON date_list = b.holiday_date AND site_code = b.site_code
--     -- ) as public_holiday_ind,

--     (SELECT
--     SUM(CASE WHEN site_code in ('DZ', 'BH', 'EG', 'IQ', 'IL', 'JO', 'KW', 'LY', 'MV', 'OM', 'QA', 'SA', 'SS', 'SY', 'YE', 'AE')
--     AND EXTRACT(DAYOFWEEK FROM date_list) in (6,7) THEN 1
--     WHEN EXTRACT(DAYOFWEEK FROM date_list) in (7,1) THEN 1
--     ELSE 0 END) as weekend_ind
--     FROM
--     UNNEST(GENERATE_DATE_ARRAY(DATE(check_in_date), DATE(check_out_date), INTERVAL 1 DAY)) as date_list
--     ) as weekend_ind

--     FROM

--       (SELECT
--       MIN(timestamp) as timestamp,
--       yorktown_session_id,
--       device,
--       supplier_code,
--       supplier_name,
--       wego_hotel_id,
--       room_type_id,
--       room_type_name,
--       supplier_channel,
--       refund_term,
--       board_basis,
--       currency_code,
--       MIN(base_price_amount) as base_price_amount,
--       MIN(tax_amount) as tax_amount,
--       MIN(base_price_amount_room) as base_price_amount_room,
--       id,
--       city_code,
--       check_in_date,
--       check_out_date,
--       rooms_count,
--       currency,
--       site_code,
--       user_country_code,
--       language_code,
--       FROM
--         (SELECT *,
--         SAFE_DIVIDE(base_price_amount, rooms_count) as base_price_amount_room
--         FROM

--         (SELECT
--         timestamp,
--         yorktown_session_id,
--         supplier_code,
--         supplier_name,
--         -- supplier_channel,
--         #client_search_id,
--         wego_hotel_id,
--         room_type_id,
--         room_type_name,
--         supplier_channel,
--         refund_term,
--         board_basis,
--         currency_code,
--         base_price_amount,
--         tax_amount,
--         -- additional_charges.currency,
--         -- additional_charges.amount,
--         -- marketing_fee.currency,
--         -- marketing_fee.amount,
--         FROM `ib_hotels_supplier_worker.raw_rates*`
--         WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
--         -- AND wego_hotel_id = "987582"
--         -- AND wego_hotel_id in (SELECT distinct hotel_id FROM hotel_details WHERE country_code = "SA")
--         -- LIMIT 500
--         -- AND room_type_name = "4BR FAMILY SUITE SEAVIEW"
--         -- and refund_term = "no_refund"
--         -- AND board_basis = "Room Only"
--         -- AND supplier_channel = "mobile_app"
--         -- AND supplier_name = "HPro"
--         -- AND room_type_id = "DB"
--         ) as a

--         INNER JOIN

--         (SELECT
--         id,
--         device,
--         -- hotel_ids,
--         city_code,
--         check_in_date,
--         check_out_date,
--         -- city_code,
--         rooms_count,
--         currency,
--         -- user_city,
--         -- user_country_code,
--         site_code,
--         user_country_code,
--         language_code,
--         FROM `wego-cloud.ib_hotels.sessions*`
--         WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
--         -- AND check_in_date = "2023-07-13"
--         -- AND check_out_date = "2023-07-14"
--         -- AND user_country_code = "SA"
--         -- AND site_code = "SA"

--         -- WHERE hotel_id = "85314"
--         -- AND site_code = "AE"
--         ) as b

--         ON a.yorktown_session_id = b.id
--         -- ORDER BY yorktown_session_id, timestamp
--       )
--     WHERE user_country_code is not null
--     GROUP BY 2,3,4,5,6,7,8,9,10,11,12,16,17,18,19,20,21,22,23,24
--     ) as rates

--     LEFT JOIN

--     (SELECT DISTINCT * FROM hotel_details) as hotel_details

--     ON rates.wego_hotel_id = hotel_details.hotel_id
--   )
--   -- WHERE min_base_price_amount_room != max_base_price_amount_room
--   -- AND pct_change > 0
--   -- AND block_no = 1764
--   -- WHERE rn != 1
--   -- WHERE rate_count > 5
--   -- AND MOD(rn,2) = 0
--     )
--   -- WHERE since_last_change_min = 0
--   -- WHERE block_combination = "1012379 USD 2023-08-31 2023-09-01 DOTW Honeymoon Suite Room Only"
--   ORDER BY block_no, rn

--   )
--   -- GROUP BY 1,2,3
--   -- ORDER BY 1,2,3 
--   -- GROUP BY 1,2
--   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
--   )
-- ORDER BY 1,2 DESC 

-- )


INSERT hotels_bow_rates_analysis.ttl_strategy_metrics

WITH hotel_details as
(SELECT CAST(hotels.id AS STRING) as hotel_id, name_en as hotel_name, countries.base_name as country, countries.code as country_code,
locations.base_name as city, city_code, CAST(hotels.brand_id AS STRING) as brand_id, brand.brand_name,
CAST(hotels.property_type_id AS STRING) as property_type_id, property_type_name, CAST(brand.chain_id AS STRING) as chain_id, chain.chain_name, reviews_score, reviews_count, IFNULL(star,0) as star
FROM
(SELECT id, brand_id, property_type_id, name_en, city_code, star
FROM `wego-cloud.hotel_services.hotels` ) as hotels

LEFT JOIN

(SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
FROM `wego-cloud.hotel_services.brands`) as brand
ON hotels.brand_id = brand.brand_id

LEFT JOIN

(SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
FROM `wego-cloud.hotel_services.chains`) as chain
ON chain.chain_id = brand.chain_id

LEFT JOIN

(SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
FROM `hotel_services.property_types`) as property_type
on property_type.property_type_id = hotels.property_type_id

LEFT JOIN

(SELECT hotel_id, MAX(score) as reviews_score, MAX(count) as reviews_count
FROM hotel_services.reviews
WHERE reviewer_group='ALL' GROUP BY hotel_id) AS reviews
on reviews.hotel_id = hotels.id

LEFT JOIN

(SELECT id, base_name, code, country_id
FROM `wego-cloud.place_services.locations`) as locations

ON hotels.city_code = locations.code

LEFT JOIN

(SELECT id, base_name, code
FROM `wego-cloud.place_services.countries`) as countries

ON locations.country_id = countries.id
)

SELECT *, 
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(min_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(min_since_last_change_min AS INT64),60)," mins") as min_since_last_change,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(average,60)) AS INT64)," hrs ",MOD(CAST(average AS INT64),60)," mins") as average_str,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(max_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(max_since_last_change_min AS INT64),60)," mins") as max_since_last_change,
-- -- CONCAT(CAST(FLOOR(SAFE_DIVIDE(mode_since_last_change_min,60)) AS INT64)," hrs ",MOD(CAST(mode_since_last_change_min AS INT64),60)," mins") as mode_since_last_change,


-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p0,60)) AS INT64)," hrs ",MOD(CAST(p0 AS INT64),60)," mins") as p0_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p5,60)) AS INT64)," hrs ",MOD(CAST(p5 AS INT64),60)," mins") as p5_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p10,60)) AS INT64)," hrs ",MOD(CAST(p10 AS INT64),60)," mins") as p10_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p15,60)) AS INT64)," hrs ",MOD(CAST(p15 AS INT64),60)," mins") as p15_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p20,60)) AS INT64)," hrs ",MOD(CAST(p20 AS INT64),60)," mins") as p20_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p25,60)) AS INT64)," hrs ",MOD(CAST(p25 AS INT64),60)," mins") as p25_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p30,60)) AS INT64)," hrs ",MOD(CAST(p30 AS INT64),60)," mins") as p30_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p35,60)) AS INT64)," hrs ",MOD(CAST(p35 AS INT64),60)," mins") as p35_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p40,60)) AS INT64)," hrs ",MOD(CAST(p40 AS INT64),60)," mins") as p40_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p45,60)) AS INT64)," hrs ",MOD(CAST(p45 AS INT64),60)," mins") as p45_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p50,60)) AS INT64)," hrs ",MOD(CAST(p50 AS INT64),60)," mins") as p50_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p55,60)) AS INT64)," hrs ",MOD(CAST(p55 AS INT64),60)," mins") as p55_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p60,60)) AS INT64)," hrs ",MOD(CAST(p60 AS INT64),60)," mins") as p60_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p65,60)) AS INT64)," hrs ",MOD(CAST(p65 AS INT64),60)," mins") as p65_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p70,60)) AS INT64)," hrs ",MOD(CAST(p70 AS INT64),60)," mins") as p70_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p75,60)) AS INT64)," hrs ",MOD(CAST(p75 AS INT64),60)," mins") as p75_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p80,60)) AS INT64)," hrs ",MOD(CAST(p80 AS INT64),60)," mins") as p80_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p85,60)) AS INT64)," hrs ",MOD(CAST(p85 AS INT64),60)," mins") as p85_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p90,60)) AS INT64)," hrs ",MOD(CAST(p90 AS INT64),60)," mins") as p90_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p95,60)) AS INT64)," hrs ",MOD(CAST(p95 AS INT64),60)," mins") as p95_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(p100,60)) AS INT64)," hrs ",MOD(CAST(p100 AS INT64),60)," mins") as p100_str,
-- CONCAT(CAST(FLOOR(SAFE_DIVIDE(iqr,60)) AS INT64)," hrs ",MOD(CAST(iqr AS INT64),60)," mins") as iqr_str,

start_date,
end_date,
date_window + 1 as date_window,
processing_timestamp

FROM
  (SELECT 
  device,
  supplier_name, 
  leadtime,
  leadtime_start,
  leadtime_end,
  site_code, 
  board_basis, 
  supplier_channel,
  refund_term,
  length_of_stay,
  length_of_stay_start,
  length_of_stay_end,
  weekend_ind,
  star,
  -- public_holiday_ind,

  COUNT(DISTINCT block_no) as unique_rate_combinations,
  SUM(1) as total_rates_count, 
  SUM(IF(rn = 1,1,0)) as starting_rates_count, 
  SUM(IF(price_change != 0,1,0)) as total_changed_rates, 
  SUM(IF(price_change = 0,1,0)) as total_unchanged_rates, 
  ROUND(100*SAFE_DIVIDE(SUM(IF(price_change != 0, 1,0)),SUM(IF(rn != 1,1,0)) )) as pct_changed_rates,
  ROUND(100*SAFE_DIVIDE(SUM(IF(price_change = 0, 1,0)),SUM(IF(rn != 1,1,0)) )) as pct_unchanged_rates,

  ---- PRESENTATION FORMATTING ----
  -- FORMAT("%'d", COUNT(DISTINCT block_no)) as unique_rate_combinations,
  -- FORMAT("%'d", SUM(1)) as total_rates_count, 
  -- FORMAT("%'d", SUM(IF(rn = 1,1,0))) as starting_rates_count, 
  -- FORMAT("%'d",SUM(IF(price_change != 0,1,0))) as total_changed_rates, 
  -- FORMAT("%'d",SUM(IF(price_change = 0,1,0))) as total_unchanged_rates, 
  -- FORMAT('%s%%',CAST(ROUND(100*SAFE_DIVIDE(SUM(IF(price_change != 0, 1,0)),SUM(IF(rn != 1,1,0)) ))AS STRING)) as pct_changed_rates,
  -- FORMAT('%s%%',CAST(ROUND(100*SAFE_DIVIDE(SUM(IF(price_change = 0, 1,0)),SUM(IF(rn != 1,1,0)) ))AS STRING)) as pct_unchanged_rates,


  -- -- MIN(IF(since_last_change_min != 0, since_last_change_min, NULL)) as min_since_last_change_min,
  -- AVG(IF(since_last_change_min != 0, since_last_change_min, NULL)) as average,
  -- -- MAX(IF(since_last_change_min != 0, since_last_change_min, NULL)) as max_since_last_change_min, 
  -- -- APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)].value as mode_since_last_change_min,
  -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),4)[SAFE_OFFSET(1)] as q1_since_last_change_min,
  -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)] as median_since_last_change_min,
  -- -- APPROX_QUANTILES(IF(since_last_change_min != 0, since_last_change_min, NULL),4)[SAFE_OFFSET(3)] as q3_since_last_change_min,
  -- -- COALESCE(APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(0)].value,APPROX_TOP_COUNT(IF(since_last_change_min != 0, since_last_change_min, NULL),2)[SAFE_OFFSET(1)].value) as mode_since_last_change_min,
  -- MAX(p0) as p0,
  -- MAX(p5) as p5,
  -- MAX(p10) as p10,
  -- MAX(p15) as p15,
  -- MAX(p20) as p20,
  -- MAX(p25) as p25,
  -- MAX(p30) as p30,
  -- MAX(p35) as p35,
  -- MAX(p40) as p40,
  -- MAX(p45) as p45,
  -- MAX(p50) as p50,
  -- MAX(p55) as p55,
  -- MAX(p60) as p60,
  -- MAX(p65) as p65,
  -- MAX(p70) as p70,
  -- MAX(p75) as p75,
  -- MAX(p80) as p80,
  -- MAX(p85) as p85,
  -- MAX(p90) as p90,
  -- MAX(p95) as p95,
  -- MAX(p100) as p100,
  -- MAX(p75)-MAX(p25) as iqr
  FROM
  (SELECT *,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.00) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p0,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.05) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p5,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.10) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p10,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.15) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p15,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.20) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p20,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.25) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p25,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.30) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p30,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.35) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p35,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.40) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p40,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.45) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p45,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.50) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p50,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.55) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p55,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.60) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p60,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.65) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p65,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.70) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p70,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.75) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p75,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.80) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p80,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.85) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p85,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.90) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p90,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 0.95) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p95,
  -- CAST(PERCENTILE_CONT(IF(since_last_change_min != 0, since_last_change_min, NULL), 1.00) OVER (PARTITION BY supplier_name, leadtime, site_code, board_basis, supplier_channel, refund_term, length_of_stay, weekend_ind, star) AS INT64) AS p100

  FROM (
  SELECT * EXCEPT (leadtime, weekend_ind, length_of_stay),
  CASE WHEN leadtime = 0 THEN "01. Same day"
  WHEN leadtime = 1 THEN "02. Next day"
  WHEN leadtime >= 2 AND leadtime <= 7 THEN "03. 2-7 days"
  WHEN leadtime >= 8 AND leadtime <= 14 THEN "04. 8-14 days"
  WHEN leadtime >= 15 AND leadtime <= 30 THEN "05. 15-30 days"
  WHEN leadtime >= 31 AND leadtime <= 60 THEN "06. 31-60 days"
  WHEN leadtime >= 61 AND leadtime <= 90 THEN "07. 61-90 days"
  WHEN leadtime >= 91 AND leadtime <= 120 THEN "08. 91-120 days"
  WHEN leadtime >= 121 AND leadtime <= 150 THEN "09. 121-150 days"
  WHEN leadtime >= 151 AND leadtime <= 180 THEN "10. 151-180 days"
  WHEN leadtime > 180 THEN "11. Over 180 days" END as leadtime,
  
  CASE WHEN leadtime = 0 THEN 0
  WHEN leadtime = 1 THEN 1
  WHEN leadtime >= 2 AND leadtime <= 7 THEN 2
  WHEN leadtime >= 8 AND leadtime <= 14 THEN 8
  WHEN leadtime >= 15 AND leadtime <= 30 THEN 15
  WHEN leadtime >= 31 AND leadtime <= 60 THEN 31
  WHEN leadtime >= 61 AND leadtime <= 90 THEN 61
  WHEN leadtime >= 91 AND leadtime <= 120 THEN 91
  WHEN leadtime >= 121 AND leadtime <= 150 THEN 121
  WHEN leadtime >= 151 AND leadtime <= 180 THEN 151
  WHEN leadtime > 180 THEN 181 END as leadtime_start,

  CASE WHEN leadtime = 0 THEN 1
  WHEN leadtime = 1 THEN 2
  WHEN leadtime >= 2 AND leadtime <= 7 THEN 8
  WHEN leadtime >= 8 AND leadtime <= 14 THEN 15
  WHEN leadtime >= 15 AND leadtime <= 30 THEN 31
  WHEN leadtime >= 31 AND leadtime <= 60 THEN 61
  WHEN leadtime >= 61 AND leadtime <= 90 THEN 91
  WHEN leadtime >= 91 AND leadtime <= 120 THEN 121
  WHEN leadtime >= 121 AND leadtime <= 150 THEN 151
  WHEN leadtime >= 151 AND leadtime <= 180 THEN 181
  WHEN leadtime > 180 THEN 100000 END as leadtime_end,

  CASE WHEN length_of_stay = 1 THEN "01. 1 Day"
  WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN "02. 2-5 Days"
  WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN "03. 6-14 Days"
  WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN "04. 15-21 Days"
  WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN "05. 22-31 Days"
  WHEN length_of_stay >= 32 THEN "06. >= 32 Days"
  END as length_of_stay,

  CASE WHEN length_of_stay = 1 THEN 1
  WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN 2
  WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN 6
  WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN 15
  WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN 22
  WHEN length_of_stay >= 32 THEN 32
  END as length_of_stay_start,

  CASE WHEN length_of_stay = 1 THEN 2
  WHEN length_of_stay >= 2 AND length_of_stay < 6 THEN 6
  WHEN length_of_stay >= 6 AND length_of_stay < 15 THEN 15
  WHEN length_of_stay >= 15 AND length_of_stay < 22 THEN 22
  WHEN length_of_stay >= 22 AND length_of_stay < 32 THEN 32
  WHEN length_of_stay >= 32 THEN 100000
  END as length_of_stay_end,


  IF(weekend_ind > 0, 1, 0) as weekend_ind,
  -- IF(public_holiday_ind > 0, 1, 0) as public_holiday_ind,

  MAX(IF(price_change != 0 OR rn = 1, timestamp, NULL)) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as since_last_change_timestamp,
  IF(price_change != 0, TIMESTAMP_DIFF(timestamp, MAX(IF(price_change != 0 OR rn = 1, timestamp, NULL)) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), MINUTE), NULL) as since_last_change_min
  FROM
    (SELECT
    CONCAT(wego_hotel_id, " ", currency_code, " ", check_in_date, " ", check_out_date, " ", supplier_name, " ", room_type_name, " ", board_basis) as block_combination,
    DENSE_RANK() OVER (ORDER BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date)) as block_no,
    COUNT(1) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rate_count,
    ROW_NUMBER() OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) AS rn,

    IFNULL(TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room), MINUTE),0) as time_diff_min,
    LAG(timestamp) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) as previous_timestamp,
    timestamp,
    base_price_amount_room,
    LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room) as previous_base_price_amount_room,
    MIN(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min_base_price_amount_room,
    MAX(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_base_price_amount_room,
    ROUND(base_price_amount_room-LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room),2) as price_change,
    ROUND(100*SAFE_DIVIDE(base_price_amount_room-LAG(base_price_amount_room) OVER (PARTITION BY wego_hotel_id, currency_code, supplier_code, supplier_channel, room_type_name, board_basis, refund_term, site_code, CONCAT(check_in_date, check_out_date) ORDER BY timestamp, base_price_amount_room),base_price_amount_room),1) as pct_change,
    * EXCEPT (timestamp, base_price_amount_room),
    EXTRACT(HOUR FROM timestamp) AS timestamp_hour,
    EXTRACT(MINUTE FROM timestamp) AS timestamp_minute,
    EXTRACT(SECOND FROM timestamp) AS timestamp_second,
    EXTRACT(DAYOFWEEK FROM timestamp) AS timestamp_day_of_week,
    IF(EXTRACT(DAYOFWEEK FROM timestamp) IN (1,7),1,0) AS timestamp_weekend,
    DATE_DIFF(DATE(check_in_date), DATE(timestamp), DAY) as leadtime,
    EXTRACT(DAY FROM DATE(check_in_date)) AS check_in_date_day,
    EXTRACT(MONTH FROM DATE(check_in_date)) AS check_in_date_month,
    EXTRACT(DAYOFWEEK FROM DATE(check_in_date)) AS check_in_date_day_of_week,
    IF(EXTRACT(DAYOFWEEK FROM DATE(check_in_date)) IN (1,7),1,0) AS check_in_date_weekend,
    DATE_DIFF(DATE(check_out_date), DATE(check_in_date), DAY) as length_of_stay,
    -- (SELECT
    -- COUNT(DISTINCT holiday_date)
    -- -- STRUCT(holiday_date, id)
    -- FROM
    -- UNNEST(GENERATE_DATE_ARRAY(DATE(check_in_date), DATE(check_out_date), INTERVAL 1 DAY)) as date_list
    -- JOIN
    -- (SELECT  DATE(date) as holiday_date, MAX(site_code) as site_code
    -- FROM `wego-cloud.place_services.public_holidays`
    -- GROUP BY 1) as b
    -- ON date_list = b.holiday_date AND site_code = b.site_code
    -- ) as public_holiday_ind,

    (SELECT
    SUM(CASE WHEN site_code in ('DZ', 'BH', 'EG', 'IQ', 'IL', 'JO', 'KW', 'LY', 'MV', 'OM', 'QA', 'SA', 'SS', 'SY', 'YE', 'AE')
    AND EXTRACT(DAYOFWEEK FROM date_list) in (6,7) THEN 1
    WHEN EXTRACT(DAYOFWEEK FROM date_list) in (7,1) THEN 1
    ELSE 0 END) as weekend_ind
    FROM
    UNNEST(GENERATE_DATE_ARRAY(DATE(check_in_date), DATE(check_out_date), INTERVAL 1 DAY)) as date_list
    ) as weekend_ind

    FROM

      (SELECT
      MIN(timestamp) as timestamp,
      yorktown_session_id,
      device,
      supplier_code,
      supplier_name,
      wego_hotel_id,
      room_type_id,
      room_type_name,
      supplier_channel,
      refund_term,
      board_basis,
      currency_code,
      MIN(base_price_amount) as base_price_amount,
      MIN(tax_amount) as tax_amount,
      MIN(base_price_amount_room) as base_price_amount_room,
      id,
      city_code,
      check_in_date,
      check_out_date,
      rooms_count,
      currency,
      site_code,
      user_country_code,
      language_code,
      FROM
        (SELECT *,
        SAFE_DIVIDE(base_price_amount, rooms_count) as base_price_amount_room
        FROM

        (SELECT
        timestamp,
        yorktown_session_id,
        supplier_code,
        supplier_name,
        -- supplier_channel,
        #client_search_id,
        wego_hotel_id,
        room_type_id,
        room_type_name,
        supplier_channel,
        refund_term,
        board_basis,
        currency_code,
        base_price_amount,
        tax_amount,
        -- additional_charges.currency,
        -- additional_charges.amount,
        -- marketing_fee.currency,
        -- marketing_fee.amount,
        FROM `ib_hotels_supplier_worker.raw_rates*`
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
        -- AND wego_hotel_id = "987582"
        -- AND wego_hotel_id in (SELECT distinct hotel_id FROM hotel_details WHERE country_code = "SA")
        -- LIMIT 500
        -- AND room_type_name = "4BR FAMILY SUITE SEAVIEW"
        -- and refund_term = "no_refund"
        -- AND board_basis = "Room Only"
        -- AND supplier_channel = "mobile_app"
        -- AND supplier_name = "HPro"
        -- AND room_type_id = "DB"
        ) as a

        INNER JOIN

        (SELECT
        id,
        device,
        -- hotel_ids,
        city_code,
        check_in_date,
        check_out_date,
        -- city_code,
        rooms_count,
        currency,
        -- user_city,
        -- user_country_code,
        site_code,
        user_country_code,
        language_code,
        FROM `wego-cloud.ib_hotels.sessions*`
        WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d",start_date) AND FORMAT_DATE("%Y%m%d",end_date)
        -- AND check_in_date = "2023-07-13"
        -- AND check_out_date = "2023-07-14"
        -- AND user_country_code = "SA"
        -- AND site_code = "SA"

        -- WHERE hotel_id = "85314"
        -- AND site_code = "AE"
        ) as b

        ON a.yorktown_session_id = b.id
        -- ORDER BY yorktown_session_id, timestamp
      )
    WHERE user_country_code is not null
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,16,17,18,19,20,21,22,23,24
    ) as rates

    LEFT JOIN

    (SELECT DISTINCT * FROM hotel_details) as hotel_details

    ON rates.wego_hotel_id = hotel_details.hotel_id
  )
  -- WHERE min_base_price_amount_room != max_base_price_amount_room
  -- AND pct_change > 0
  -- AND block_no = 1764
  -- WHERE rn != 1
  -- WHERE rate_count > 5
  -- AND MOD(rn,2) = 0
    )
  -- WHERE since_last_change_min = 0
  -- WHERE block_combination = "1012379 USD 2023-08-31 2023-09-01 DOTW Honeymoon Suite Room Only"
  ORDER BY block_no, rn

  )
  -- GROUP BY 1,2,3
  -- ORDER BY 1,2,3 
  -- GROUP BY 1,2
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
  )
ORDER BY 1,2 DESC
{% endraw %}
