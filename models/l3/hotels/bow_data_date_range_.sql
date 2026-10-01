{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotel_sort_order_bqml_bow
-- Destination: hotel_sort_order_ml.bow_data_date_range_  (unchanged)
-- Schedule   : every mon 00:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
## HOTEL SORT ORDER RANDOM FOREST
--- a. Manual Date Range
-- DECLARE null_proportion FLOAT64;
-- DECLARE start_date DATE DEFAULT "2024-02-13";
-- DECLARE end_date DATE DEFAULT "2024-02-13";
-- DECLARE start_date_string STRING;
-- DECLARE end_date_string STRING;
-- DECLARE start_date_suffix STRING;
-- DECLARE end_date_suffix STRING;
-- SET start_date_string = CAST(start_date AS STRING);
-- SET end_date_string = CAST(end_date AS STRING);
-- SET start_date_suffix = FORMAT_DATE("%Y%m%d", start_date);
-- SET end_date_suffix = FORMAT_DATE("%Y%m%d", end_date);

--- b. Dynamic Date Range
DECLARE null_proportion FLOAT64;
DECLARE start_date DATE;
DECLARE end_date DATE;
DECLARE secondary_start_date DATE;
DECLARE secondary_end_date DATE;
DECLARE tertiary_start_date DATE;
DECLARE tertiary_end_date DATE;
DECLARE start_date_string STRING;
DECLARE end_date_string STRING;
DECLARE secondary_start_date_string STRING;
DECLARE secondary_end_date_string STRING;
DECLARE tertiary_start_date_string STRING;
DECLARE tertiary_end_date_string STRING;
DECLARE start_date_suffix STRING;
DECLARE end_date_suffix STRING;
DECLARE secondary_start_date_suffix STRING;
DECLARE secondary_end_date_suffix STRING;
DECLARE tertiary_start_date_suffix STRING;
DECLARE tertiary_end_date_suffix STRING;
DECLARE processing_date_suffix STRING;
DECLARE processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE processing_date DATE DEFAULT CURRENT_DATE();
DECLARE date_before_processing_date INT64 DEFAULT 1;
DECLARE date_windows_gap INT64 DEFAULT 1;
DECLARE date_window INT64 DEFAULT 29; -- i.e. If 12 day range, put 11 for window
DECLARE secondary_date_window INT64 DEFAULT 395; -- i.e. If 12 day range, put 11 for window
DECLARE tertiary_date_window INT64 DEFAULT 89; -- i.e. If 12 day range, put 11 for window
DECLARE i INT64 DEFAULT 0;
DECLARE model_count INT64;
DECLARE model_list ARRAY<STRING>;
DECLARE model_name STRING;
DECLARE j INT64 DEFAULT 0;
DECLARE table_count INT64;
DECLARE table_list ARRAY<STRING>;
DECLARE table_name STRING;

EXECUTE IMMEDIATE "SELECT DATE_SUB(DATE(?), INTERVAL ? DAY)" INTO end_date USING processing_timestamp, date_before_processing_date;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO start_date USING end_date, date_window;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO secondary_end_date USING start_date, date_windows_gap;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO secondary_start_date USING secondary_end_date, secondary_date_window;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO tertiary_end_date USING start_date, date_windows_gap;
EXECUTE IMMEDIATE "SELECT DATE_SUB(?, INTERVAL ? DAY)" INTO tertiary_start_date USING tertiary_end_date, tertiary_date_window;

SET start_date_string = CAST(start_date AS STRING);
SET end_date_string = CAST(end_date AS STRING);
SET secondary_start_date_string = CAST(secondary_start_date AS STRING);
SET secondary_end_date_string = CAST(secondary_end_date AS STRING);
SET tertiary_start_date_string = CAST(tertiary_start_date AS STRING);
SET tertiary_end_date_string = CAST(tertiary_end_date AS STRING);
SET start_date_suffix = FORMAT_DATE("%Y%m%d", start_date);
SET end_date_suffix = FORMAT_DATE("%Y%m%d", end_date);
SET secondary_start_date_suffix = FORMAT_DATE("%Y%m%d", secondary_start_date);
SET secondary_end_date_suffix = FORMAT_DATE("%Y%m%d", secondary_end_date);
SET tertiary_start_date_suffix = FORMAT_DATE("%Y%m%d", tertiary_start_date);
SET tertiary_end_date_suffix = FORMAT_DATE("%Y%m%d", tertiary_end_date);
SET processing_date_suffix = FORMAT_DATE("%Y%m%d", processing_date);

------- -1. Record Date Ranges ------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_data_date_range_%s AS (
SELECT 
@start_date AS start_date,
@end_date AS end_date, 
@date_window + 1 as date_window,
@secondary_start_date as secondary_start_date,
@secondary_end_date as secondary_end_date,
@secondary_date_window + 1 as secondary_date_window,
@tertiary_start_date as tertiary_start_date,
@tertiary_end_date as tertiary_end_date,
@tertiary_date_window + 1 as tertiary_date_window,
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
tertiary_start_date AS tertiary_start_date, 
tertiary_end_date AS tertiary_end_date, 
tertiary_date_window AS tertiary_date_window, 
processing_date AS processing_date;

-- ------ 0. Generate Training Dataset ------
EXECUTE IMMEDIATE 
FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_training_data_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT a.*, 
detail_pageviews_clicks_1mth,
detail_pageviews_clicks_3mth,
detail_pageviews_clicks_12mth,
conversions_1mth,
conversions_3mth,
conversions_12mth,
cvr_1mth,
cvr_3mth,
cvr_12mth,
gmv_1mth,
gmv_3mth,
gmv_12mth,
revenue_1mth,
revenue_3mth,
revenue_12mth,
avg_price_amount_per_night_usd, 
usual_price, 
100 * (1 - SAFE_DIVIDE(avg_price_amount_per_night_usd, usual_price)) as discount_percent,
-- min_displayed_position,
-- avg_displayed_position,
total_itinerary_card_clicks,
total_conversions,
total_converted_sessions, 
total_gmv,
total_revenue,
CURRENT_DATE() as processing_date 

FROM
    (SELECT
    pl.site_code as site_code, pl.locale as locale,
    hs.hotel_id as hotel_id, hotel_name, hs.chain_id as chain_id, chain_name, hs.brand_id as brand_id, brand_name, hs.country as country, hs.country_code as country_code, hs.location as location, hs.location_id as location_id, hs.location_code as location_code, hs.district_id as district_id, hs.district_name as district_name,
    hotel_star_rating, image_count, overall_score, reviews_count, property_type_id, property_type_name, distance_to_city_centre,
    ihg_ind,
    FROM
    (SELECT *,
    FROM
        (SELECT
        CAST(h.id AS STRING) as hotel_id, h.name_en as hotel_name, brand.chain_id as chain_id, chain_name, h.brand_id as brand_id, brand_name, c.base_name as country, c.code as country_code, l.base_name as location, l.code as location_code, l.id as location_id, h.district_id as district_id, dt.district_name as district_name, star as hotel_star_rating,
        img.image_count as image_count, trustyou.score as overall_score, trustyou.reviews_count as reviews_count, distance_to_city_centre, h.property_type_id, property_type_name,
        IF(built_year IS NULL OR SAFE_CAST(built_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(built_year AS INT64) < 1900, NULL, SAFE_CAST(built_year AS INT64)) as built_year, -- Force null for strange years & less than 1900
        IF(renovated_year IS NULL OR SAFE_CAST(renovated_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(renovated_year AS INT64) < 1900, NULL, SAFE_CAST(renovated_year AS INT64)) as renovated_year, -- Force null for strange years & less than 1900
        ihg_ind
        FROM `wego-cloud.hotel_services.hotels` as h
        LEFT JOIN
        (SELECT * EXCEPT (rn)
        FROM (SELECT base_name, code, id, country_id, RANK() OVER (PARTITION BY code ORDER BY updated_at DESC) as rn
        FROM `wego-cloud.place_services.locations` )
        WHERE rn = 1) as l on l.code=h.city_code
        LEFT JOIN `wego-cloud.place_services.countries` as c on c.id=l.country_id
        LEFT JOIN
        (SELECT id, ANY_VALUE(base_name) as district_name 
        FROM `wego-cloud.place_services.districts` 
        GROUP BY 1) as dt on h.district_id = dt.id
    --            LEFT JOIN `wego-cloud.hotels.hotel_stats` as st on st.id=h.id
    --            LEFT JOIN
    --                (SELECT hotel_id, MAX(score) as score FROM hotel_services.reviews
    --                WHERE reviewer_group='ALL' GROUP BY hotel_id) as trustyou on trustyou.hotel_id=h.id

        LEFT JOIN

        (SELECT hotel_id,
        IF(COUNTIF(provider_id IN (18, 830)) > 0,1,0) as ihg_ind
        FROM `wego-cloud.hotels.provider_hotels`
        GROUP BY 1) as ph

        ON h.id = ph.hotel_id 

        LEFT JOIN

        (SELECT hotel_id, sum(1) as image_count
        FROM `wego-cloud.hotel_services.images`
        GROUP BY hotel_id) as img

        ON img.hotel_id=h.id

        LEFT JOIN

        (SELECT hotel_id, score, reviews_count,
        -- IF(reviews_count IS NULL, AVG(reviews_count) OVER (), SAFE_DIVIDE(reviews_count,(MAX(reviews_count) OVER () - MIN(reviews_count) OVER()))) as reviews_count_weighted -- reviews_count/(max_reviews_count-min_reviews_count), otherwise take average.
        FROM
            (SELECT hotel_id, MAX(score) as score, MAX(count) as reviews_count
            FROM hotel_services.reviews
            WHERE reviewer_group='ALL'
            GROUP BY hotel_id)
        ) as trustyou

        ON trustyou.hotel_id=h.id

        LEFT JOIN

        (SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
        FROM `wego-cloud.hotel_services.brands`) as brand
        ON h.brand_id = brand.brand_id

        LEFT JOIN

        (SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
        FROM `wego-cloud.hotel_services.chains`) as chain
        ON chain.chain_id = brand.chain_id

        LEFT JOIN

        (SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
        FROM `hotel_services.property_types`) as property_type
        on h.property_type_id = property_type.property_type_id

        )
    ) as hs

    CROSS JOIN

    (SELECT site_code, locale
    FROM `wego-cloud.analytics.wego_pos`
    GROUP BY site_code, locale) as pl

) as a

LEFT JOIN

(SELECT 
site_code,
locale,
hotel_id, 
-- MAX(min_displayed_position) as min_displayed_position,
-- MAX(avg_displayed_position) as avg_displayed_position,
SUM(IFNULL(total_detail_pageviews_clicks,0)) as total_itinerary_card_clicks,
FROM

(SELECT site_code, locale, event_id as hotel_id, session_id, SUM(1) as total_detail_pageviews_clicks
FROM `wego-cloud.wego_analytics.pageviews`
WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
AND page_type = "hotels_detail_page_live"
GROUP BY 1,2,3,4) as a

-- LEFT JOIN

-- (SELECT o.id as hotel_id,
-- MIN(o.order) as min_displayed_position,
-- AVG(o.order) as avg_displayed_position,
-- FROM 
-- (SELECT *, MAX(created_at) OVER (PARTITION BY search_id) AS latest_pageload_time
-- FROM `wego-cloud.services_genzo.impressions_logs*`
-- WHERE _TABLE_SUFFIX BETWEEN "%s" AND "%s" -- Adjust the date range as needed
-- AND impression.page = "hotels_search_results"
-- AND impression.trigger = "page_load") il,
-- UNNEST(objects) o
-- WHERE il.created_at = il.latest_pageload_time
-- AND o.itinerary_category = "meta"
-- GROUP BY 1) as c

-- ON a.hotel_id = c.hotel_id

GROUP BY 1,2,3
HAVING hotel_id IS NOT NULL) as c

ON a.site_code = c.site_code AND a.locale = c.locale AND a.hotel_id = c.hotel_id

LEFT JOIN

(SELECT
site_code,
locale,
CAST(hotel_id AS STRING) AS hotel_id,
SUM(conversions_tracked) AS total_conversions, 
COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
SUM(IF(conversions_tracked > 0, wego_total_price_usd, NULL)) as total_gmv,
SUM(revenue_in_usd) as total_revenue
FROM `wego-cloud.wego_analytics.hotels_bookings`
WHERE DATE(created_at) BETWEEN "%s" AND "%s"
GROUP BY 1,2,3) as hotels_bookings_table

ON a.site_code = hotels_bookings_table.site_code AND a.locale = hotels_bookings_table.locale AND a.hotel_id = hotels_bookings_table.hotel_id

LEFT JOIN

(SELECT 
* EXCEPT (total_conversions, total_converted_sessions, total_gmv, total_revenue),
total_conversions as conversions_1mth,
total_converted_sessions as converted_sessions_1mth,
total_gmv as gmv_1mth,
total_revenue as revenue_1mth,
total_detail_pageviews_clicks as detail_pageviews_clicks_1mth,

ROUND(SAFE_DIVIDE(total_conversions, total_detail_pageviews_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, detail_pageviews_clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, detail_pageviews_clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, detail_pageviews_clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, detail_pageviews_clicks_12mth) * 100, 2) AS cvr_12mth,
FROM 
  (SELECT 
  CAST(hotel_id AS STRING) as hotel_id, 
  month,
  total_detail_pageviews_clicks,
  total_conversions,
  total_converted_sessions,
  total_gmv,
  total_revenue,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS detail_pageviews_clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS revenue_2mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS detail_pageviews_clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS revenue_3mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS detail_pageviews_clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS revenue_6mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS detail_pageviews_clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS revenue_12mth,
  row_number
  FROM
  (SELECT a.hotel_id as hotel_id,
  a.month as month,
  * EXCEPT (hotel_id, month), 
  ROW_NUMBER() OVER (PARTITION BY a.hotel_id ORDER BY a.month DESC) AS row_number
  FROM

    (SELECT id as hotel_id, month 
    FROM `wego-cloud.hotel_services.hotels` 
    CROSS JOIN 
    (SELECT FORMAT_TIMESTAMP('%%Y-%%m', month_list) as month 
    FROM UNNEST(GENERATE_DATE_ARRAY("%s", LAST_DAY(DATE_SUB("%s" , INTERVAL 1 MONTH))  , INTERVAL 1 MONTH)) AS month_list)
    ) as a

    LEFT JOIN

    (SELECT
    hotel_id, 
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    SUM(conversions_tracked) AS total_conversions, 
    COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
    SUM(IF(conversions_tracked > 0, wego_total_price_usd, NULL)) as total_gmv,
    SUM(revenue_in_usd) as total_revenue
    FROM `wego-cloud.wego_analytics.hotels_bookings`
    WHERE DATE(created_at) BETWEEN "%s" AND "%s"
    GROUP BY 1,2
    ) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT CAST(event_id AS INT64) as hotel_id, 
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    SUM(1) as total_detail_pageviews_clicks
    FROM `wego-cloud.wego_analytics.pageviews`
    WHERE DATE(created_at) BETWEEN "%s" AND "%s"
    AND page_type = "hotels_detail_page_live"
    GROUP BY 1,2) as d

    ON a.hotel_id = d.hotel_id AND a.month = d.month

    )
  )
  WHERE row_number = 1
) as lagging_ind

ON lagging_ind.hotel_id=a.hotel_id

LEFT JOIN 

-- Recent BoW Price
-- AVG USD price per hotel from 5 days of Akasha Rates (Dates Limited as it is expensive, 82GB)
(SELECT hotel_id, 
AVG(price.amount_per_night_usd) as avg_price_amount_per_night_usd, 
FROM `wego-cloud.services_akasha.rates*` 
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB("%s", INTERVAL 4 DAY)) AND FORMAT_DATE('%%Y%%m%%d',"%s")
AND provider.code = "hotels.wego.com"
GROUP BY 1
) as akasha_price

ON akasha_price.hotel_id=a.hotel_id

LEFT JOIN 

-- Usual Price
(SELECT
hotel_id,
AVG(price) AS usual_price
FROM `wego-cloud.hotels.usual_price`
WHERE month IN (
SELECT EXTRACT(MONTH FROM DATE_SUB(DATE("%s"), INTERVAL i MONTH))
FROM UNNEST(GENERATE_ARRAY(0, 2)) AS i
)
GROUP BY hotel_id) as up

ON up.hotel_id=a.hotel_id


)
"""
, processing_date_suffix, start_date_string, end_date_string, start_date_suffix, end_date_suffix, start_date_string, end_date_string,
secondary_start_date_string, secondary_end_date_string, secondary_start_date_string, secondary_end_date_string, secondary_start_date_string, secondary_end_date_string,
end_date_string, end_date_string, end_date_string
);


-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT ROUND(SAFE_DIVIDE(cases_not_null,cases_null),10) as null_to_not_null_deci_pct 
FROM
  (SELECT COUNTIF(total_itinerary_card_clicks IS NULL) AS cases_null,
  COUNTIF(total_itinerary_card_clicks IS NOT NULL) AS cases_not_null,
  SUM(1) AS total_cases
  FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s`
  -- WHERE avg_displayed_position <= (
  --   SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
  --   FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s`
  -- )
  -- OR avg_displayed_position IS NULL
  ) 
"""
, processing_date_suffix, processing_date_suffix)
INTO null_proportion
;

----- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s` 
WHERE total_itinerary_card_clicks IS NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s`)
-- OR avg_displayed_position IS NULL
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s` 
WHERE total_itinerary_card_clicks IS NOT NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s`)
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.bow_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_itinerary_card_clicks']) AS
SELECT
site_code,
locale,
-- hotel_id,
hotel_star_rating,
chain_id,
brand_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
detail_pageviews_clicks_3mth, 
conversions_3mth,
detail_pageviews_clicks_1mth, 
conversions_1mth,
cvr_3mth,
cvr_1mth,
revenue_1mth,
revenue_3mth,
gmv_1mth,
gmv_3mth,
avg_price_amount_per_night_usd, 
IFNULL(total_itinerary_card_clicks,0) as total_itinerary_card_clicks
FROM `hotel_sort_order_ml.bow_training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.bow_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.bow_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.bow_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

-- --- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.bow_rf_model_%s`,
-- (SELECT 
-- -- -- Additional IDs 
-- hotel_id,
-- hotel_name,
-- country,
-- country_code,
-- location,
-- location_id, 
-- location_code,
-- total_itinerary_card_clicks,
-- -- -- Main Features 
-- site_code,
-- locale,
-- -- hotel_id,
-- hotel_star_rating,
-- chain_id,
-- brand_id,
-- image_count,
-- overall_score,
-- reviews_count,
-- property_type_id,
-- distance_to_city_centre,
-- detail_pageviews_clicks_3mth, 
-- conversions_3mth,
-- detail_pageviews_clicks_1mth, 
-- conversions_1mth,
-- cvr_3mth,
-- cvr_1mth,
-- revenue_1mth,
-- revenue_3mth,
-- gmv_1mth,
-- gmv_3mth,
-- avg_price_amount_per_night_usd, 
-- FROM `wego-cloud.hotel_sort_order_ml.bow_training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- -- 8. Productionise it in Vertex AI
-- -- EXECUTE IMMEDIATE FORMAT("""
-- -- ALTER MODEL hotel_sort_order_ml.bow_rf_model_%s 
-- -- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_bow_rf_model')
-- -- """
-- -- , start_date_suffix);

-- -- -- -- DROP MODEL IF EXISTS hotel_sort_order_ml.bow_rf_model20240213

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.bow_rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT * , CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.bow_rf_model_%s`,
(SELECT
-- -- Additional IDs 
hotel_id,
hotel_name,
country,
country_code,
location,
location_id, 
location_code,
chain_name,
brand_name, 
property_type_name,
ihg_ind,
total_itinerary_card_clicks,
-- -- Main Features 
site_code,
locale,
-- hotel_id,
hotel_star_rating,
chain_id,
brand_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
detail_pageviews_clicks_3mth, 
conversions_3mth,
detail_pageviews_clicks_1mth, 
conversions_1mth,
cvr_3mth,
cvr_1mth,
revenue_1mth,
revenue_3mth,
gmv_1mth,
gmv_3mth,
avg_price_amount_per_night_usd, 
FROM `hotel_sort_order_ml.bow_training_data_%s` 
-- -- WHERE total_itinerary_card_clicks IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_wa_ml AS (
SELECT * FROM `hotel_sort_order_ml.bow_rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_pivot_wa_ml AS (
SELECT
hotel_id,
location_id,
location_code,
sum(if(site_code='AE' and locale='en', formula, 0)) as AE_en,
sum(if(site_code='AE' and locale='ar', formula, 0)) as AE_ar,
sum(if(site_code='AR' and locale='en', formula, 0)) as AR_en,
sum(if(site_code='AR' and locale='es-419', formula, 0)) as AR_es_419,
sum(if(site_code='AU' and locale='en', formula, 0)) as AU_en,
sum(if(site_code='BD' and locale='en', formula, 0)) as BD_en,
sum(if(site_code='BH' and locale='ar', formula, 0)) as BH_ar,
sum(if(site_code='BH' and locale='en', formula, 0)) as BH_en,
sum(if(site_code='BR' and locale='en', formula, 0)) as BR_en,
sum(if(site_code='BR' and locale='pt-br', formula, 0)) as BR_pt_br,
sum(if(site_code='CA' and locale='en', formula, 0)) as CA_en,
sum(if(site_code='CA' and locale='fr', formula, 0)) as CA_fr,
sum(if(site_code='CH' and locale='de', formula, 0)) as CH_de,
sum(if(site_code='CH' and locale='en', formula, 0)) as CH_en,
sum(if(site_code='CH' and locale='fr', formula, 0)) as CH_fr,
sum(if(site_code='CH' and locale='it', formula, 0)) as CH_it,
sum(if(site_code='CL' and locale='en', formula, 0)) as CL_en,
sum(if(site_code='CL' and locale='es-419', formula, 0)) as CL_es_419,
sum(if(site_code='CN' and locale='en', formula, 0)) as CN_en,
sum(if(site_code='CN' and locale='zh-cn', formula, 0)) as CN_zh_cn,
sum(if(site_code='CO' and locale='en', formula, 0)) as CO_en,
sum(if(site_code='CO' and locale='es-419', formula, 0)) as CO_es_419,
sum(if(site_code='DE' and locale='en', formula, 0)) as DE_en,
sum(if(site_code='DE' and locale='de', formula, 0)) as DE_de,
sum(if(site_code='DZ' and locale='ar', formula, 0)) as DZ_ar,
sum(if(site_code='DZ' and locale='fr', formula, 0)) as DZ_fr,
sum(if(site_code='DZ' and locale='en', formula, 0)) as DZ_en,
sum(if(site_code='EG' and locale='ar', formula, 0)) as EG_ar,
sum(if(site_code='EG' and locale='en', formula, 0)) as EG_en,
sum(if(site_code='ES' and locale='en', formula, 0)) as ES_en,
sum(if(site_code='ES' and locale='es', formula, 0)) as ES_es,
sum(if(site_code='FR' and locale='en', formula, 0)) as FR_en,
sum(if(site_code='FR' and locale='fr', formula, 0)) as FR_fr,
sum(if(site_code='GB' and locale='en', formula, 0)) as GB_en,
sum(if(site_code='GH' and locale='en', formula, 0)) as GH_en,
sum(if(site_code='HK' and locale='en', formula, 0)) as HK_en,
sum(if(site_code='HK' and locale='zh-hk', formula, 0)) as HK_zh_hk,
sum(if(site_code='ID' and locale='en', formula, 0)) as ID_en,
sum(if(site_code='ID' and locale='id', formula, 0)) as ID_id,
sum(if(site_code='IE' and locale='en', formula, 0)) as IE_en,
sum(if(site_code='IN' and locale='en', formula, 0)) as IN_en,
sum(if(site_code='IR' and locale='en', formula, 0)) as IR_en,
sum(if(site_code='IR' and locale='fa', formula, 0)) as IR_fa,
sum(if(site_code='IT' and locale='en', formula, 0)) as IT_en,
sum(if(site_code='IT' and locale='it', formula, 0)) as IT_it,
sum(if(site_code='JO' and locale='ar', formula, 0)) as JO_ar,
sum(if(site_code='JO' and locale='en', formula, 0)) as JO_en,
sum(if(site_code='JP' and locale='en', formula, 0)) as JP_en,
sum(if(site_code='JP' and locale='ja', formula, 0)) as JP_ja,
sum(if(site_code='KR' and locale='en', formula, 0)) as KR_en,
sum(if(site_code='KR' and locale='ko', formula, 0)) as KR_ko,
sum(if(site_code='KW' and locale='ar', formula, 0)) as KW_ar,
sum(if(site_code='KW' and locale='en', formula, 0)) as KW_en,
sum(if(site_code='LK' and locale='en', formula, 0)) as LK_en,
sum(if(site_code='MA' and locale='ar', formula, 0)) as MA_ar,
sum(if(site_code='MA' and locale='fr', formula, 0)) as MA_fr,
sum(if(site_code='MA' and locale='en', formula, 0)) as MA_en,
sum(if(site_code='MX' and locale='en', formula, 0)) as MX_en,
sum(if(site_code='MX' and locale='es-419', formula, 0)) as MX_es_419,
sum(if(site_code='MY' and locale='en', formula, 0)) as MY_en,
sum(if(site_code='MY' and locale='ms', formula, 0)) as MY_ms,
sum(if(site_code='MY' and locale='zh-cn', formula, 0)) as MY_zh_cn,
sum(if(site_code='NG' and locale='en', formula, 0)) as NG_en,
sum(if(site_code='NL' and locale='en', formula, 0)) as NL_en,
sum(if(site_code='NL' and locale='nl', formula, 0)) as NL_nl,
sum(if(site_code='NZ' and locale='en', formula, 0)) as NZ_en,
sum(if(site_code='OM' and locale='ar', formula, 0)) as OM_ar,
sum(if(site_code='OM' and locale='en', formula, 0)) as OM_en,
sum(if(site_code='PH' and locale='en', formula, 0)) as PH_en,
sum(if(site_code='PK' and locale='en', formula, 0)) as PK_en,
sum(if(site_code='PL' and locale='en', formula, 0)) as PL_en,
sum(if(site_code='PL' and locale='pl', formula, 0)) as PL_pl,
sum(if(site_code='PT' and locale='en', formula, 0)) as PT_en,
sum(if(site_code='PT' and locale='pt', formula, 0)) as PT_pt,
sum(if(site_code='QA' and locale='ar', formula, 0)) as QA_ar,
sum(if(site_code='QA' and locale='en', formula, 0)) as QA_en,
sum(if(site_code='RU' and locale='en', formula, 0)) as RU_en,
sum(if(site_code='RU' and locale='ru', formula, 0)) as RU_ru,
sum(if(site_code='SA' and locale='ar', formula, 0)) as SA_ar,
sum(if(site_code='SA' and locale='en', formula, 0)) as SA_en,
sum(if(site_code='SE' and locale='en', formula, 0)) as SE_en,
sum(if(site_code='SE' and locale='sv', formula, 0)) as SE_sv,
sum(if(site_code='SG' and locale='en', formula, 0)) as SG_en,
sum(if(site_code='SG' and locale='ms', formula, 0)) as SG_ms,
sum(if(site_code='SG' and locale='zh-cn', formula, 0)) as SG_zh_cn,
sum(if(site_code='TH' and locale='en', formula, 0)) as TH_en,
sum(if(site_code='TH' and locale='th', formula, 0)) as TH_th,
sum(if(site_code='TN' and locale='ar', formula, 0)) as TN_ar,
sum(if(site_code='TN' and locale='fr', formula, 0)) as TN_fr,
sum(if(site_code='TN' and locale='en', formula, 0)) as TN_en,
sum(if(site_code='TR' and locale='en', formula, 0)) as TR_en,
sum(if(site_code='TR' and locale='tr', formula, 0)) as TR_tr,
sum(if(site_code='TW' and locale='en', formula, 0)) as TW_en,
sum(if(site_code='TW' and locale='zh-tw', formula, 0)) as TW_zh_tw,
sum(if(site_code='US' and locale='en', formula, 0)) as US_en,
sum(if(site_code='VN' and locale='en', formula, 0)) as VN_en,
sum(if(site_code='VN' and locale='vi', formula, 0)) as VN_vi,
sum(if(site_code='ZA' and locale='en', formula, 0)) as ZA_en
FROM
  (SELECT *,
  SAFE_DIVIDE(predicted_total_itinerary_card_clicks - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id),  
  MAX(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
  as formula
  FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml`
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_prioritisation Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_pivot_wa_ml_prioritisation AS (
SELECT
hotel_id,
location_id,
location_code,
sum(if(site_code='AE' and locale='en', formula, 0)) as AE_en,
sum(if(site_code='AE' and locale='ar', formula, 0)) as AE_ar,
sum(if(site_code='AR' and locale='en', formula, 0)) as AR_en,
sum(if(site_code='AR' and locale='es-419', formula, 0)) as AR_es_419,
sum(if(site_code='AU' and locale='en', formula, 0)) as AU_en,
sum(if(site_code='BD' and locale='en', formula, 0)) as BD_en,
sum(if(site_code='BH' and locale='ar', formula, 0)) as BH_ar,
sum(if(site_code='BH' and locale='en', formula, 0)) as BH_en,
sum(if(site_code='BR' and locale='en', formula, 0)) as BR_en,
sum(if(site_code='BR' and locale='pt-br', formula, 0)) as BR_pt_br,
sum(if(site_code='CA' and locale='en', formula, 0)) as CA_en,
sum(if(site_code='CA' and locale='fr', formula, 0)) as CA_fr,
sum(if(site_code='CH' and locale='de', formula, 0)) as CH_de,
sum(if(site_code='CH' and locale='en', formula, 0)) as CH_en,
sum(if(site_code='CH' and locale='fr', formula, 0)) as CH_fr,
sum(if(site_code='CH' and locale='it', formula, 0)) as CH_it,
sum(if(site_code='CL' and locale='en', formula, 0)) as CL_en,
sum(if(site_code='CL' and locale='es-419', formula, 0)) as CL_es_419,
sum(if(site_code='CN' and locale='en', formula, 0)) as CN_en,
sum(if(site_code='CN' and locale='zh-cn', formula, 0)) as CN_zh_cn,
sum(if(site_code='CO' and locale='en', formula, 0)) as CO_en,
sum(if(site_code='CO' and locale='es-419', formula, 0)) as CO_es_419,
sum(if(site_code='DE' and locale='en', formula, 0)) as DE_en,
sum(if(site_code='DE' and locale='de', formula, 0)) as DE_de,
sum(if(site_code='DZ' and locale='ar', formula, 0)) as DZ_ar,
sum(if(site_code='DZ' and locale='fr', formula, 0)) as DZ_fr,
sum(if(site_code='DZ' and locale='en', formula, 0)) as DZ_en,
sum(if(site_code='EG' and locale='ar', formula, 0)) as EG_ar,
sum(if(site_code='EG' and locale='en', formula, 0)) as EG_en,
sum(if(site_code='ES' and locale='en', formula, 0)) as ES_en,
sum(if(site_code='ES' and locale='es', formula, 0)) as ES_es,
sum(if(site_code='FR' and locale='en', formula, 0)) as FR_en,
sum(if(site_code='FR' and locale='fr', formula, 0)) as FR_fr,
sum(if(site_code='GB' and locale='en', formula, 0)) as GB_en,
sum(if(site_code='GH' and locale='en', formula, 0)) as GH_en,
sum(if(site_code='HK' and locale='en', formula, 0)) as HK_en,
sum(if(site_code='HK' and locale='zh-hk', formula, 0)) as HK_zh_hk,
sum(if(site_code='ID' and locale='en', formula, 0)) as ID_en,
sum(if(site_code='ID' and locale='id', formula, 0)) as ID_id,
sum(if(site_code='IE' and locale='en', formula, 0)) as IE_en,
sum(if(site_code='IN' and locale='en', formula, 0)) as IN_en,
sum(if(site_code='IR' and locale='en', formula, 0)) as IR_en,
sum(if(site_code='IR' and locale='fa', formula, 0)) as IR_fa,
sum(if(site_code='IT' and locale='en', formula, 0)) as IT_en,
sum(if(site_code='IT' and locale='it', formula, 0)) as IT_it,
sum(if(site_code='JO' and locale='ar', formula, 0)) as JO_ar,
sum(if(site_code='JO' and locale='en', formula, 0)) as JO_en,
sum(if(site_code='JP' and locale='en', formula, 0)) as JP_en,
sum(if(site_code='JP' and locale='ja', formula, 0)) as JP_ja,
sum(if(site_code='KR' and locale='en', formula, 0)) as KR_en,
sum(if(site_code='KR' and locale='ko', formula, 0)) as KR_ko,
sum(if(site_code='KW' and locale='ar', formula, 0)) as KW_ar,
sum(if(site_code='KW' and locale='en', formula, 0)) as KW_en,
sum(if(site_code='LK' and locale='en', formula, 0)) as LK_en,
sum(if(site_code='MA' and locale='ar', formula, 0)) as MA_ar,
sum(if(site_code='MA' and locale='fr', formula, 0)) as MA_fr,
sum(if(site_code='MA' and locale='en', formula, 0)) as MA_en,
sum(if(site_code='MX' and locale='en', formula, 0)) as MX_en,
sum(if(site_code='MX' and locale='es-419', formula, 0)) as MX_es_419,
sum(if(site_code='MY' and locale='en', formula, 0)) as MY_en,
sum(if(site_code='MY' and locale='ms', formula, 0)) as MY_ms,
sum(if(site_code='MY' and locale='zh-cn', formula, 0)) as MY_zh_cn,
sum(if(site_code='NG' and locale='en', formula, 0)) as NG_en,
sum(if(site_code='NL' and locale='en', formula, 0)) as NL_en,
sum(if(site_code='NL' and locale='nl', formula, 0)) as NL_nl,
sum(if(site_code='NZ' and locale='en', formula, 0)) as NZ_en,
sum(if(site_code='OM' and locale='ar', formula, 0)) as OM_ar,
sum(if(site_code='OM' and locale='en', formula, 0)) as OM_en,
sum(if(site_code='PH' and locale='en', formula, 0)) as PH_en,
sum(if(site_code='PK' and locale='en', formula, 0)) as PK_en,
sum(if(site_code='PL' and locale='en', formula, 0)) as PL_en,
sum(if(site_code='PL' and locale='pl', formula, 0)) as PL_pl,
sum(if(site_code='PT' and locale='en', formula, 0)) as PT_en,
sum(if(site_code='PT' and locale='pt', formula, 0)) as PT_pt,
sum(if(site_code='QA' and locale='ar', formula, 0)) as QA_ar,
sum(if(site_code='QA' and locale='en', formula, 0)) as QA_en,
sum(if(site_code='RU' and locale='en', formula, 0)) as RU_en,
sum(if(site_code='RU' and locale='ru', formula, 0)) as RU_ru,
sum(if(site_code='SA' and locale='ar', formula, 0)) as SA_ar,
sum(if(site_code='SA' and locale='en', formula, 0)) as SA_en,
sum(if(site_code='SE' and locale='en', formula, 0)) as SE_en,
sum(if(site_code='SE' and locale='sv', formula, 0)) as SE_sv,
sum(if(site_code='SG' and locale='en', formula, 0)) as SG_en,
sum(if(site_code='SG' and locale='ms', formula, 0)) as SG_ms,
sum(if(site_code='SG' and locale='zh-cn', formula, 0)) as SG_zh_cn,
sum(if(site_code='TH' and locale='en', formula, 0)) as TH_en,
sum(if(site_code='TH' and locale='th', formula, 0)) as TH_th,
sum(if(site_code='TN' and locale='ar', formula, 0)) as TN_ar,
sum(if(site_code='TN' and locale='fr', formula, 0)) as TN_fr,
sum(if(site_code='TN' and locale='en', formula, 0)) as TN_en,
sum(if(site_code='TR' and locale='en', formula, 0)) as TR_en,
sum(if(site_code='TR' and locale='tr', formula, 0)) as TR_tr,
sum(if(site_code='TW' and locale='en', formula, 0)) as TW_en,
sum(if(site_code='TW' and locale='zh-tw', formula, 0)) as TW_zh_tw,
sum(if(site_code='US' and locale='en', formula, 0)) as US_en,
sum(if(site_code='VN' and locale='en', formula, 0)) as VN_en,
sum(if(site_code='VN' and locale='vi', formula, 0)) as VN_vi,
sum(if(site_code='ZA' and locale='en', formula, 0)) as ZA_en
FROM
  (SELECT *
  ----- Manual override of scores based on hotels and manual override score listed in LEFT JOIN b -----
  -- a.* EXCEPT (formula), COALESCE(manual_override_score, formula) AS formula
  FROM
    (SELECT * EXCEPT (formula, x_position_formula), 
    -- IF(ihg_ind > 0, PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id) + (100000000000-PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id))*RAND(), formula)*100 + RAND()*100 as formula
    ----- Boosting transformation to push ihg properties to the similar score to 6th position but with strength of 0.85 -----
    IF(ihg_ind > 0, IF(formula < x_position_formula, (SAFE_DIVIDE(formula, 100000000000) + (SAFE_DIVIDE(x_position_formula,100000000000) - SAFE_DIVIDE(formula, 100000000000))*0.85)*100000000000, formula), formula) as formula
    FROM 
      (SELECT * EXCEPT (ranking), MAX(CASE WHEN ranking = 6 THEN formula END) OVER (PARTITION BY site_code, locale, location_id) as x_position_formula 
      FROM
        (SELECT *, ROW_NUMBER() OVER (PARTITION BY site_code, locale, location_id ORDER BY formula DESC) ranking
        FROM
          (----- PARTITION OVER BY site_code, locale, location_id means range of 0 to 100000000000 for each site_code, locale, location_id combination -----
          SELECT *,
          SAFE_DIVIDE(predicted_total_itinerary_card_clicks - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id),  
          MAX(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
          as formula
          FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml`
          )
        )
      )
    ) as a

    -- LEFT JOIN
  
    -- (SELECT *
    -- FROM UNNEST([
    -- STRUCT("2146679" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85329" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85616" AS hotel_id, 150000000000 AS manual_override_score)
    -- ])) as b

    -- ON a.hotel_id = b.hotel_id
  )

GROUP BY hotel_id, location_id, location_code
);


------------------------- V1: Price Per Night Target Metric ---------------------------------
-- ------ 0. Generate Training Dataset ------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_training_data_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT a.*, 
detail_pageviews_clicks_1mth,
detail_pageviews_clicks_3mth,
detail_pageviews_clicks_12mth,
conversions_1mth,
conversions_3mth,
conversions_12mth,
cvr_1mth,
cvr_3mth,
cvr_12mth,
gmv_1mth,
gmv_3mth,
gmv_12mth,
revenue_1mth,
revenue_3mth,
revenue_12mth,
avg_price_amount_per_night_usd, 
usual_price, 
100 * (1 - SAFE_DIVIDE(avg_price_amount_per_night_usd, usual_price)) as discount_percent,
-- min_displayed_position,
-- avg_displayed_position,
total_itinerary_card_clicks,
total_conversions,
total_converted_sessions, 
total_gmv,
total_revenue,
CURRENT_DATE() as processing_date 

FROM
    (SELECT
    pl.site_code as site_code, pl.locale as locale,
    hs.hotel_id as hotel_id, hotel_name, hs.chain_id as chain_id, chain_name, hs.brand_id as brand_id, brand_name, hs.country as country, hs.country_code as country_code, hs.location as location, hs.location_id as location_id, hs.location_code as location_code, hs.district_id as district_id, hs.district_name as district_name,
    hotel_star_rating, image_count, overall_score, reviews_count, property_type_id, property_type_name, distance_to_city_centre,
    ihg_ind,
    FROM
    (SELECT *,
    FROM
        (SELECT
        CAST(h.id AS STRING) as hotel_id, h.name_en as hotel_name, brand.chain_id as chain_id, chain_name, h.brand_id as brand_id, brand_name, c.base_name as country, c.code as country_code, l.base_name as location, l.code as location_code, l.id as location_id, h.district_id as district_id, dt.district_name as district_name, star as hotel_star_rating,
        img.image_count as image_count, trustyou.score as overall_score, trustyou.reviews_count as reviews_count, distance_to_city_centre, h.property_type_id, property_type_name,
        IF(built_year IS NULL OR SAFE_CAST(built_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(built_year AS INT64) < 1900, NULL, SAFE_CAST(built_year AS INT64)) as built_year, -- Force null for strange years & less than 1900
        IF(renovated_year IS NULL OR SAFE_CAST(renovated_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(renovated_year AS INT64) < 1900, NULL, SAFE_CAST(renovated_year AS INT64)) as renovated_year, -- Force null for strange years & less than 1900
        ihg_ind
        FROM `wego-cloud.hotel_services.hotels` as h
        LEFT JOIN
        (SELECT * EXCEPT (rn)
        FROM (SELECT base_name, code, id, country_id, RANK() OVER (PARTITION BY code ORDER BY updated_at DESC) as rn
        FROM `wego-cloud.place_services.locations` )
        WHERE rn = 1) as l on l.code=h.city_code
        LEFT JOIN `wego-cloud.place_services.countries` as c on c.id=l.country_id
        LEFT JOIN
        (SELECT id, ANY_VALUE(base_name) as district_name 
        FROM `wego-cloud.place_services.districts` 
        GROUP BY 1) as dt on h.district_id = dt.id
    --            LEFT JOIN `wego-cloud.hotels.hotel_stats` as st on st.id=h.id
    --            LEFT JOIN
    --                (SELECT hotel_id, MAX(score) as score FROM hotel_services.reviews
    --                WHERE reviewer_group='ALL' GROUP BY hotel_id) as trustyou on trustyou.hotel_id=h.id

        LEFT JOIN

        (SELECT hotel_id,
        IF(COUNTIF(provider_id IN (18, 830)) > 0,1,0) as ihg_ind
        FROM `wego-cloud.hotels.provider_hotels`
        GROUP BY 1) as ph

        ON h.id = ph.hotel_id 

        LEFT JOIN

        (SELECT hotel_id, sum(1) as image_count
        FROM `wego-cloud.hotel_services.images`
        GROUP BY hotel_id) as img

        ON img.hotel_id=h.id

        LEFT JOIN

        (SELECT hotel_id, score, reviews_count,
        -- IF(reviews_count IS NULL, AVG(reviews_count) OVER (), SAFE_DIVIDE(reviews_count,(MAX(reviews_count) OVER () - MIN(reviews_count) OVER()))) as reviews_count_weighted -- reviews_count/(max_reviews_count-min_reviews_count), otherwise take average.
        FROM
            (SELECT hotel_id, MAX(score) as score, MAX(count) as reviews_count
            FROM hotel_services.reviews
            WHERE reviewer_group='ALL'
            GROUP BY hotel_id)
        ) as trustyou

        ON trustyou.hotel_id=h.id

        LEFT JOIN

        (SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
        FROM `wego-cloud.hotel_services.brands`) as brand
        ON h.brand_id = brand.brand_id

        LEFT JOIN

        (SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
        FROM `wego-cloud.hotel_services.chains`) as chain
        ON chain.chain_id = brand.chain_id

        LEFT JOIN

        (SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
        FROM `hotel_services.property_types`) as property_type
        on h.property_type_id = property_type.property_type_id

        )
    ) as hs

    CROSS JOIN

    (SELECT site_code, locale
    FROM `wego-cloud.analytics.wego_pos`
    GROUP BY site_code, locale) as pl

) as a

LEFT JOIN

(SELECT 
site_code,
locale,
hotel_id, 
-- MAX(min_displayed_position) as min_displayed_position,
-- MAX(avg_displayed_position) as avg_displayed_position,
SUM(IFNULL(total_detail_pageviews_clicks,0)) as total_itinerary_card_clicks,
FROM

(SELECT site_code, locale, event_id as hotel_id, session_id, SUM(1) as total_detail_pageviews_clicks
FROM `wego-cloud.wego_analytics.pageviews`
WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
AND page_type = "hotels_detail_page_live"
GROUP BY 1,2,3,4) as a

-- LEFT JOIN

-- (SELECT o.id as hotel_id,
-- MIN(o.order) as min_displayed_position,
-- AVG(o.order) as avg_displayed_position,
-- FROM 
-- (SELECT *, MAX(created_at) OVER (PARTITION BY search_id) AS latest_pageload_time
-- FROM `wego-cloud.services_genzo.impressions_logs*`
-- WHERE _TABLE_SUFFIX BETWEEN "%s" AND "%s" -- Adjust the date range as needed
-- AND impression.page = "hotels_search_results"
-- AND impression.trigger = "page_load") il,
-- UNNEST(objects) o
-- WHERE il.created_at = il.latest_pageload_time
-- AND o.itinerary_category = "meta"
-- GROUP BY 1) as c

-- ON a.hotel_id = c.hotel_id

GROUP BY 1,2,3
HAVING hotel_id IS NOT NULL) as c

ON a.site_code = c.site_code AND a.locale = c.locale AND a.hotel_id = c.hotel_id

LEFT JOIN

(SELECT
site_code,
locale,
CAST(hotel_id AS STRING) AS hotel_id,
SUM(conversions_tracked) AS total_conversions, 
COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
SUM(IF(conversions_tracked > 0, wego_total_price_usd, NULL)) as total_gmv,
SUM(revenue_in_usd) as total_revenue
FROM `wego-cloud.wego_analytics.hotels_bookings`
WHERE DATE(created_at) BETWEEN "%s" AND "%s"
GROUP BY 1,2,3) as hotels_bookings_table

ON a.site_code = hotels_bookings_table.site_code AND a.locale = hotels_bookings_table.locale AND a.hotel_id = hotels_bookings_table.hotel_id

LEFT JOIN

(SELECT 
* EXCEPT (total_conversions, total_converted_sessions, total_gmv, total_revenue),
total_conversions as conversions_1mth,
total_converted_sessions as converted_sessions_1mth,
total_gmv as gmv_1mth,
total_revenue as revenue_1mth,
total_detail_pageviews_clicks as detail_pageviews_clicks_1mth,

ROUND(SAFE_DIVIDE(total_conversions, total_detail_pageviews_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, detail_pageviews_clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, detail_pageviews_clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, detail_pageviews_clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, detail_pageviews_clicks_12mth) * 100, 2) AS cvr_12mth,
FROM 
  (SELECT 
  CAST(hotel_id AS STRING) as hotel_id, 
  month,
  total_detail_pageviews_clicks,
  total_conversions,
  total_converted_sessions,
  total_gmv,
  total_revenue,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS detail_pageviews_clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS revenue_2mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS detail_pageviews_clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS revenue_3mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS detail_pageviews_clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS revenue_6mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_detail_pageviews_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS detail_pageviews_clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_revenue) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS revenue_12mth,
  row_number
  FROM
  (SELECT a.hotel_id as hotel_id,
  a.month as month,
  * EXCEPT (hotel_id, month), 
  ROW_NUMBER() OVER (PARTITION BY a.hotel_id ORDER BY a.month DESC) AS row_number
  FROM

    (SELECT id as hotel_id, month 
    FROM `wego-cloud.hotel_services.hotels` 
    CROSS JOIN 
    (SELECT FORMAT_TIMESTAMP('%%Y-%%m', month_list) as month 
    FROM UNNEST(GENERATE_DATE_ARRAY("%s", LAST_DAY(DATE_SUB("%s" , INTERVAL 1 MONTH))  , INTERVAL 1 MONTH)) AS month_list)
    ) as a

    LEFT JOIN

    (SELECT
    hotel_id, 
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    SUM(conversions_tracked) AS total_conversions, 
    COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
    SUM(IF(conversions_tracked > 0, wego_total_price_usd, NULL)) as total_gmv,
    SUM(revenue_in_usd) as total_revenue
    FROM `wego-cloud.wego_analytics.hotels_bookings`
    WHERE DATE(created_at) BETWEEN "%s" AND "%s"
    GROUP BY 1,2
    ) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT CAST(event_id AS INT64) as hotel_id, 
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    SUM(1) as total_detail_pageviews_clicks
    FROM `wego-cloud.wego_analytics.pageviews`
    WHERE DATE(created_at) BETWEEN "%s" AND "%s"
    AND page_type = "hotels_detail_page_live"
    GROUP BY 1,2) as d

    ON a.hotel_id = d.hotel_id AND a.month = d.month

    )
  )
  WHERE row_number = 1
) as lagging_ind

ON lagging_ind.hotel_id=a.hotel_id

LEFT JOIN 

-- Recent BoW Price
-- AVG USD price per hotel from 5 days of Akasha Rates (Dates Limited as it is expensive, 82GB)
(SELECT hotel_id, 
AVG(price.amount_per_night_usd) as avg_price_amount_per_night_usd, 
FROM `wego-cloud.services_akasha.rates*` 
WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB("%s", INTERVAL 4 DAY)) AND FORMAT_DATE('%%Y%%m%%d',"%s")
AND provider.code = "hotels.wego.com"
GROUP BY 1
) as akasha_price

ON akasha_price.hotel_id=a.hotel_id

LEFT JOIN 

-- Usual Price
(SELECT
hotel_id,
AVG(price) AS usual_price
FROM `wego-cloud.hotels.usual_price`
WHERE month IN (
SELECT EXTRACT(MONTH FROM DATE_SUB(DATE("%s"), INTERVAL i MONTH))
FROM UNNEST(GENERATE_ARRAY(0, 2)) AS i
)
GROUP BY hotel_id) as up

ON up.hotel_id=a.hotel_id


)
"""
, processing_date_suffix, start_date_string, end_date_string, start_date_suffix, end_date_suffix, start_date_string, end_date_string,
secondary_start_date_string, secondary_end_date_string, secondary_start_date_string, secondary_end_date_string, secondary_start_date_string, secondary_end_date_string,
end_date_string, end_date_string, end_date_string
);


-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT ROUND(SAFE_DIVIDE(cases_not_null,cases_null),10) as null_to_not_null_deci_pct 
FROM
  (SELECT COUNTIF(avg_price_amount_per_night_usd IS NULL) AS cases_null,
  COUNTIF(avg_price_amount_per_night_usd IS NOT NULL) AS cases_not_null,
  SUM(1) AS total_cases
  FROM `wego-cloud.hotel_sort_order_ml.v1_bow_training_data_%s`
  -- WHERE avg_displayed_position <= (
  --   SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
  --   FROM `wego-cloud.hotel_sort_order_ml.v1_bow_training_data_%s`
  -- )
  -- OR avg_displayed_position IS NULL
  ) 
"""
, processing_date_suffix, processing_date_suffix)
INTO null_proportion
;

----- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.hotel_sort_order_ml.v1_bow_training_data_%s` 
WHERE avg_price_amount_per_night_usd IS NOT NULL
LIMIT 10000000
)
"""
, processing_date_suffix, processing_date_suffix);


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.v1_bow_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['avg_price_amount_per_night_usd']) AS
SELECT
site_code,
locale,
-- hotel_id,
hotel_star_rating,
chain_id,
brand_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
detail_pageviews_clicks_3mth, 
conversions_3mth,
detail_pageviews_clicks_1mth, 
conversions_1mth,
cvr_3mth,
cvr_1mth,
revenue_1mth,
revenue_3mth,
gmv_1mth,
gmv_3mth,
IFNULL(avg_price_amount_per_night_usd,0) as avg_price_amount_per_night_usd
FROM `hotel_sort_order_ml.v1_bow_training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.v1_bow_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.v1_bow_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.v1_bow_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

-- --- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.v1_bow_rf_model_%s`,
-- (SELECT 
-- -- -- Additional IDs 
-- hotel_id,
-- hotel_name,
-- country,
-- country_code,
-- location,
-- location_id, 
-- location_code,
-- total_itinerary_card_clicks,
-- -- -- Main Features 
-- site_code,
-- locale,
-- -- hotel_id,
-- hotel_star_rating,
-- chain_id,
-- brand_id,
-- image_count,
-- overall_score,
-- reviews_count,
-- property_type_id,
-- distance_to_city_centre,
-- detail_pageviews_clicks_3mth, 
-- conversions_3mth,
-- detail_pageviews_clicks_1mth, 
-- conversions_1mth,
-- cvr_3mth,
-- cvr_1mth,
-- revenue_1mth,
-- revenue_3mth,
-- gmv_1mth,
-- gmv_3mth,
-- avg_price_amount_per_night_usd, 
-- FROM `wego-cloud.hotel_sort_order_ml.v1_bow_training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- -- 8. Productionise it in Vertex AI
-- -- EXECUTE IMMEDIATE FORMAT("""
-- -- ALTER MODEL hotel_sort_order_ml.v1_bow_rf_model_%s 
-- -- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_bow_rf_model')
-- -- """
-- -- , start_date_suffix);

-- -- -- -- DROP MODEL IF EXISTS hotel_sort_order_ml.v1_bow_rf_model20240213

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.v1_bow_rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT * , CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.v1_bow_rf_model_%s`,
(SELECT
-- -- Additional IDs 
hotel_id,
hotel_name,
country,
country_code,
location,
location_id, 
location_code,
chain_name,
brand_name, 
property_type_name,
ihg_ind,
total_itinerary_card_clicks,
avg_price_amount_per_night_usd, 
-- -- Main Features 
site_code,
locale,
-- hotel_id,
hotel_star_rating,
chain_id,
brand_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
detail_pageviews_clicks_3mth, 
conversions_3mth,
detail_pageviews_clicks_1mth, 
conversions_1mth,
cvr_3mth,
cvr_1mth,
revenue_1mth,
revenue_3mth,
gmv_1mth,
gmv_3mth,
FROM `hotel_sort_order_ml.v1_bow_training_data_%s` 
-- -- WHERE total_itinerary_card_clicks IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_wa_ml_v1 AS (
SELECT * FROM `hotel_sort_order_ml.v1_bow_rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_pivot_wa_ml_v1 AS (
SELECT
hotel_id,
location_id,
location_code,
sum(if(site_code='AE' and locale='en', formula, 0)) as AE_en,
sum(if(site_code='AE' and locale='ar', formula, 0)) as AE_ar,
sum(if(site_code='AR' and locale='en', formula, 0)) as AR_en,
sum(if(site_code='AR' and locale='es-419', formula, 0)) as AR_es_419,
sum(if(site_code='AU' and locale='en', formula, 0)) as AU_en,
sum(if(site_code='BD' and locale='en', formula, 0)) as BD_en,
sum(if(site_code='BH' and locale='ar', formula, 0)) as BH_ar,
sum(if(site_code='BH' and locale='en', formula, 0)) as BH_en,
sum(if(site_code='BR' and locale='en', formula, 0)) as BR_en,
sum(if(site_code='BR' and locale='pt-br', formula, 0)) as BR_pt_br,
sum(if(site_code='CA' and locale='en', formula, 0)) as CA_en,
sum(if(site_code='CA' and locale='fr', formula, 0)) as CA_fr,
sum(if(site_code='CH' and locale='de', formula, 0)) as CH_de,
sum(if(site_code='CH' and locale='en', formula, 0)) as CH_en,
sum(if(site_code='CH' and locale='fr', formula, 0)) as CH_fr,
sum(if(site_code='CH' and locale='it', formula, 0)) as CH_it,
sum(if(site_code='CL' and locale='en', formula, 0)) as CL_en,
sum(if(site_code='CL' and locale='es-419', formula, 0)) as CL_es_419,
sum(if(site_code='CN' and locale='en', formula, 0)) as CN_en,
sum(if(site_code='CN' and locale='zh-cn', formula, 0)) as CN_zh_cn,
sum(if(site_code='CO' and locale='en', formula, 0)) as CO_en,
sum(if(site_code='CO' and locale='es-419', formula, 0)) as CO_es_419,
sum(if(site_code='DE' and locale='en', formula, 0)) as DE_en,
sum(if(site_code='DE' and locale='de', formula, 0)) as DE_de,
sum(if(site_code='DZ' and locale='ar', formula, 0)) as DZ_ar,
sum(if(site_code='DZ' and locale='fr', formula, 0)) as DZ_fr,
sum(if(site_code='DZ' and locale='en', formula, 0)) as DZ_en,
sum(if(site_code='EG' and locale='ar', formula, 0)) as EG_ar,
sum(if(site_code='EG' and locale='en', formula, 0)) as EG_en,
sum(if(site_code='ES' and locale='en', formula, 0)) as ES_en,
sum(if(site_code='ES' and locale='es', formula, 0)) as ES_es,
sum(if(site_code='FR' and locale='en', formula, 0)) as FR_en,
sum(if(site_code='FR' and locale='fr', formula, 0)) as FR_fr,
sum(if(site_code='GB' and locale='en', formula, 0)) as GB_en,
sum(if(site_code='GH' and locale='en', formula, 0)) as GH_en,
sum(if(site_code='HK' and locale='en', formula, 0)) as HK_en,
sum(if(site_code='HK' and locale='zh-hk', formula, 0)) as HK_zh_hk,
sum(if(site_code='ID' and locale='en', formula, 0)) as ID_en,
sum(if(site_code='ID' and locale='id', formula, 0)) as ID_id,
sum(if(site_code='IE' and locale='en', formula, 0)) as IE_en,
sum(if(site_code='IN' and locale='en', formula, 0)) as IN_en,
sum(if(site_code='IR' and locale='en', formula, 0)) as IR_en,
sum(if(site_code='IR' and locale='fa', formula, 0)) as IR_fa,
sum(if(site_code='IT' and locale='en', formula, 0)) as IT_en,
sum(if(site_code='IT' and locale='it', formula, 0)) as IT_it,
sum(if(site_code='JO' and locale='ar', formula, 0)) as JO_ar,
sum(if(site_code='JO' and locale='en', formula, 0)) as JO_en,
sum(if(site_code='JP' and locale='en', formula, 0)) as JP_en,
sum(if(site_code='JP' and locale='ja', formula, 0)) as JP_ja,
sum(if(site_code='KR' and locale='en', formula, 0)) as KR_en,
sum(if(site_code='KR' and locale='ko', formula, 0)) as KR_ko,
sum(if(site_code='KW' and locale='ar', formula, 0)) as KW_ar,
sum(if(site_code='KW' and locale='en', formula, 0)) as KW_en,
sum(if(site_code='LK' and locale='en', formula, 0)) as LK_en,
sum(if(site_code='MA' and locale='ar', formula, 0)) as MA_ar,
sum(if(site_code='MA' and locale='fr', formula, 0)) as MA_fr,
sum(if(site_code='MA' and locale='en', formula, 0)) as MA_en,
sum(if(site_code='MX' and locale='en', formula, 0)) as MX_en,
sum(if(site_code='MX' and locale='es-419', formula, 0)) as MX_es_419,
sum(if(site_code='MY' and locale='en', formula, 0)) as MY_en,
sum(if(site_code='MY' and locale='ms', formula, 0)) as MY_ms,
sum(if(site_code='MY' and locale='zh-cn', formula, 0)) as MY_zh_cn,
sum(if(site_code='NG' and locale='en', formula, 0)) as NG_en,
sum(if(site_code='NL' and locale='en', formula, 0)) as NL_en,
sum(if(site_code='NL' and locale='nl', formula, 0)) as NL_nl,
sum(if(site_code='NZ' and locale='en', formula, 0)) as NZ_en,
sum(if(site_code='OM' and locale='ar', formula, 0)) as OM_ar,
sum(if(site_code='OM' and locale='en', formula, 0)) as OM_en,
sum(if(site_code='PH' and locale='en', formula, 0)) as PH_en,
sum(if(site_code='PK' and locale='en', formula, 0)) as PK_en,
sum(if(site_code='PL' and locale='en', formula, 0)) as PL_en,
sum(if(site_code='PL' and locale='pl', formula, 0)) as PL_pl,
sum(if(site_code='PT' and locale='en', formula, 0)) as PT_en,
sum(if(site_code='PT' and locale='pt', formula, 0)) as PT_pt,
sum(if(site_code='QA' and locale='ar', formula, 0)) as QA_ar,
sum(if(site_code='QA' and locale='en', formula, 0)) as QA_en,
sum(if(site_code='RU' and locale='en', formula, 0)) as RU_en,
sum(if(site_code='RU' and locale='ru', formula, 0)) as RU_ru,
sum(if(site_code='SA' and locale='ar', formula, 0)) as SA_ar,
sum(if(site_code='SA' and locale='en', formula, 0)) as SA_en,
sum(if(site_code='SE' and locale='en', formula, 0)) as SE_en,
sum(if(site_code='SE' and locale='sv', formula, 0)) as SE_sv,
sum(if(site_code='SG' and locale='en', formula, 0)) as SG_en,
sum(if(site_code='SG' and locale='ms', formula, 0)) as SG_ms,
sum(if(site_code='SG' and locale='zh-cn', formula, 0)) as SG_zh_cn,
sum(if(site_code='TH' and locale='en', formula, 0)) as TH_en,
sum(if(site_code='TH' and locale='th', formula, 0)) as TH_th,
sum(if(site_code='TN' and locale='ar', formula, 0)) as TN_ar,
sum(if(site_code='TN' and locale='fr', formula, 0)) as TN_fr,
sum(if(site_code='TN' and locale='en', formula, 0)) as TN_en,
sum(if(site_code='TR' and locale='en', formula, 0)) as TR_en,
sum(if(site_code='TR' and locale='tr', formula, 0)) as TR_tr,
sum(if(site_code='TW' and locale='en', formula, 0)) as TW_en,
sum(if(site_code='TW' and locale='zh-tw', formula, 0)) as TW_zh_tw,
sum(if(site_code='US' and locale='en', formula, 0)) as US_en,
sum(if(site_code='VN' and locale='en', formula, 0)) as VN_en,
sum(if(site_code='VN' and locale='vi', formula, 0)) as VN_vi,
sum(if(site_code='ZA' and locale='en', formula, 0)) as ZA_en
FROM
  (SELECT *,
  SAFE_DIVIDE(predicted_avg_price_amount_per_night_usd - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id),  
  MAX(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
  as formula
  FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml_v1`
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_prioritisation Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_pivot_wa_ml_prioritisation_v1 AS (
SELECT
hotel_id,
location_id,
location_code,
sum(if(site_code='AE' and locale='en', formula, 0)) as AE_en,
sum(if(site_code='AE' and locale='ar', formula, 0)) as AE_ar,
sum(if(site_code='AR' and locale='en', formula, 0)) as AR_en,
sum(if(site_code='AR' and locale='es-419', formula, 0)) as AR_es_419,
sum(if(site_code='AU' and locale='en', formula, 0)) as AU_en,
sum(if(site_code='BD' and locale='en', formula, 0)) as BD_en,
sum(if(site_code='BH' and locale='ar', formula, 0)) as BH_ar,
sum(if(site_code='BH' and locale='en', formula, 0)) as BH_en,
sum(if(site_code='BR' and locale='en', formula, 0)) as BR_en,
sum(if(site_code='BR' and locale='pt-br', formula, 0)) as BR_pt_br,
sum(if(site_code='CA' and locale='en', formula, 0)) as CA_en,
sum(if(site_code='CA' and locale='fr', formula, 0)) as CA_fr,
sum(if(site_code='CH' and locale='de', formula, 0)) as CH_de,
sum(if(site_code='CH' and locale='en', formula, 0)) as CH_en,
sum(if(site_code='CH' and locale='fr', formula, 0)) as CH_fr,
sum(if(site_code='CH' and locale='it', formula, 0)) as CH_it,
sum(if(site_code='CL' and locale='en', formula, 0)) as CL_en,
sum(if(site_code='CL' and locale='es-419', formula, 0)) as CL_es_419,
sum(if(site_code='CN' and locale='en', formula, 0)) as CN_en,
sum(if(site_code='CN' and locale='zh-cn', formula, 0)) as CN_zh_cn,
sum(if(site_code='CO' and locale='en', formula, 0)) as CO_en,
sum(if(site_code='CO' and locale='es-419', formula, 0)) as CO_es_419,
sum(if(site_code='DE' and locale='en', formula, 0)) as DE_en,
sum(if(site_code='DE' and locale='de', formula, 0)) as DE_de,
sum(if(site_code='DZ' and locale='ar', formula, 0)) as DZ_ar,
sum(if(site_code='DZ' and locale='fr', formula, 0)) as DZ_fr,
sum(if(site_code='DZ' and locale='en', formula, 0)) as DZ_en,
sum(if(site_code='EG' and locale='ar', formula, 0)) as EG_ar,
sum(if(site_code='EG' and locale='en', formula, 0)) as EG_en,
sum(if(site_code='ES' and locale='en', formula, 0)) as ES_en,
sum(if(site_code='ES' and locale='es', formula, 0)) as ES_es,
sum(if(site_code='FR' and locale='en', formula, 0)) as FR_en,
sum(if(site_code='FR' and locale='fr', formula, 0)) as FR_fr,
sum(if(site_code='GB' and locale='en', formula, 0)) as GB_en,
sum(if(site_code='GH' and locale='en', formula, 0)) as GH_en,
sum(if(site_code='HK' and locale='en', formula, 0)) as HK_en,
sum(if(site_code='HK' and locale='zh-hk', formula, 0)) as HK_zh_hk,
sum(if(site_code='ID' and locale='en', formula, 0)) as ID_en,
sum(if(site_code='ID' and locale='id', formula, 0)) as ID_id,
sum(if(site_code='IE' and locale='en', formula, 0)) as IE_en,
sum(if(site_code='IN' and locale='en', formula, 0)) as IN_en,
sum(if(site_code='IR' and locale='en', formula, 0)) as IR_en,
sum(if(site_code='IR' and locale='fa', formula, 0)) as IR_fa,
sum(if(site_code='IT' and locale='en', formula, 0)) as IT_en,
sum(if(site_code='IT' and locale='it', formula, 0)) as IT_it,
sum(if(site_code='JO' and locale='ar', formula, 0)) as JO_ar,
sum(if(site_code='JO' and locale='en', formula, 0)) as JO_en,
sum(if(site_code='JP' and locale='en', formula, 0)) as JP_en,
sum(if(site_code='JP' and locale='ja', formula, 0)) as JP_ja,
sum(if(site_code='KR' and locale='en', formula, 0)) as KR_en,
sum(if(site_code='KR' and locale='ko', formula, 0)) as KR_ko,
sum(if(site_code='KW' and locale='ar', formula, 0)) as KW_ar,
sum(if(site_code='KW' and locale='en', formula, 0)) as KW_en,
sum(if(site_code='LK' and locale='en', formula, 0)) as LK_en,
sum(if(site_code='MA' and locale='ar', formula, 0)) as MA_ar,
sum(if(site_code='MA' and locale='fr', formula, 0)) as MA_fr,
sum(if(site_code='MA' and locale='en', formula, 0)) as MA_en,
sum(if(site_code='MX' and locale='en', formula, 0)) as MX_en,
sum(if(site_code='MX' and locale='es-419', formula, 0)) as MX_es_419,
sum(if(site_code='MY' and locale='en', formula, 0)) as MY_en,
sum(if(site_code='MY' and locale='ms', formula, 0)) as MY_ms,
sum(if(site_code='MY' and locale='zh-cn', formula, 0)) as MY_zh_cn,
sum(if(site_code='NG' and locale='en', formula, 0)) as NG_en,
sum(if(site_code='NL' and locale='en', formula, 0)) as NL_en,
sum(if(site_code='NL' and locale='nl', formula, 0)) as NL_nl,
sum(if(site_code='NZ' and locale='en', formula, 0)) as NZ_en,
sum(if(site_code='OM' and locale='ar', formula, 0)) as OM_ar,
sum(if(site_code='OM' and locale='en', formula, 0)) as OM_en,
sum(if(site_code='PH' and locale='en', formula, 0)) as PH_en,
sum(if(site_code='PK' and locale='en', formula, 0)) as PK_en,
sum(if(site_code='PL' and locale='en', formula, 0)) as PL_en,
sum(if(site_code='PL' and locale='pl', formula, 0)) as PL_pl,
sum(if(site_code='PT' and locale='en', formula, 0)) as PT_en,
sum(if(site_code='PT' and locale='pt', formula, 0)) as PT_pt,
sum(if(site_code='QA' and locale='ar', formula, 0)) as QA_ar,
sum(if(site_code='QA' and locale='en', formula, 0)) as QA_en,
sum(if(site_code='RU' and locale='en', formula, 0)) as RU_en,
sum(if(site_code='RU' and locale='ru', formula, 0)) as RU_ru,
sum(if(site_code='SA' and locale='ar', formula, 0)) as SA_ar,
sum(if(site_code='SA' and locale='en', formula, 0)) as SA_en,
sum(if(site_code='SE' and locale='en', formula, 0)) as SE_en,
sum(if(site_code='SE' and locale='sv', formula, 0)) as SE_sv,
sum(if(site_code='SG' and locale='en', formula, 0)) as SG_en,
sum(if(site_code='SG' and locale='ms', formula, 0)) as SG_ms,
sum(if(site_code='SG' and locale='zh-cn', formula, 0)) as SG_zh_cn,
sum(if(site_code='TH' and locale='en', formula, 0)) as TH_en,
sum(if(site_code='TH' and locale='th', formula, 0)) as TH_th,
sum(if(site_code='TN' and locale='ar', formula, 0)) as TN_ar,
sum(if(site_code='TN' and locale='fr', formula, 0)) as TN_fr,
sum(if(site_code='TN' and locale='en', formula, 0)) as TN_en,
sum(if(site_code='TR' and locale='en', formula, 0)) as TR_en,
sum(if(site_code='TR' and locale='tr', formula, 0)) as TR_tr,
sum(if(site_code='TW' and locale='en', formula, 0)) as TW_en,
sum(if(site_code='TW' and locale='zh-tw', formula, 0)) as TW_zh_tw,
sum(if(site_code='US' and locale='en', formula, 0)) as US_en,
sum(if(site_code='VN' and locale='en', formula, 0)) as VN_en,
sum(if(site_code='VN' and locale='vi', formula, 0)) as VN_vi,
sum(if(site_code='ZA' and locale='en', formula, 0)) as ZA_en
FROM
  (SELECT *
  ----- Manual override of scores based on hotels and manual override score listed in LEFT JOIN b -----
  -- a.* EXCEPT (formula), COALESCE(manual_override_score, formula) AS formula
  FROM
    (SELECT * EXCEPT (formula, x_position_formula), 
    -- IF(ihg_ind > 0, PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id) + (100000000000-PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id))*RAND(), formula)*100 + RAND()*100 as formula
    ----- Boosting transformation to push ihg properties to the similar score to 6th position but with strength of 0.85 -----
    IF(ihg_ind > 0, IF(formula < x_position_formula, (SAFE_DIVIDE(formula, 100000000000) + (SAFE_DIVIDE(x_position_formula,100000000000) - SAFE_DIVIDE(formula, 100000000000))*0.85)*100000000000, formula), formula) as formula
    FROM 
      (SELECT * EXCEPT (ranking), MAX(CASE WHEN ranking = 6 THEN formula END) OVER (PARTITION BY site_code, locale, location_id) as x_position_formula 
      FROM
        (SELECT *, ROW_NUMBER() OVER (PARTITION BY site_code, locale, location_id ORDER BY formula DESC) ranking
        FROM
          (----- PARTITION OVER BY site_code, locale, location_id means range of 0 to 100000000000 for each site_code, locale, location_id combination -----
          SELECT *,
          SAFE_DIVIDE(predicted_avg_price_amount_per_night_usd - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id),  
          MAX(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
          as formula
          FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml_v1`
          )
        )
      )
    ) as a

    -- LEFT JOIN
  
    -- (SELECT *
    -- FROM UNNEST([
    -- STRUCT("2146679" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85329" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85616" AS hotel_id, 150000000000 AS manual_override_score)
    -- ])) as b

    -- ON a.hotel_id = b.hotel_id
  )

GROUP BY hotel_id, location_id, location_code
);



-------- Blended Clicks & Avg Nightly Price --------

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_prioritisation Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.bow_hotel_rank_pivot_wa_ml_prioritisation_v2 AS (
SELECT
hotel_id,
location_id,
location_code,
sum(if(site_code='AE' and locale='en', formula, 0)) as AE_en,
sum(if(site_code='AE' and locale='ar', formula, 0)) as AE_ar,
sum(if(site_code='AR' and locale='en', formula, 0)) as AR_en,
sum(if(site_code='AR' and locale='es-419', formula, 0)) as AR_es_419,
sum(if(site_code='AU' and locale='en', formula, 0)) as AU_en,
sum(if(site_code='BD' and locale='en', formula, 0)) as BD_en,
sum(if(site_code='BH' and locale='ar', formula, 0)) as BH_ar,
sum(if(site_code='BH' and locale='en', formula, 0)) as BH_en,
sum(if(site_code='BR' and locale='en', formula, 0)) as BR_en,
sum(if(site_code='BR' and locale='pt-br', formula, 0)) as BR_pt_br,
sum(if(site_code='CA' and locale='en', formula, 0)) as CA_en,
sum(if(site_code='CA' and locale='fr', formula, 0)) as CA_fr,
sum(if(site_code='CH' and locale='de', formula, 0)) as CH_de,
sum(if(site_code='CH' and locale='en', formula, 0)) as CH_en,
sum(if(site_code='CH' and locale='fr', formula, 0)) as CH_fr,
sum(if(site_code='CH' and locale='it', formula, 0)) as CH_it,
sum(if(site_code='CL' and locale='en', formula, 0)) as CL_en,
sum(if(site_code='CL' and locale='es-419', formula, 0)) as CL_es_419,
sum(if(site_code='CN' and locale='en', formula, 0)) as CN_en,
sum(if(site_code='CN' and locale='zh-cn', formula, 0)) as CN_zh_cn,
sum(if(site_code='CO' and locale='en', formula, 0)) as CO_en,
sum(if(site_code='CO' and locale='es-419', formula, 0)) as CO_es_419,
sum(if(site_code='DE' and locale='en', formula, 0)) as DE_en,
sum(if(site_code='DE' and locale='de', formula, 0)) as DE_de,
sum(if(site_code='DZ' and locale='ar', formula, 0)) as DZ_ar,
sum(if(site_code='DZ' and locale='fr', formula, 0)) as DZ_fr,
sum(if(site_code='DZ' and locale='en', formula, 0)) as DZ_en,
sum(if(site_code='EG' and locale='ar', formula, 0)) as EG_ar,
sum(if(site_code='EG' and locale='en', formula, 0)) as EG_en,
sum(if(site_code='ES' and locale='en', formula, 0)) as ES_en,
sum(if(site_code='ES' and locale='es', formula, 0)) as ES_es,
sum(if(site_code='FR' and locale='en', formula, 0)) as FR_en,
sum(if(site_code='FR' and locale='fr', formula, 0)) as FR_fr,
sum(if(site_code='GB' and locale='en', formula, 0)) as GB_en,
sum(if(site_code='GH' and locale='en', formula, 0)) as GH_en,
sum(if(site_code='HK' and locale='en', formula, 0)) as HK_en,
sum(if(site_code='HK' and locale='zh-hk', formula, 0)) as HK_zh_hk,
sum(if(site_code='ID' and locale='en', formula, 0)) as ID_en,
sum(if(site_code='ID' and locale='id', formula, 0)) as ID_id,
sum(if(site_code='IE' and locale='en', formula, 0)) as IE_en,
sum(if(site_code='IN' and locale='en', formula, 0)) as IN_en,
sum(if(site_code='IR' and locale='en', formula, 0)) as IR_en,
sum(if(site_code='IR' and locale='fa', formula, 0)) as IR_fa,
sum(if(site_code='IT' and locale='en', formula, 0)) as IT_en,
sum(if(site_code='IT' and locale='it', formula, 0)) as IT_it,
sum(if(site_code='JO' and locale='ar', formula, 0)) as JO_ar,
sum(if(site_code='JO' and locale='en', formula, 0)) as JO_en,
sum(if(site_code='JP' and locale='en', formula, 0)) as JP_en,
sum(if(site_code='JP' and locale='ja', formula, 0)) as JP_ja,
sum(if(site_code='KR' and locale='en', formula, 0)) as KR_en,
sum(if(site_code='KR' and locale='ko', formula, 0)) as KR_ko,
sum(if(site_code='KW' and locale='ar', formula, 0)) as KW_ar,
sum(if(site_code='KW' and locale='en', formula, 0)) as KW_en,
sum(if(site_code='LK' and locale='en', formula, 0)) as LK_en,
sum(if(site_code='MA' and locale='ar', formula, 0)) as MA_ar,
sum(if(site_code='MA' and locale='fr', formula, 0)) as MA_fr,
sum(if(site_code='MA' and locale='en', formula, 0)) as MA_en,
sum(if(site_code='MX' and locale='en', formula, 0)) as MX_en,
sum(if(site_code='MX' and locale='es-419', formula, 0)) as MX_es_419,
sum(if(site_code='MY' and locale='en', formula, 0)) as MY_en,
sum(if(site_code='MY' and locale='ms', formula, 0)) as MY_ms,
sum(if(site_code='MY' and locale='zh-cn', formula, 0)) as MY_zh_cn,
sum(if(site_code='NG' and locale='en', formula, 0)) as NG_en,
sum(if(site_code='NL' and locale='en', formula, 0)) as NL_en,
sum(if(site_code='NL' and locale='nl', formula, 0)) as NL_nl,
sum(if(site_code='NZ' and locale='en', formula, 0)) as NZ_en,
sum(if(site_code='OM' and locale='ar', formula, 0)) as OM_ar,
sum(if(site_code='OM' and locale='en', formula, 0)) as OM_en,
sum(if(site_code='PH' and locale='en', formula, 0)) as PH_en,
sum(if(site_code='PK' and locale='en', formula, 0)) as PK_en,
sum(if(site_code='PL' and locale='en', formula, 0)) as PL_en,
sum(if(site_code='PL' and locale='pl', formula, 0)) as PL_pl,
sum(if(site_code='PT' and locale='en', formula, 0)) as PT_en,
sum(if(site_code='PT' and locale='pt', formula, 0)) as PT_pt,
sum(if(site_code='QA' and locale='ar', formula, 0)) as QA_ar,
sum(if(site_code='QA' and locale='en', formula, 0)) as QA_en,
sum(if(site_code='RU' and locale='en', formula, 0)) as RU_en,
sum(if(site_code='RU' and locale='ru', formula, 0)) as RU_ru,
sum(if(site_code='SA' and locale='ar', formula, 0)) as SA_ar,
sum(if(site_code='SA' and locale='en', formula, 0)) as SA_en,
sum(if(site_code='SE' and locale='en', formula, 0)) as SE_en,
sum(if(site_code='SE' and locale='sv', formula, 0)) as SE_sv,
sum(if(site_code='SG' and locale='en', formula, 0)) as SG_en,
sum(if(site_code='SG' and locale='ms', formula, 0)) as SG_ms,
sum(if(site_code='SG' and locale='zh-cn', formula, 0)) as SG_zh_cn,
sum(if(site_code='TH' and locale='en', formula, 0)) as TH_en,
sum(if(site_code='TH' and locale='th', formula, 0)) as TH_th,
sum(if(site_code='TN' and locale='ar', formula, 0)) as TN_ar,
sum(if(site_code='TN' and locale='fr', formula, 0)) as TN_fr,
sum(if(site_code='TN' and locale='en', formula, 0)) as TN_en,
sum(if(site_code='TR' and locale='en', formula, 0)) as TR_en,
sum(if(site_code='TR' and locale='tr', formula, 0)) as TR_tr,
sum(if(site_code='TW' and locale='en', formula, 0)) as TW_en,
sum(if(site_code='TW' and locale='zh-tw', formula, 0)) as TW_zh_tw,
sum(if(site_code='US' and locale='en', formula, 0)) as US_en,
sum(if(site_code='VN' and locale='en', formula, 0)) as VN_en,
sum(if(site_code='VN' and locale='vi', formula, 0)) as VN_vi,
sum(if(site_code='ZA' and locale='en', formula, 0)) as ZA_en
FROM
(
SELECT * EXCEPT (formula, x_position_formula), 
    ----- Boosting transformation to push preferred properties to the similar score to 6th position but with strength of 0.85 -----
    IF(ihg_ind > 0, IF(formula < x_position_formula, (SAFE_DIVIDE(formula, 100000000000) + (SAFE_DIVIDE(x_position_formula,100000000000) - SAFE_DIVIDE(formula, 100000000000))*0.85)*100000000000, formula), formula) as formula
    FROM 
      (SELECT * EXCEPT (ranking), MAX(CASE WHEN ranking = 6 THEN formula END) OVER (PARTITION BY site_code, locale, location_id) as x_position_formula 
      FROM
        (SELECT *, ROW_NUMBER() OVER (PARTITION BY site_code, locale, location_id ORDER BY formula DESC) ranking
        FROM
  (SELECT 
    COALESCE(a.hotel_id, c.hotel_id) hotel_id,
    COALESCE(a.site_code, c.site_code) site_code,
    COALESCE(a.locale, c.locale) locale,
    COALESCE(a.location_id, c.location_id) location_id,
    COALESCE(a.location_code, c.location_code) location_code,
    COALESCE(a.ihg_ind, c.ihg_ind) ihg_ind,
  IFNULL(a.formula,0) * 0.4 + IFNULL(c.formula,0) * 0.6 as formula        
  FROM
    (----- PARTITION OVER BY site_code, locale, location_id means range of 0 to 100000000000 for each site_code, locale, location_id combination -----
          SELECT *,
          SAFE_DIVIDE(predicted_avg_price_amount_per_night_usd - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id),  
          MAX(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_avg_price_amount_per_night_usd) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
          as formula
          FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml_v1`
    ) as a

    FULL OUTER JOIN
    (----- PARTITION OVER BY site_code, locale, location_id means range of 0 to 100000000000 for each site_code, locale, location_id combination -----
          SELECT *,
          SAFE_DIVIDE(predicted_total_itinerary_card_clicks - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id),  
          MAX(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id)) * 100000000000 
          as formula
          FROM `wego-cloud.hotels.bow_hotel_rank_wa_ml`
    ) as c

    ON a.hotel_id = c.hotel_id AND a.site_code = c.site_code AND a.locale = c.locale AND a.location_id = c.location_id


    -- LEFT JOIN
  
    -- (SELECT *
    -- FROM UNNEST([
    -- STRUCT("2146679" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85329" AS hotel_id, 150000000000 AS manual_override_score),
    -- STRUCT("85616" AS hotel_id, 150000000000 AS manual_override_score)
    -- ])) as b

    -- ON a.hotel_id = b.hotel_id
  ))))
  GROUP BY
  hotel_id, location_id, location_code
  
  );






------------------------ 17. DELETE OLDER MODELS ------------------------
---------- Maintain the past 10 models, delete the rest -------------
SET (model_list, model_count) = (
SELECT AS STRUCT ARRAY_AGG(table_id) as model_list, COUNT(*) as model_count
FROM (
  SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY base_table_id ORDER BY creation_time DESC) AS rn FROM
    (SELECT *, 
    REGEXP_REPLACE(table_id, r'_[0-9]{8}$', '') AS base_table_id,
    FROM `wego-cloud.hotel_sort_order_ml.__TABLES__`
    WHERE type = 4
    AND (table_id LIKE 'bow_rf_model_%'
    OR table_id LIKE 'v1_bow_rf_model_%')
    )
  )
  WHERE rn > 10
  ORDER BY table_id, creation_time
  )
);  

LOOP
  IF i >= model_count THEN
    LEAVE;
  END IF;
    SET model_name = model_list[OFFSET(i)];
    --RAISE USING MESSAGE = FORMAT("""Model to drop: %s""", model_name);
    EXECUTE IMMEDIATE FORMAT("""DROP MODEL `wego-cloud.hotel_sort_order_ml.%s`""", model_name);
    SET i = i + 1;
END LOOP;

------------------------ 18. DELETE OLDER TABLES ------------------------
---------- Maintain the past 15 tables, delete the rest -------------
SET (table_list, table_count) = (
SELECT AS STRUCT ARRAY_AGG(table_id) as table_list, COUNT(*) as table_count
FROM (
  SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY base_table_id ORDER BY creation_time DESC) AS rn FROM
    (SELECT *, 
    REGEXP_REPLACE(table_id, r'_[0-9]{8}$', '') AS base_table_id,
    FROM `wego-cloud.hotel_sort_order_ml.__TABLES__`
    WHERE type = 1
    AND (table_id LIKE 'bow_%'
    OR table_id LIKE 'v1_bow_%'
    )
    )
  )
  WHERE rn > 15
  ORDER BY table_id, creation_time
  )
);  

LOOP
  IF j >= table_count THEN
    LEAVE;
  END IF;
    SET table_name = table_list[OFFSET(j)];
    EXECUTE IMMEDIATE FORMAT("""DROP TABLE `wego-cloud.hotel_sort_order_ml.%s`""", table_name);
    SET j = j + 1;
END LOOP;
{% endraw %}
