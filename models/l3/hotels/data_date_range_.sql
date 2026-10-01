{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotel_sort_order_bqml
-- Destination: hotel_sort_order_ml.data_date_range_  (unchanged)
-- Schedule   : every mon 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
## HOTEL SORT ORDER RANDOM FOREST
##
## v2 — see "Hotel Sort Order — Current State and Critical Evaluation"
## https://wegomushi.atlassian.net/wiki/spaces/HIB/pages/4074307589/
##
## Changes vs v1, each tagged [v2:<n>] at the site of the change:
##   [v2:1] served_pos allowlist — train only the 10 POS the Redis importer
##          loads (ScoreServices.java). US_en is trained on GLOBALLY POOLED
##          traffic because it is the runtime fallback for every unserved POS.
##   [v2:2] Price source swapped from the dead hotel_popular_itineraries.fares
##          (last data 1-7 Mar 2026) to wego_analytics.hotels_rates. Price is
##          now POS-specific and normalised per city instead of globally.
##   [v2:3] Downsampling is stratified per POS, so POS identity stops being a
##          proxy for the label.
##   [v2:4] NULL quality features are explicit zeros + missing-indicators
##          instead of being mean-imputed to "average quality" by BQML.
##   [v2:5] chain_id / brand_id / property_type_id are categorical (STRING),
##          not ordinal INT64s. Also stops BQML mean-imputing a NULL chain into
##          a meaningless average chain ID.
##   [v2:6] CVR is smoothed toward the global prior, so a hotel with 2 clicks
##          and 1 booking no longer reads as 50% CVR.
##   [v2:7] Quality floor applied at publish time — unproven inventory is
##          banded below all proven inventory within the same city.
##   [v2:8] Per-city min-max normalisation is degenerate-safe (single-hotel
##          cities no longer produce NULL -> 0 -> bottom of the page).
##   [v2:9] pipeline_health checks + a gate that refuses to publish a bad pivot.
##   [v2:10] Models 2-4 (gmv / add / remove) are gated off by default — none of
##          them reaches Redis. Set train_secondary_models = TRUE to restore.
##   [v2:11] price_missing indicator, so "no rates for this hotel on this POS"
##          is not read as "mid-priced".
##   [v2:12] locations dedup RANK() -> ROW_NUMBER(). RANK() ties could duplicate
##          a hotel row and double its score in the pivot.
##
## Post-first-run calibration (2026-08-03 run, gate held, nothing published):
##   [v2:13] Price thresholds recalibrated and cvr_smoothing_k 50 -> 20. Both
##          original values were set before any measurement; both were wrong in
##          the direction of a gate that fails healthy runs / a feature that
##          ignores its own evidence.
##   [v2:14] rank_quality_spearman + rank_quality_cities health checks. The
##          pipeline previously emitted no ranking metric at all, only absolute
##          -error regression metrics, which on the 2026-08-03 run moved in the
##          OPPOSITE direction to ranking quality and nearly caused a good
##          release to be rejected.
##   [v2:15] price_distinct_per_city dropped. Decomposition showed 99.87% of its
##          value was a restatement of price_coverage_impressed, and the flat-
##          city case it was meant to catch does not occur above 10 priced
##          hotels per city. Replaced by price_flat_city (a near-invariant, no
##          calibration history required) + price_cities_scored to stop that
##          ratio passing vacuously on a collapsed denominator.
##   [v2:16] Price coverage gated on an IMPRESSION-WEIGHTED share instead of a
##          per-row count. Same quantity reads 0.102 by row, 0.5515 by impressed
##          row and 0.8774 weighted by impressions (0.9313 by clicks) -- rate
##          cards exist for hotels users search for, so an unweighted count is
##          dominated by inventory nobody ever sees. Cities with >=10 priced
##          hotels are 13.8% of cities but 97.4% of card clicks.
##
## Measured on the 2026-08-03 run (v2) vs the 2026-06-29 run (v1):
##   rank correlation, 9 non-pooled POS   0.4952 -> 0.5425   (better on 8 of 9)
##   US_en (per-POS -> pooled)            0.3868 -> 0.4659   over 9x the hotels
##   MAE / MSLE                           worse, ~75% of it attributable to the
##                                        pooled arm inflating label magnitude
##
## NOT changed (deliberately) — see the Confluence page:
##   - the ~100-column pivot schema. Untrained POS now emit 0 in their columns,
##     exactly as an empty POS does today. No downstream schema change.
##   - the target metric (detail pageviews + handovers). Changing it needs an
##     experiment, not a patch.

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

--- c. [v2] Configuration
-- [v2:1] The POS the Redis importer actually loads. MUST stay in sync with
-- hotel-management-services ScoreServices.java `posCodes`. Format is
-- {site_code}_{locale}, identical to the Redis key suffix.
-- US_en is special: it is the runtime fallback for every POS not in this list,
-- so it is trained on globally pooled traffic rather than US traffic alone.
DECLARE served_pos ARRAY<STRING> DEFAULT [
  'AE_en', 'AE_ar', 'EG_ar', 'IN_en', 'JO_ar',
  'KW_ar', 'OM_ar', 'SA_ar', 'SA_en', 'US_en'];
DECLARE global_fallback_pos STRING DEFAULT 'US_en';

-- [v2:6] CVR smoothing. cvr = (conversions + k*prior) / (clicks + k).
-- k is "how many clicks of evidence before the hotel's own rate is believed".
--
-- [v2:13] k lowered 50 -> 20 after measuring the 2026-08-03 run. Of 495,780
-- hotels with any clicks in the window, only 14,210 (2.9%) had more than 50,
-- so at k=50 the prior outweighed the hotel's own evidence for 97% of hotels
-- that actually had evidence. The feature was over-damped.
--
-- Not lowered to 10. The defect this smoother exists to fix is a hotel with
-- 1 conversion on 2 clicks reading as a 50-in-100 rate. Against a prior of
-- ~2.4, that case lands at:
--     k=50 -> 4.3    k=20 -> 6.7    k=10 -> 10.3
-- k=10 puts a two-click hotel above four times the prior, which walks most of
-- the way back to the original problem. k=20 keeps single-conversion noise
-- bounded while letting a hotel with 20+ clicks express its own rate.
DECLARE cvr_smoothing_k INT64 DEFAULT 20;
DECLARE prior_cvr FLOAT64;

-- [v2:7] Quality floor. A hotel failing ANY of these is banded into the bottom
-- `quality_floor_band` share of its city's score range, below all hotels that
-- pass. Relative order within each band is preserved.
DECLARE min_reviews_count INT64 DEFAULT 10;
DECLARE min_image_count INT64 DEFAULT 3;
DECLARE quality_floor_band FLOAT64 DEFAULT 0.4;
DECLARE score_scale FLOAT64 DEFAULT 100000000000; -- 1e11, matches akasha

-- [v2:9] Health gate. FALSE = log failures but publish anyway (use for the
-- first run or two while thresholds are calibrated).
DECLARE enforce_health_gate BOOL DEFAULT TRUE;
DECLARE health_failures INT64 DEFAULT 0;

-- [v2:10] gmv / add / remove models. None is read by the Redis importer.
DECLARE train_secondary_models BOOL DEFAULT FALSE;

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

------- -2. [v2:6] Global prior CVR for smoothing ------
-- Computed over the same window the monthly lagging indicators use, so the
-- prior and the per-hotel rates are measured on the same population.
EXECUTE IMMEDIATE FORMAT("""
SELECT ROUND(SAFE_DIVIDE(SUM(conversions_tracked), NULLIF(COUNT(click_id), 0)) * 100, 6)
FROM `wego-cloud.wego_analytics.hotels_clicks`
WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
""", secondary_start_date_string, secondary_end_date_string)
INTO prior_cvr;

SET prior_cvr = IFNULL(prior_cvr, 0);

------- -1. Record Date Ranges ------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.data_date_range_%s AS (
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
CREATE OR REPLACE TABLE hotel_sort_order_ml.training_data_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT a.*, 
impressions_1mth,
impressions_3mth,
impressions_12mth,
clicks_1mth,
clicks_3mth,
clicks_12mth,
conversions_1mth,
conversions_3mth,
conversions_12mth,
ctr_1mth,
ctr_3mth,
ctr_12mth,
cvr_1mth,
cvr_3mth,
cvr_12mth,
-- [v2:6] Empirical-Bayes smoothed CVR. Raw cvr_Nmth has no minimum
-- denominator, so 1 booking on 2 clicks reads as a 50-in-100 rate and outranks
-- a hotel with 400 bookings on 10,000 clicks. Shrinking toward the prior costs
-- nothing on high-volume hotels and neutralises low-volume noise.
-- NOTE: ctr_Nmth has the identical defect against impressions. Left as-is to
-- keep this diff reviewable; same fix applies if it proves to matter.
ROUND(SAFE_DIVIDE(IFNULL(conversions_1mth, 0) + @cvr_smoothing_k * @prior_cvr / 100,
                  IFNULL(clicks_1mth, 0) + @cvr_smoothing_k) * 100, 4) AS cvr_1mth_smoothed,
ROUND(SAFE_DIVIDE(IFNULL(conversions_3mth, 0) + @cvr_smoothing_k * @prior_cvr / 100,
                  IFNULL(clicks_3mth, 0) + @cvr_smoothing_k) * 100, 4) AS cvr_3mth_smoothed,
gmv_1mth,
gmv_3mth,
gmv_12mth,
price_in_usd,
rate_card_appearances,

-- [v2:2] Per-city, per-POS min-max. Previously min/max were computed OVER ()
-- across every hotel on the platform, so one luxury outlier compressed all
-- ordinary inventory into a sliver near zero and the feature carried almost no
-- information. Partitioning by (site_code, locale, location_id) makes it
-- "how expensive is this hotel for this city on this POS", which is the
-- question a ranking model can actually use.
MIN(price_in_usd) OVER city AS min_price_in_usd,
MAX(price_in_usd) OVER city AS max_price_in_usd,
-- 0.5 (neutral midpoint) covers two cases: a city where every hotel resolves
-- to the same price, and a hotel with no rates in the window at all. The
-- companion flag tells the model which rows those are, so "unknown price" is
-- not silently read as "mid-priced".
IFNULL(
  SAFE_DIVIDE(price_in_usd - MIN(price_in_usd) OVER city,
              NULLIF(MAX(price_in_usd) OVER city - MIN(price_in_usd) OVER city, 0)),
  0.5) AS price_in_usd_weighted,
IF(price_in_usd IS NULL, 1, 0) AS price_missing,

usual_price,
100 * (1 - SAFE_DIVIDE(price_in_usd, usual_price)) as discount_percent,
-- min_displayed_position,
-- avg_displayed_position,
total_itinerary_card_clicks,
IF(total_gmv = 0, NULL, total_gmv) as total_gmv,
CURRENT_DATE() as processing_date 

FROM
    (SELECT
    pl.site_code as site_code, pl.locale as locale,
    hs.hotel_id as hotel_id, hotel_name, hs.chain_id as chain_id, chain_name, hs.brand_id as brand_id, brand_name, hs.country as country, hs.country_code as country_code, hs.location as location, hs.location_id as location_id, hs.location_code as location_code, hs.district_id as district_id, hs.district_name as district_name,
    hotel_star_rating, image_count, overall_score, reviews_count, property_type_id, property_type_name, distance_to_city_centre,
    booking_avg_reviews_score,
    booking_reviews_count,
    booking_0_5_reviews_count,
    booking_5_6_reviews_count,  
    booking_6_7_reviews_count,  
    booking_7_8_reviews_count,  
    booking_8_9_reviews_count,  
    booking_9_10_reviews_count, 
    ihg_ind,
    FROM
    (SELECT *,
    FROM
        (SELECT
        CAST(h.id AS STRING) as hotel_id, h.name_en as hotel_name, brand.chain_id as chain_id, chain_name, h.brand_id as brand_id, brand_name, c.base_name as country, c.code as country_code, l.base_name as location, l.code as location_code, l.id as location_id, h.district_id as district_id, dt.district_name as district_name, star as hotel_star_rating,
        img.image_count as image_count, trustyou.score as overall_score, trustyou.reviews_count as reviews_count, distance_to_city_centre, h.property_type_id, property_type_name,
        IF(built_year IS NULL OR SAFE_CAST(built_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(built_year AS INT64) < 1900, NULL, SAFE_CAST(built_year AS INT64)) as built_year, -- Force null for strange years & less than 1900
        IF(renovated_year IS NULL OR SAFE_CAST(renovated_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(renovated_year AS INT64) < 1900, NULL, SAFE_CAST(renovated_year AS INT64)) as renovated_year, -- Force null for strange years & less than 1900
        booking_avg_reviews_score,
        booking_reviews_count,
        booking_0_5_reviews_count,
        booking_5_6_reviews_count,  
        booking_6_7_reviews_count,  
        booking_7_8_reviews_count,  
        booking_8_9_reviews_count,  
        booking_9_10_reviews_count, 
        ihg_ind
        FROM `wego-cloud.hotel_services.hotels` as h
        LEFT JOIN
        (SELECT * EXCEPT (rn)
        -- [v2:12] RANK() -> ROW_NUMBER(). RANK() ties, so two location rows
        -- sharing a `code` with the same updated_at BOTH get rn = 1, the hotel
        -- row duplicates, and the pivot's sum(if(...)) then DOUBLES that
        -- hotel's score. ROW_NUMBER() guarantees exactly one row per code.
        -- (wego_hotels_rates_analysis.sql hits the same hazard and dedups with
        -- ROW_NUMBER() OVER (PARTITION BY hotel_id) for the same reason.)
        FROM (SELECT base_name, code, id, country_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY updated_at DESC, id) as rn
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
        IF(COUNTIF(provider_id = 18) > 0,1,0) as ihg_ind
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

        (SELECT 
        hotel_id,
        AVG(rating) as booking_avg_reviews_score,
        COUNT(*) as booking_reviews_count,
        COUNTIF(rating >= 0 AND rating < 5) as booking_0_5_reviews_count,
        COUNTIF(rating >= 5 AND rating < 6) as booking_5_6_reviews_count,  
        COUNTIF(rating >= 6 AND rating < 7) as booking_6_7_reviews_count,  
        COUNTIF(rating >= 7 AND rating < 8) as booking_7_8_reviews_count,  
        COUNTIF(rating >= 8 AND rating < 9) as booking_8_9_reviews_count,  
        COUNTIF(rating >= 9 AND rating <= 10) as booking_9_10_reviews_count,  
        FROM `wego-cloud.hotel_services.provider_reviews`
        GROUP BY 1) as booking

        ON booking.hotel_id=h.id

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

    -- [v2:1] Was: every (site_code, locale) in `analytics.wego_pos` (~100 POS).
    -- The Redis importer only ever loads 10 of them; the rest were trained,
    -- scored, pivoted and discarded. Worse, because the label join is the ONLY
    -- POS-varying input (every feature below joins on hotel_id alone), the
    -- untrafficked POS contributed ~90 duplicate rows per hotel carrying an
    -- all-NULL label. After downsampling that made "is this a real POS?" the
    -- single most predictive split available to the forest.
    (SELECT
     SPLIT(pos, '_')[OFFSET(0)] AS site_code,
     SUBSTR(pos, STRPOS(pos, '_') + 1) AS locale   -- tolerates locales with '-' e.g. es-419
     FROM UNNEST(@served_pos) AS pos) as pl

) as a

LEFT JOIN

-- [v2:1] Label. Two arms:
--   - the 9 genuinely-served POS get their own traffic;
--   - US_en gets ALL POS pooled, because akasha falls back to the US_en Redis
--     key for every POS not in served_pos (StaticScoreServiceImpl). Training it
--     on US traffic alone would make the de-facto global default a US model.
--     Pooling keeps the fallback meaningful with no downstream change.
(
SELECT
IF(arm = 'pooled', SPLIT(@global_fallback_pos, '_')[OFFSET(0)], site_code) AS site_code,
IF(arm = 'pooled', SUBSTR(@global_fallback_pos, STRPOS(@global_fallback_pos, '_') + 1), locale) AS locale,
hotel_id,
SUM(total_itinerary_card_clicks) AS total_itinerary_card_clicks,
SUM(total_gmv) AS total_gmv
FROM
(SELECT
COALESCE(a.site_code, b.site_code) as site_code,
COALESCE(a.locale, b.locale) as locale,
COALESCE(a.hotel_id, b.hotel_id) as hotel_id,
-- MAX(min_displayed_position) as min_displayed_position,
-- MAX(avg_displayed_position) as avg_displayed_position,
SUM(IFNULL(total_detail_pageviews_clicks,0) + IFNULL(total_handovers,0)) as total_itinerary_card_clicks,
SUM(total_gmv) as total_gmv
FROM

(SELECT site_code, locale, event_id as hotel_id, session_id, SUM(1) as total_detail_pageviews_clicks
FROM `wego-cloud.wego_analytics.pageviews`
WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
AND page_type = "hotels_detail_page_live"
AND page_subtype = "hotels_detail_page_live"
GROUP BY 1,2,3,4) as a

FULL OUTER JOIN 

(SELECT site_code, locale, CAST(hotel_id as STRING) as hotel_id, session_id, SUM(1) as total_handovers,
SUM(booking_value_usd) as total_gmv
FROM `wego-cloud.wego_analytics.hotels_clicks`
WHERE DATE(_PARTITIONTIME) BETWEEN "%s" AND "%s"
-- AND conversions_tracked > 0 
GROUP BY 1,2,3,4) as b

ON a.site_code = b.site_code AND a.locale = b.locale AND a.hotel_id = b.hotel_id AND a.session_id = b.session_id

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
HAVING hotel_id IS NOT NULL) as pos_labels

-- [v2:1] One scan, two arms. 'per_pos' keeps a POS's own traffic; 'pooled'
-- collapses every POS into the fallback key. Cross-joining the arm selector
-- avoids scanning the source twice.
CROSS JOIN UNNEST(['per_pos', 'pooled']) AS arm
WHERE arm = 'pooled'
   OR (CONCAT(site_code, '_', locale) IN UNNEST(@served_pos)
       AND CONCAT(site_code, '_', locale) != @global_fallback_pos)
GROUP BY 1, 2, 3
) as c

ON a.site_code = c.site_code AND a.locale = c.locale AND a.hotel_id = c.hotel_id

LEFT JOIN


(SELECT 
* EXCEPT (total_clicks, total_conversions, total_clicked_sessions, total_converted_sessions, total_gmv, total_impressions),
total_clicks as clicks_1mth,
total_conversions as conversions_1mth,
total_clicked_sessions as clicked_sessions_1mth,
total_converted_sessions as converted_sessions_1mth,
total_gmv as gmv_1mth,
total_impressions as impressions_1mth,
ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS ctr_1mth,
ROUND(SAFE_DIVIDE(clicks_2mth, impressions_2mth) * 100, 2) AS ctr_2mth,
ROUND(SAFE_DIVIDE(clicks_3mth, impressions_3mth) * 100, 2) AS ctr_3mth,
ROUND(SAFE_DIVIDE(clicks_6mth, impressions_6mth) * 100, 2) AS ctr_6mth,
ROUND(SAFE_DIVIDE(clicks_12mth, impressions_12mth) * 100, 2) AS ctr_12mth,

ROUND(SAFE_DIVIDE(total_conversions, total_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, clicks_12mth) * 100, 2) AS cvr_12mth,

ROUND(SAFE_DIVIDE(total_converted_sessions, total_clicked_sessions) * 100, 2) AS converted_sessions_rate_1mth,
ROUND(SAFE_DIVIDE(converted_sessions_2mth, clicked_sessions_2mth) * 100, 2) AS converted_sessions_rate_2mth,
ROUND(SAFE_DIVIDE(converted_sessions_3mth, clicked_sessions_3mth) * 100, 2) AS converted_sessions_rate_3mth,
ROUND(SAFE_DIVIDE(converted_sessions_6mth, clicked_sessions_6mth) * 100, 2) AS converted_sessions_rate_6mth,
ROUND(SAFE_DIVIDE(converted_sessions_12mth, clicked_sessions_12mth) * 100, 2) AS converted_sessions_rate_12mth,
FROM 
  (SELECT 
  CAST(hotel_id AS STRING) as hotel_id, 
  month,
  total_clicks,
  total_conversions,
  total_clicked_sessions,
  total_converted_sessions,
  total_gmv,
  total_impressions,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicked_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS impressions_2mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicked_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS impressions_3mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicked_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS impressions_6mth,
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicked_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS impressions_12mth,
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
    COUNT(click_id) AS total_clicks,
    COUNT(DISTINCT session_id) AS total_clicked_sessions,
    COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
    SUM(booking_value_usd) as total_gmv
    FROM `wego-cloud.wego_analytics.hotels_clicks`
    WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
    GROUP BY 1,2) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT
    CAST(hotel_id AS INT64) as hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', search_created_at) AS month, 
    COUNT(DISTINCT search_id) as total_impressions
    FROM `wego_analytics.hotels_impressions*`
    WHERE _TABLE_SUFFIX BETWEEN "%s" AND "%s"
    GROUP BY 1,2) as c

    ON a.hotel_id = c.hotel_id AND a.month = c.month

    )
  )
  WHERE row_number = 1
) as lagging_ind

ON lagging_ind.hotel_id=a.hotel_id

LEFT JOIN 

-- [v2:2] PRICE. Was `hotel_popular_itineraries.fares` over the 89-day tertiary
-- window. That table's last data is 1-7 Mar 2026: the 29 Jun run still caught
-- it (window opened 1 Mar), the 6 Jul run did not (window opened 8 Mar), the
-- join returned nothing for every hotel, and BQML died trying to impute an
-- all-NULL column. Nothing writes to that table any more.
--
-- Now: wego_analytics.hotels_rates, over the SAME 30-day window as the label,
-- so the price is the one users saw during the period the clicks were counted.
--
-- Two things this buys that fares could not:
--   1. hotels_rates carries site_code + locale, so price becomes POS-specific.
--      With markups varying by site_code x supplier x channel, the same hotel
--      genuinely costs different amounts per POS.
--   2. It is alive.
--
-- COST WARNING: this table is 17.5 TB / 85B rows, partitioned on _PARTITIONTIME
-- (INGESTION time), and partition filter is NOT required. Filtering on
-- created_at -- which is what the fares query did -- prunes NOTHING and scans
-- the entire table every week. The _PARTITIONDATE predicate below is load
-- bearing. Do not "tidy" it into created_at.
--
-- provider_code filter is belt-and-braces: the last ~year is hotels.wego.com
-- only. rate_rank is NOT filtered -- it is a parity-era column and is constant
-- at 1 now, so filtering on it would be a no-op that reads as meaningful.
(SELECT
IF(arm = 'pooled', SPLIT(@global_fallback_pos, '_')[OFFSET(0)], site_code) AS site_code,
IF(arm = 'pooled', SUBSTR(@global_fallback_pos, STRPOS(@global_fallback_pos, '_') + 1), locale) AS locale,
hotel_id,
-- SUM(sum)/SUM(count), not AVG(AVG): pooling averages of averages would weight
-- a 3-search POS the same as a 3-million-search one.
SAFE_DIVIDE(SUM(price_sum), NULLIF(SUM(rate_rows), 0)) AS price_in_usd,
-- Exposure count. Not yet a model feature -- candidate replacement for the
-- impressions denominator the BoW feature set lost. Costs nothing to carry.
SUM(searches) AS rate_card_appearances
FROM
    (SELECT
     hotel_id,
     site_code,
     locale,
     SUM(price_amount_usd) AS price_sum,
     COUNT(*) AS rate_rows,
     COUNT(DISTINCT search_id) AS searches
     FROM `wego-cloud.wego_analytics.hotels_rates`
     WHERE DATE(_PARTITIONDATE) BETWEEN "%s" AND "%s"
     AND provider_code = "hotels.wego.com"
     AND price_amount_usd > 0
     GROUP BY 1, 2, 3) as pos_prices

-- Same two-arm pooling as the label, for the same reason: US_en is the runtime
-- fallback for every unserved POS, so its price must be pooled across all POS
-- to match its pooled label. Mixing a US-only price against a global label
-- would put the two sides of the fallback row on different populations.
CROSS JOIN UNNEST(['per_pos', 'pooled']) AS arm
WHERE arm = 'pooled'
   OR (CONCAT(site_code, '_', locale) IN UNNEST(@served_pos)
       AND CONCAT(site_code, '_', locale) != @global_fallback_pos)
GROUP BY 1, 2, 3
) as rates

ON rates.hotel_id = a.hotel_id
AND rates.site_code = a.site_code
AND rates.locale = a.locale

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

-- [v2:2] Named window for the per-city price normalisation in the SELECT list.
-- MUST be qualified with `a.`: the label subquery `c` and the price subquery
-- `rates` both also expose site_code and locale, so bare references here are
-- ambiguous. location_id comes from the hotel_services.hotels ->
-- place_services.locations join in `hs`, NOT from any location column on the
-- rate or click sources.
WINDOW city AS (PARTITION BY a.site_code, a.locale, a.location_id)

)
"""
, processing_date_suffix, start_date_string, end_date_string, start_date_string, end_date_string, start_date_suffix, end_date_suffix,
secondary_start_date_string, secondary_end_date_string, secondary_start_date_string, secondary_end_date_string, secondary_start_date_suffix, secondary_end_date_suffix,
start_date_string, end_date_string, tertiary_end_date_string
)
USING served_pos AS served_pos,
global_fallback_pos AS global_fallback_pos,
cvr_smoothing_k AS cvr_smoothing_k,
prior_cvr AS prior_cvr;


------------------- MODEL 1: Total Clicks -------------------
----- 1. [v2:3] Find Proportion of Null vs Not Null Cases, PER POS -----
-- Was: one global proportion. Non-null labels concentrate in high-traffic POS
-- while the null sample was drawn flat across all POS, so the retained null
-- half and the non-null half came from different POS populations. That handed
-- the forest a spurious POS -> label correlation before it saw a single hotel
-- attribute. Stratifying keeps each POS internally balanced.
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.null_proportion_by_pos_%s AS (
SELECT
site_code,
locale,
COUNTIF(total_itinerary_card_clicks IS NULL) AS cases_null,
COUNTIF(total_itinerary_card_clicks IS NOT NULL) AS cases_not_null,
ROUND(SAFE_DIVIDE(COUNTIF(total_itinerary_card_clicks IS NOT NULL),
                  NULLIF(COUNTIF(total_itinerary_card_clicks IS NULL), 0)), 10) AS null_proportion
FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`
GROUP BY 1, 2)
"""
, processing_date_suffix, processing_date_suffix);

-- The old global `SELECT ... INTO null_proportion` for this model is gone: it
-- is now dead code. Models 2-4 each compute their own null_proportion inline,
-- so the variable is still declared and still used down there.

----- 2. Downsample to 50:50 Not Null:Null Cases, within each POS -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.training_data_downsample_%s AS (
SELECT t.* FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` t
JOIN `wego-cloud.hotel_sort_order_ml.null_proportion_by_pos_%s` p
  ON t.site_code = p.site_code AND t.locale = p.locale
WHERE t.total_itinerary_card_clicks IS NULL
-- IFNULL(...,1) keeps every null row for a POS that has no labelled rows at
-- all, rather than silently dropping the POS from training.
AND RAND() < IFNULL(p.null_proportion, 1)

UNION ALL

SELECT t.* FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` t
WHERE t.total_itinerary_card_clicks IS NOT NULL
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix);


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_itinerary_card_clicks']) AS
SELECT
site_code,
locale,
-- hotel_id,

-- [v2:4] Quality features: explicit zero + a missing-indicator.
-- BQML imputes a NULL numeric feature with that COLUMN'S MEAN. A guesthouse
-- with no TrustYou score, no reviews, no images and no star rating therefore
-- entered training as an AVERAGE-quality property on all four axes, and was
-- scored accordingly. Absence of evidence was being read as average evidence.
-- Zero + a flag says "unproven", and lets the forest learn separately where
-- unknown genuinely is not bad.
IFNULL(hotel_star_rating, 0) AS hotel_star_rating,
IF(hotel_star_rating IS NULL, 1, 0) AS hotel_star_rating_missing,
IFNULL(image_count, 0) AS image_count,
IF(image_count IS NULL, 1, 0) AS image_count_missing,
IFNULL(overall_score, 0) AS overall_score,
IF(overall_score IS NULL, 1, 0) AS overall_score_missing,
IFNULL(reviews_count, 0) AS reviews_count,
IF(reviews_count IS NULL, 1, 0) AS reviews_count_missing,
IFNULL(booking_avg_reviews_score, 0) AS booking_avg_reviews_score,
IF(booking_avg_reviews_score IS NULL, 1, 0) AS booking_avg_reviews_score_missing,
IFNULL(booking_reviews_count, 0) AS booking_reviews_count,
IFNULL(booking_0_5_reviews_count, 0) AS booking_0_5_reviews_count,
IFNULL(booking_5_6_reviews_count, 0) AS booking_5_6_reviews_count,
IFNULL(booking_6_7_reviews_count, 0) AS booking_6_7_reviews_count,
IFNULL(booking_7_8_reviews_count, 0) AS booking_7_8_reviews_count,
IFNULL(booking_8_9_reviews_count, 0) AS booking_8_9_reviews_count,
IFNULL(booking_9_10_reviews_count, 0) AS booking_9_10_reviews_count,

-- [v2:5] Identifiers are categories, not quantities. As INT64 the forest
-- splits on ranges like "brand_id < 4127" or "property_type_id < 14", which
-- groups unrelated brands and unrelated property types purely by how their IDs
-- happen to have been assigned. Every such split is noise the model then has
-- to spend depth undoing.
--
-- This also fixes a second defect: BQML mean-imputes a NULL INT64, so an
-- independent hotel with no chain was entering training as the arithmetic mean
-- of every chain ID -- a number identifying no chain at all. As STRING, NULL
-- becomes its own category, which is what "independent" actually is.
--
-- Cardinality is a cost question, not a correctness one. If brand_id turns out
-- to be large enough that one-hot encoding hurts, the answer is
-- CATEGORY_ENCODING_METHOD='TARGET_ENCODING' in the OPTIONS block above, not
-- reverting to an ordinal. Check with:
--   SELECT COUNT(DISTINCT brand_id), COUNT(DISTINCT chain_id)
--   FROM `wego-cloud.hotel_services.hotels`;
CAST(chain_id AS STRING) AS chain_id,
CAST(brand_id AS STRING) AS brand_id,
CAST(property_type_id AS STRING) AS property_type_id,

-- Left alone: neither NULL -> 0 (reads as "at the city centre", i.e. best) nor
-- NULL -> mean is right here. Needs a product decision, not a patch.
distance_to_city_centre,

impressions_3mth,
clicks_3mth,
conversions_3mth,
impressions_1mth,
clicks_1mth,
conversions_1mth,
ctr_3mth,
ctr_1mth,
-- [v2:6] smoothed, see training-data build
cvr_3mth_smoothed,
cvr_1mth_smoothed,
-- price_in_usd,
-- min_price_in_usd,
-- max_price_in_usd,
price_in_usd_weighted,
price_missing,
IFNULL(total_itinerary_card_clicks,0) as total_itinerary_card_clicks
FROM `hotel_sort_order_ml.training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.rf_model_%s`,
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
-- impressions_3mth, 
-- clicks_3mth, 
-- conversions_3mth,
-- impressions_1mth, 
-- clicks_1mth, 
-- conversions_1mth,
-- ctr_3mth,
-- ctr_1mth,
-- cvr_3mth,
-- cvr_1mth,
-- -- price_in_usd, 
-- -- min_price_in_usd, 
-- -- max_price_in_usd, 
-- price_in_usd_weighted,
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- 8. Productionise it in Vertex AI
-- EXECUTE IMMEDIATE FORMAT("""
-- ALTER MODEL hotel_sort_order_ml.rf_model_%s 
-- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_rf_model')
-- """
-- , start_date_suffix);

-- -- -- DROP MODEL IF EXISTS hotel_sort_order_ml.rf_model20240213

-- -- ----------------- DUMMY MODEL --------------------
-- -- ----- 9. Train Random Forest Model (Dummy) -----
-- -- CREATE OR REPLACE MODEL `hotel_sort_order_ml.rf_model_dummy2`
-- -- OPTIONS
-- -- ( model_type='RANDOM_FOREST_REGRESSOR',
-- --   ENABLE_GLOBAL_EXPLAIN = TRUE,
-- --   input_label_cols=['formula_output'],
-- --   DATA_SPLIT_METHOD='NO_SPLIT') AS
-- -- SELECT 
-- -- -- site_code, 
-- -- -- locale, 
-- -- -- hotel_id,
-- -- formula,
-- -- formula as formula_output
-- -- FROM `wego-cloud.hotels.hotel_rank_wa` 
-- -- TABLESAMPLE SYSTEM (10 PERCENT)
-- -- ;


-- -- CREATE OR REPLACE MODEL `hotel_sort_order_ml.lr_model_dummy`
-- -- OPTIONS
-- -- ( model_type='linear_reg',
-- --   ENABLE_GLOBAL_EXPLAIN = TRUE,
-- --   input_label_cols=['formula_output'],
-- --   DATA_SPLIT_METHOD='NO_SPLIT') AS
-- -- SELECT 
-- -- -- site_code, 
-- -- -- locale, 
-- -- -- hotel_id,
-- -- formula,
-- -- formula as formula_output
-- -- FROM `wego-cloud.hotels.hotel_rank_wa` 
-- -- TABLESAMPLE SYSTEM (10 PERCENT)
-- -- ;


-- -- ----- 10. Model Evaluation Metrics (Dummy) -----
-- -- CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_dummy_evaluation AS (
-- -- SELECT * FROM
-- -- ML.EVALUATE(MODEL`hotel_sort_order_ml.rf_model_dummy2`)
-- -- );

-- -- ----- 11. Model Feature Importance (Dummy) -----
-- -- CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_dummy_feature_importance AS (
-- -- SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.rf_model_dummy2`)
-- -- );

-- -- ----- 12. Model Global Explain (Dummy) -----
-- -- CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_dummy_global_explain AS (
-- -- SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.rf_model_dummy2`)
-- -- );

-- -- 13. Productionise it in Vertex AI (Dummy) -----
-- -- ALTER MODEL hotel_sort_order_ml.lr_model_dummy
-- -- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_rf_dummy');

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT * , CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.rf_model_%s`,
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
price_in_usd,
total_itinerary_card_clicks,
-- -- Main Features
-- [v2:4/5/6] This list MUST stay byte-identical in transform to the CREATE
-- MODEL list above. A silent train/predict mismatch is the single easiest way
-- to break this pipeline without any error being raised.
site_code,
locale,
IFNULL(hotel_star_rating, 0) AS hotel_star_rating,
IF(hotel_star_rating IS NULL, 1, 0) AS hotel_star_rating_missing,
-- hotel_id,
IFNULL(image_count, 0) AS image_count,
IF(image_count IS NULL, 1, 0) AS image_count_missing,
IFNULL(overall_score, 0) AS overall_score,
IF(overall_score IS NULL, 1, 0) AS overall_score_missing,
IFNULL(reviews_count, 0) AS reviews_count,
IF(reviews_count IS NULL, 1, 0) AS reviews_count_missing,
IFNULL(booking_avg_reviews_score, 0) AS booking_avg_reviews_score,
IF(booking_avg_reviews_score IS NULL, 1, 0) AS booking_avg_reviews_score_missing,
IFNULL(booking_reviews_count, 0) AS booking_reviews_count,
IFNULL(booking_0_5_reviews_count, 0) AS booking_0_5_reviews_count,
IFNULL(booking_5_6_reviews_count, 0) AS booking_5_6_reviews_count,
IFNULL(booking_6_7_reviews_count, 0) AS booking_6_7_reviews_count,
IFNULL(booking_7_8_reviews_count, 0) AS booking_7_8_reviews_count,
IFNULL(booking_8_9_reviews_count, 0) AS booking_8_9_reviews_count,
IFNULL(booking_9_10_reviews_count, 0) AS booking_9_10_reviews_count,
CAST(chain_id AS STRING) AS chain_id,
CAST(brand_id AS STRING) AS brand_id,
CAST(property_type_id AS STRING) AS property_type_id,
distance_to_city_centre,
impressions_3mth,
clicks_3mth,
conversions_3mth,
impressions_1mth,
clicks_1mth,
conversions_1mth,
ctr_3mth,
ctr_1mth,
cvr_3mth_smoothed,
cvr_1mth_smoothed,
-- price_in_usd,
-- min_price_in_usd,
-- max_price_in_usd,
price_in_usd_weighted,
price_missing,
FROM `hotel_sort_order_ml.training_data_%s`
-- -- WHERE total_itinerary_card_clicks IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_wa_ml AS (
SELECT * FROM `hotel_sort_order_ml.rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 15b. [v2:9] Health checks, run BEFORE the pivot is published -----
-- Neither pipeline previously asserted anything about its own output. A model
-- can train and publish a table of NULLs, or lose 90% of its POS, with no
-- error anywhere. These checks are the promotion gate.
CREATE TABLE IF NOT EXISTS hotel_sort_order_ml.pipeline_health (
  run_date DATE,
  pipeline STRING,
  check_name STRING,
  value FLOAT64,
  threshold FLOAT64,
  passed BOOL,
  detail STRING
);

DELETE FROM hotel_sort_order_ml.pipeline_health
WHERE run_date = processing_date AND pipeline = 'hotel_sort_order_bqml';

INSERT INTO hotel_sort_order_ml.pipeline_health
WITH preds AS (
  SELECT *,
    SAFE_DIVIDE(predicted_total_itinerary_card_clicks
                  - MIN(predicted_total_itinerary_card_clicks) OVER w,
                NULLIF(MAX(predicted_total_itinerary_card_clicks) OVER w
                  - MIN(predicted_total_itinerary_card_clicks) OVER w, 0)) AS norm
  FROM `wego-cloud.hotels.hotel_rank_wa_ml`
  WINDOW w AS (PARTITION BY site_code, locale, location_id)
),
-- [v2:14] Per-city rank correlation between what the model predicted and what
-- actually happened. total_itinerary_card_clicks survives ML.PREDICT as a
-- passthrough input column, so no join back to training_data is needed.
--
-- This is the only check that measures the thing the pipeline is FOR. Every
-- other metric the pipeline emits (MAE, MSE, MSLE, r2) is an absolute-error
-- metric on a regression target, and the 2026-08-03 run proved those can move
-- opposite to ranking quality: AE_en got 24% worse on MSLE while its rank
-- correlation improved by 0.071. Absolute error was the wrong instrument.
-- [v2:15] Per-city price spread, for the flat-city check below.
price_city AS (
  SELECT site_code, locale, location_id,
         MIN(price_in_usd) = MAX(price_in_usd) AS flat
  FROM preds
  WHERE impressions_1mth > 0
  GROUP BY 1, 2, 3
  HAVING COUNTIF(price_missing = 0) >= 10
),
city_rho AS (
  SELECT site_code, locale, location_id,
         COUNT(*) AS n,
         CORR(pred_rank, actual_rank) AS rho
  FROM (
    SELECT site_code, locale, location_id,
      RANK() OVER (PARTITION BY site_code, locale, location_id
                   ORDER BY predicted_total_itinerary_card_clicks DESC) AS pred_rank,
      RANK() OVER (PARTITION BY site_code, locale, location_id
                   ORDER BY total_itinerary_card_clicks DESC) AS actual_rank
    FROM preds
    WHERE total_itinerary_card_clicks IS NOT NULL
  )
  GROUP BY 1, 2, 3
  HAVING COUNT(*) >= 20
)
SELECT processing_date, 'hotel_sort_order_bqml', check_name, value, threshold,
       passed, detail
FROM (
  -- [v2:2] Price-source health, read off the predictions rather than by
  -- re-scanning hotels_rates. Catches the class of failure that killed the
  -- 6 Jul run -- price source silently stops delivering -- without paying for
  -- a second 17.5 TB-table scan.
  --
  -- [v2:13] Thresholds recalibrated against the 2026-08-03 run. The original
  -- 0.50 was set against the wrong denominator: every hotel in the catalogue
  -- appears once per POS (2,018,926 rows each), but only hotels that turned up
  -- in a search in the 30-day window get a rate. Measured coverage was 0.102 --
  -- roughly what the data can produce, not a fault. A gate that fails on every
  -- healthy run gets switched off, which is worse than having no gate.
  SELECT 'price_coverage' AS check_name,
         (SELECT SAFE_DIVIDE(COUNTIF(price_missing = 0), NULLIF(COUNT(*), 0)) FROM preds) AS value,
         0.05 AS threshold,
         (SELECT SAFE_DIVIDE(COUNTIF(price_missing = 0), NULLIF(COUNT(*), 0)) FROM preds) >= 0.05 AS passed,
         'full-catalogue coverage; low by construction (measured 0.102), floor only catches total source death' AS detail
  UNION ALL
  -- [v2:16] Coverage WEIGHTED BY IMPRESSIONS, not counted per row. Counting
  -- rows gives every hotel in the catalogue equal say, which is why the same
  -- quantity reads 0.102 unweighted, 0.5515 over impressed rows, and 0.8774
  -- weighted by impressions. Rate cards are produced for hotels users search
  -- for, so coverage rises steeply with demand -- click-weighted it is 0.9313.
  --
  -- Weighted by impressions rather than by clicks deliberately: clicks are the
  -- label, present on only ~426k of 20m rows and heavy-tailed, so a handful of
  -- big hotels would swing the metric run to run. Impressions are dense and
  -- nearly as decision-relevant.
  --
  -- This REPLACES the row-counted version rather than joining it. Two checks
  -- measuring the same quantity two ways can only disagree by being wrong, and
  -- the one that fails first sets the gate -- see [v2:15].
  SELECT 'price_coverage_weighted',
         (SELECT SAFE_DIVIDE(SUM(IF(price_missing = 0, impressions_1mth, 0)),
                             NULLIF(SUM(impressions_1mth), 0)) FROM preds),
         0.70,
         (SELECT SAFE_DIVIDE(SUM(IF(price_missing = 0, impressions_1mth, 0)),
                             NULLIF(SUM(impressions_1mth), 0)) FROM preds) >= 0.70,
         'impression-weighted price coverage -- share of what users SEE that is priced (measured 0.8774)'
  UNION ALL
  -- The check that needs no calibration, and the one that would have caught
  -- the fares table dying in March. Absolute levels drift with traffic mix;
  -- a 30% single-run drop does not happen for benign reasons. Compares against
  -- the most recent prior run, so it no-ops on the first run (threshold 0.0).
  SELECT 'price_coverage_regression',
         (SELECT SAFE_DIVIDE(COUNTIF(price_missing = 0), NULLIF(COUNT(*), 0)) FROM preds),
         IFNULL((SELECT value * 0.7 FROM hotel_sort_order_ml.pipeline_health
                 WHERE check_name = 'price_coverage' AND pipeline = 'hotel_sort_order_bqml'
                   AND run_date < processing_date
                 ORDER BY run_date DESC LIMIT 1), 0.0),
         (SELECT SAFE_DIVIDE(COUNTIF(price_missing = 0), NULLIF(COUNT(*), 0)) FROM preds)
           >= IFNULL((SELECT value * 0.7 FROM hotel_sort_order_ml.pipeline_health
                      WHERE check_name = 'price_coverage' AND pipeline = 'hotel_sort_order_bqml'
                        AND run_date < processing_date
                      ORDER BY run_date DESC LIMIT 1), 0.0),
         'coverage must not fall >30% vs the previous run -- catches a source dying silently'
  UNION ALL
  -- [v2:15] Replaces price_distinct_per_city, which measured nothing the
  -- coverage checks did not already measure. Decomposing it on the 2026-08-03
  -- run: of its 0.4492, no-price accounted for 0.4485, flat cities 0.0006 and
  -- true midpoints 0.0000. The 0.4485 is exactly 1 - price_coverage_impressed,
  -- so the check was a restatement of its neighbour to four decimals.
  --
  -- Flat cities are also almost entirely an artefact. Of 1,660 flat cities,
  -- 1,657 had exactly ONE priced hotel and are flat by arithmetic necessity.
  -- Above 10 priced hotels: 0 flat out of 6,209 cities.
  --
  -- So gate on the case that would actually be a fault -- a city with real
  -- price competition showing no spread at all, which is what a rate pipeline
  -- pinning every price to a default looks like. That failure hits thousands
  -- of cities at once, never one or two, hence the loose 0.01 rather than a
  -- hair-trigger 0. Measured 0.0000; this is a near-invariant, so unlike an
  -- absolute level it needs no run history behind it.
  SELECT 'price_flat_city',
         (SELECT SAFE_DIVIDE(COUNTIF(flat), NULLIF(COUNT(*), 0)) FROM price_city),
         0.01,
         (SELECT SAFE_DIVIDE(COUNTIF(flat), NULLIF(COUNT(*), 0)) FROM price_city) <= 0.01,
         'cities with >=10 priced hotels and zero price spread (measured 0 of 6209)'
  UNION ALL
  -- A ratio check passes vacuously if its denominator collapses. price_flat_city
  -- going NULL on an empty price_city already fails via IFNULL(passed, FALSE),
  -- but a PARTIAL collapse -- 6,209 scoreable cities down to 50 -- would sail
  -- through on 0 flat out of 50. This is that guard.
  SELECT 'price_cities_scored',
         CAST((SELECT COUNT(*) FROM price_city) AS FLOAT64),
         1000.0,
         (SELECT COUNT(*) FROM price_city) >= 1000,
         'cities with >=10 priced hotels among impressed rows (measured 6209)'
  UNION ALL
  -- [v2:14] Ranking quality. Baseline 0.495 all-POS on 2026-08-03 (0.5425
  -- weighted across the nine non-pooled POS, up from 0.4952 for the same POS
  -- on the 2026-06-29 v1 run). Threshold ~10% below the observed level.
  --
  -- IS_NAN filter is load-bearing: CORR returns NaN, not NULL, for a city
  -- where every prediction ties, and a single NaN propagates through SUM and
  -- takes the whole metric with it. OM_ar hit exactly this on the v1 run.
  SELECT 'rank_quality_spearman',
         (SELECT SAFE_DIVIDE(SUM(rho * n), NULLIF(SUM(n), 0)) FROM city_rho
          WHERE rho IS NOT NULL AND NOT IS_NAN(rho)),
         0.45,
         (SELECT SAFE_DIVIDE(SUM(rho * n), NULLIF(SUM(n), 0)) FROM city_rho
          WHERE rho IS NOT NULL AND NOT IS_NAN(rho)) >= 0.45,
         'in-sample city-weighted rank correlation, predicted vs actual clicks'
  UNION ALL
  -- A collapse in how many cities can even be scored is its own failure mode,
  -- separate from the correlation being low within the cities that remain.
  SELECT 'rank_quality_cities',
         CAST((SELECT COUNT(*) FROM city_rho
               WHERE rho IS NOT NULL AND NOT IS_NAN(rho)) AS FLOAT64),
         500.0,
         (SELECT COUNT(*) FROM city_rho WHERE rho IS NOT NULL AND NOT IS_NAN(rho)) >= 500,
         'cities with >=20 labelled hotels and a computable correlation (measured 3762)'
  UNION ALL
  SELECT 'served_pos_present',
         CAST((SELECT COUNT(DISTINCT CONCAT(site_code, "_", locale)) FROM preds) AS FLOAT64),
         CAST(ARRAY_LENGTH(served_pos) AS FLOAT64),
         (SELECT COUNT(DISTINCT CONCAT(site_code, "_", locale)) FROM preds)
           = ARRAY_LENGTH(served_pos),
         'every POS the Redis importer loads must have scores'
  UNION ALL
  SELECT 'prediction_rows',
         CAST((SELECT COUNT(*) FROM preds) AS FLOAT64), 1.0,
         (SELECT COUNT(*) FROM preds) > 0,
         'prediction table is not empty'
  UNION ALL
  SELECT 'prediction_null_rate',
         (SELECT SAFE_DIVIDE(COUNTIF(predicted_total_itinerary_card_clicks IS NULL),
                             NULLIF(COUNT(*), 0)) FROM preds),
         0.0,
         (SELECT COUNTIF(predicted_total_itinerary_card_clicks IS NULL) FROM preds) = 0,
         'a NULL prediction becomes score 0 and sinks the hotel'
  UNION ALL
  SELECT 'degenerate_city_share',
         (SELECT SAFE_DIVIDE(COUNTIF(norm IS NULL), NULLIF(COUNT(*), 0)) FROM preds),
         0.05,
         (SELECT SAFE_DIVIDE(COUNTIF(norm IS NULL), NULLIF(COUNT(*), 0)) FROM preds) <= 0.05,
         'share of rows in cities where every prediction is identical'
);

-- IFNULL(passed, FALSE): a check that could not be evaluated is a failure, not
-- a pass. Otherwise a NULL quietly counts as healthy, which is the exact class
-- of silence this block exists to remove.
SET health_failures = (
  SELECT COUNTIF(NOT IFNULL(passed, FALSE)) FROM hotel_sort_order_ml.pipeline_health
  WHERE run_date = processing_date AND pipeline = 'hotel_sort_order_bqml');

IF health_failures > 0 THEN
  IF enforce_health_gate THEN
    RAISE USING MESSAGE = FORMAT(
      "%d health check(s) failed - pivot NOT published. See hotel_sort_order_ml.pipeline_health for run_date %t.",
      health_failures, processing_date);
  END IF;
END IF;

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_pivot_wa_ml AS (
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
  -- [v2:8] Was normalised OVER () -- globally across every city at once, unlike
  -- the _prioritisation pivot below which partitions by city. That made this
  -- table's scores incomparable with the one actually served. Aligned here.
  (SELECT *,
  SAFE_DIVIDE(predicted_total_itinerary_card_clicks
                - MIN(predicted_total_itinerary_card_clicks) OVER w,
              NULLIF(MAX(predicted_total_itinerary_card_clicks) OVER w
                - MIN(predicted_total_itinerary_card_clicks) OVER w, 0)) * 100000000000
  as formula
  FROM `wego-cloud.hotels.hotel_rank_wa_ml`
  WINDOW w AS (PARTITION BY site_code, locale, location_id)
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_prioritisation Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_pivot_wa_ml_prioritisation AS (
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
  -- [v2:7] QUALITY FLOOR -- outermost, so nothing downstream of the model can
  -- lift unproven inventory above proven inventory in the same city.
  --
  -- Two disjoint bands inside the existing 0..1e11 range:
  --   unproven -> 0 .. 40%     (relative order preserved)
  --   proven   -> 40% .. 100%  (relative order preserved)
  -- Banding rather than subtracting a constant is deliberate: akasha's deal
  -- boost (+1e11) and sponsored-ad penalty (-1e11) key off the same scale, so a
  -- demotion of comparable magnitude would collide with those flags.
  --
  -- This is the one change here that ships against ALREADY-TRAINED scores. It
  -- needs no retraining and applies to whichever model feeds the table.
  -- min_reviews_count / min_image_count are the knobs -- start loose, tighten
  -- on evidence, and measure before/after on the same city set.
  (SELECT * EXCEPT (formula),
  CASE
    WHEN IFNULL(overall_score, 0) <= 0
      OR IFNULL(reviews_count, 0) < min_reviews_count
      OR IFNULL(image_count, 0) < min_image_count
    THEN SAFE_DIVIDE(IFNULL(formula, 0), score_scale) * (score_scale * quality_floor_band)
    ELSE (score_scale * quality_floor_band)
       + SAFE_DIVIDE(IFNULL(formula, 0), score_scale) * (score_scale * (1 - quality_floor_band))
  END as formula
  FROM
  (SELECT * EXCEPT (formula, x_position_formula),
  -- IF(ihg_ind > 0, PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id) + (100000000000-PERCENTILE_CONT(formula, 0.9) OVER (PARTITION BY site_code, locale, location_id))*RAND(), formula)*100 + RAND()*100 as formula
  -- Boosting transformation to push ihg properties to the similar score to 6th position but with strength of 0.8
  IF(ihg_ind > 0, IF(formula < x_position_formula, (SAFE_DIVIDE(formula, 100000000000) + (SAFE_DIVIDE(x_position_formula,100000000000) - SAFE_DIVIDE(formula, 100000000000))*0.85)*100000000000, formula), formula) as formula
  FROM
  (SELECT * EXCEPT (ranking), MAX(CASE WHEN ranking = 6 THEN formula END) OVER (PARTITION BY site_code, locale, location_id) as x_position_formula FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY site_code, locale, location_id ORDER BY formula DESC) ranking
  FROM
    -- [v2:8] NULLIF on the denominator. A city with one hotel, or one where
    -- every prediction is identical, previously produced formula = NULL for
    -- every hotel -> 0 in the pivot -> bottom of the page.
    (SELECT *,
    SAFE_DIVIDE(predicted_total_itinerary_card_clicks - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id),
    NULLIF(MAX(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id) - MIN(predicted_total_itinerary_card_clicks) OVER (PARTITION BY site_code, locale, location_id), 0)) * 100000000000
    as formula
    FROM `wego-cloud.hotels.hotel_rank_wa_ml`
    )
  )
  )))
GROUP BY hotel_id, location_id, location_code
);

-- [v2:10] MODELS 2-4 (gmv / add / remove) are gated off.
-- They train three more RANDOM_FOREST_REGRESSORs and write three more pivot
-- tables. None of hotel_rank_pivot_wa_ml_gmv / _add / _remove is read by
-- ScoreServices.java, which imports hotel_rank_pivot_wa_ml_prioritisation
-- only. They are roughly three quarters of this query's runtime and cost.
-- Skipping them leaves the existing _gmv / _add / _remove tables in place,
-- unrefreshed -- the same state they are in whenever the query fails today.
-- If anything turns out to consume them, set train_secondary_models = TRUE.
IF train_secondary_models THEN

-- -- ------------------- MODEL 2: Total GMV -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT ROUND(SAFE_DIVIDE(cases_not_null,cases_null),10) as null_to_not_null_deci_pct 
FROM
  (SELECT COUNTIF(total_gmv IS NULL) AS cases_null,
  COUNTIF(total_gmv IS NOT NULL) AS cases_not_null,
  SUM(1) AS total_cases
  FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`
  -- WHERE avg_displayed_position <= (
  --   SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
  --   FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`
  -- )
  -- OR avg_displayed_position IS NULL
  ) 
"""
, processing_date_suffix, processing_date_suffix)
INTO null_proportion
;

----- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_gmv IS NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`)
-- OR avg_displayed_position IS NULL
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_gmv IS NOT NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`)
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.gmv_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_gmv']) AS
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
impressions_3mth, 
clicks_3mth, 
conversions_3mth,
impressions_1mth, 
clicks_1mth, 
conversions_1mth,
ctr_3mth,
ctr_1mth,
cvr_3mth,
cvr_1mth,
-- price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
price_in_usd_weighted,
IFNULL(total_gmv,0) as total_gmv
FROM `hotel_sort_order_ml.gmv_training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.gmv_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.gmv_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.gmv_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);


----- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.gmv_rf_model_%s`,
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
-- impressions_3mth, 
-- clicks_3mth, 
-- conversions_3mth,
-- impressions_1mth, 
-- clicks_1mth, 
-- conversions_1mth,
-- ctr_3mth,
-- ctr_1mth,
-- cvr_3mth,
-- cvr_1mth,
-- -- price_in_usd, 
-- -- min_price_in_usd, 
-- -- max_price_in_usd, 
-- price_in_usd_weighted,
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- -- 8. Productionise it in Vertex AI
-- -- EXECUTE IMMEDIATE FORMAT("""
-- -- ALTER MODEL hotel_sort_order_ml.gmv_rf_model_%s 
-- -- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_gmv_rf_model')
-- -- """
-- -- , start_date_suffix);

-- -- -- DROP MODEL IF EXISTS hotel_sort_order_ml.rf_model20240213

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.gmv_rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT * , CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.gmv_rf_model_%s`,
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
price_in_usd,
total_itinerary_card_clicks,
total_gmv,
-- -- Main Features 
site_code,
locale,
hotel_star_rating,
-- hotel_id,
chain_id,
brand_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
impressions_3mth, 
clicks_3mth, 
conversions_3mth,
impressions_1mth, 
clicks_1mth, 
conversions_1mth,
ctr_3mth,
ctr_1mth,
cvr_3mth,
cvr_1mth,
-- price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
price_in_usd_weighted,
FROM `hotel_sort_order_ml.training_data_%s` 
-- -- WHERE total_gmv IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml_gmv Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_wa_ml_gmv AS (
SELECT * FROM `hotel_sort_order_ml.gmv_rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_gmv Table -----
-- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_pivot_wa_ml_gmv AS (
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
  SAFE_DIVIDE(predicted_total_gmv - MIN(predicted_total_gmv) OVER (),  
  MAX(predicted_total_gmv) OVER () - MIN(predicted_total_gmv) OVER ()) * 100000000000 as formula
  FROM `wego-cloud.hotels.hotel_rank_wa_ml_gmv`
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


-- -- ------------------- MODEL 3: Additional Features  -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT ROUND(SAFE_DIVIDE(cases_not_null,cases_null),10) as null_to_not_null_deci_pct 
FROM
  (SELECT COUNTIF(total_itinerary_card_clicks IS NULL) AS cases_null,
  COUNTIF(total_itinerary_card_clicks IS NOT NULL) AS cases_not_null,
  SUM(1) AS total_cases
  FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`
  -- WHERE avg_displayed_position <= (
  --   SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
  --   FROM `wego-cloud.hotel_sort_order_ml.add_training_data_%s`
  -- )
  -- OR avg_displayed_position IS NULL
  ) 
"""
, processing_date_suffix, processing_date_suffix)
INTO null_proportion
;

----- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.add_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_itinerary_card_clicks IS NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.add_training_data_%s`)
-- OR avg_displayed_position IS NULL
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_itinerary_card_clicks IS NOT NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.add_training_data_%s`)
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.add_rf_model_%s`
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
district_id,
image_count,
overall_score,
reviews_count,
booking_avg_reviews_score,
booking_reviews_count,
booking_0_5_reviews_count,
booking_5_6_reviews_count,  
booking_6_7_reviews_count,  
booking_7_8_reviews_count,  
booking_8_9_reviews_count,  
booking_9_10_reviews_count, 
property_type_id,
distance_to_city_centre,
impressions_12mth, 
clicks_12mth, 
conversions_12mth,
impressions_3mth, 
clicks_3mth, 
conversions_3mth,
impressions_1mth, 
clicks_1mth, 
conversions_1mth,
ctr_12mth,
ctr_3mth,
ctr_1mth,
cvr_12mth,
cvr_3mth,
cvr_1mth,
gmv_12mth,
gmv_3mth,
gmv_1mth,
-- price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
price_in_usd_weighted,
usual_price, 
discount_percent,
IFNULL(total_itinerary_card_clicks,0) as total_itinerary_card_clicks
FROM `hotel_sort_order_ml.add_training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.add_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.add_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.add_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.add_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.add_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.add_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);


----- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.add_rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.add_rf_model_%s`,
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
-- impressions_3mth, 
-- clicks_3mth, 
-- conversions_3mth,
-- impressions_1mth, 
-- clicks_1mth, 
-- conversions_1mth,
-- ctr_3mth,
-- ctr_1mth,
-- cvr_3mth,
-- cvr_1mth,
-- -- price_in_usd, 
-- -- min_price_in_usd, 
-- -- max_price_in_usd, 
-- price_in_usd_weighted,
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- 8. Productionise it in Vertex AI
-- EXECUTE IMMEDIATE FORMAT("""
-- ALTER MODEL hotel_sort_order_ml.add_rf_model_%s 
-- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_add_rf_model')
-- """
-- , start_date_suffix);

-- -- DROP MODEL IF EXISTS hotel_sort_order_ml.add_rf_model20240213

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.add_rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT *, CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.add_rf_model_%s`,
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
district_name,
price_in_usd,
total_itinerary_card_clicks,
total_gmv,
-- -- Main Features
site_code,
locale,
hotel_star_rating,
-- hotel_id,
chain_id,
brand_id,
district_id,
image_count,
overall_score,
reviews_count,
booking_avg_reviews_score,
booking_reviews_count,
booking_0_5_reviews_count,
booking_5_6_reviews_count,  
booking_6_7_reviews_count,  
booking_7_8_reviews_count,  
booking_8_9_reviews_count,  
booking_9_10_reviews_count, 
property_type_id,
distance_to_city_centre,
impressions_12mth, 
clicks_12mth, 
conversions_12mth,
impressions_3mth, 
clicks_3mth, 
conversions_3mth,
impressions_1mth, 
clicks_1mth, 
conversions_1mth,
ctr_12mth,
ctr_3mth,
ctr_1mth,
cvr_12mth,
cvr_3mth,
cvr_1mth,
gmv_12mth,
gmv_3mth,
gmv_1mth,
-- price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
price_in_usd_weighted,
usual_price, 
discount_percent,
FROM `hotel_sort_order_ml.training_data_%s` 
-- -- WHERE total_gmv IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml_add Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_wa_ml_add AS (
SELECT * FROM `hotel_sort_order_ml.add_rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_add Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_pivot_wa_ml_add AS (
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
  SAFE_DIVIDE(predicted_total_itinerary_card_clicks - MIN(predicted_total_itinerary_card_clicks) OVER (),  
  MAX(predicted_total_itinerary_card_clicks) OVER () - MIN(predicted_total_itinerary_card_clicks) OVER ()) * 100000000000 as formula
  FROM `wego-cloud.hotels.hotel_rank_wa_ml_add`
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


-- -- ------------------- MODEL 4: Removal of Features  -------------------
-- -- ----- 1. Find Proportion of Null vs Not Null Cases -----
EXECUTE IMMEDIATE FORMAT(
"""
SELECT ROUND(SAFE_DIVIDE(cases_not_null,cases_null),10) as null_to_not_null_deci_pct 
FROM
  (SELECT COUNTIF(total_itinerary_card_clicks IS NULL) AS cases_null,
  COUNTIF(total_itinerary_card_clicks IS NOT NULL) AS cases_not_null,
  SUM(1) AS total_cases
  FROM `wego-cloud.hotel_sort_order_ml.training_data_%s`
  -- WHERE avg_displayed_position <= (
  --   SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
  --   FROM `wego-cloud.hotel_sort_order_ml.remove_training_data_%s`
  -- )
  -- OR avg_displayed_position IS NULL
  ) 
"""
, processing_date_suffix, processing_date_suffix)
INTO null_proportion
;

----- 2. Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_training_data_downsample_%s AS (
SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_itinerary_card_clicks IS NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.remove_training_data_%s`)
-- OR avg_displayed_position IS NULL
AND RAND() < @null_proportion -- * 0.2/(1-0.2)

UNION ALL 

SELECT * FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
WHERE total_itinerary_card_clicks IS NOT NULL
-- AND avg_displayed_position <= (
-- SELECT APPROX_QUANTILES(avg_displayed_position, 4)[SAFE_OFFSET(1)] 
-- FROM `wego-cloud.hotel_sort_order_ml.remove_training_data_%s`)
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix, processing_date_suffix)
USING null_proportion as null_proportion;


-- ----- 3. Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `hotel_sort_order_ml.remove_rf_model_%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['total_itinerary_card_clicks']) AS
SELECT
site_code,
locale,
hotel_id,
hotel_star_rating,
chain_id,
brand_id,
district_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
-- price_in_usd_weighted,
IFNULL(total_itinerary_card_clicks,0) as total_itinerary_card_clicks
FROM `hotel_sort_order_ml.remove_training_data_downsample_%s`
"""
, processing_date_suffix, processing_date_suffix);

----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_rf_model_evaluation_%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`hotel_sort_order_ml.remove_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);

----- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_rf_model_feature_importance_%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `hotel_sort_order_ml.remove_rf_model_%s`)
ORDER BY importance_gain DESC
)
"""
, processing_date_suffix, processing_date_suffix);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_rf_model_global_explain_%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `hotel_sort_order_ml.remove_rf_model_%s`)
)
"""
, processing_date_suffix, processing_date_suffix);


----- 7. Individual Record Explainability (Optional) -----
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_rf_model_explain_predict_%s AS (
-- SELECT * FROM
-- ML.EXPLAIN_PREDICT(MODEL `hotel_sort_order_ml.remove_rf_model_%s`,
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
-- -- price_in_usd, 
-- -- min_price_in_usd, 
-- -- max_price_in_usd, 
-- price_in_usd_weighted,
-- FROM `wego-cloud.hotel_sort_order_ml.training_data_%s` 
-- WHERE site_code = "GH" AND locale = "en" AND location_code = "RUH"
-- AND hotel_id = "3226555"
-- )
-- )
-- )
-- """
-- , processing_date_suffix, processing_date_suffix, processing_date_suffix);

-- 8. Productionise it in Vertex AI
-- EXECUTE IMMEDIATE FORMAT("""
-- ALTER MODEL hotel_sort_order_ml.remove_rf_model_%s 
-- SET OPTIONS (vertex_ai_model_id='hotel_sort_order_remove_rf_model')
-- """
-- , start_date_suffix);

-- -- DROP MODEL IF EXISTS hotel_sort_order_ml.remove_rf_model20240213

----- 14. Offline Batch Prediction -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotel_sort_order_ml.remove_rf_model_predict_%s 
PARTITION BY processing_date
CLUSTER BY location_code, site_code, locale
AS (
SELECT *, CURRENT_DATE() as processing_date FROM
ML.PREDICT(MODEL `hotel_sort_order_ml.remove_rf_model_%s`,
(SELECT
-- -- Additional IDs
-- hotel_id,
hotel_name,
country,
country_code,
location,
location_id, 
location_code,
chain_name,
brand_name, 
property_type_name,
district_name,
total_itinerary_card_clicks,
-- -- Main Features
site_code,
locale,
hotel_star_rating,
hotel_id,
chain_id,
brand_id,
district_id,
image_count,
overall_score,
reviews_count,
property_type_id,
distance_to_city_centre,
price_in_usd, 
-- min_price_in_usd, 
-- max_price_in_usd, 
-- price_in_usd_weighted,
FROM `hotel_sort_order_ml.training_data_%s` 
-- -- WHERE total_itinerary_card_clicks IS NOT NULL
-- -- LIMIT 10
))
)
"""
, processing_date_suffix, processing_date_suffix, processing_date_suffix);

--- 15. Create hotel_rank_wa_ml_remove Table -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_wa_ml_remove AS (
SELECT * FROM `hotel_sort_order_ml.remove_rf_model_predict_%s`)
"""
, processing_date_suffix);

--- 16. Create site_code, locale pivot hotel_rank_pivot_wa_ml_remove Table -----
-- -- EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE hotels.hotel_rank_pivot_wa_ml_remove AS (
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
  SAFE_DIVIDE(total_itinerary_card_clicks - MIN(total_itinerary_card_clicks) OVER (),  
  MAX(total_itinerary_card_clicks) OVER () - MIN(total_itinerary_card_clicks) OVER ()) * 100000000000 as formula
  FROM `wego-cloud.hotels.hotel_rank_wa_ml_remove`
  )
GROUP BY hotel_id, location_id, location_code
);
-- -- """
-- -- , start_date_suffix, start_date_suffix,start_date_suffix);


END IF;  -- [v2:10] end train_secondary_models

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
    AND (table_id LIKE 'add_rf_model_%'
    OR table_id LIKE 'gmv_rf_model_%'
    OR table_id LIKE 'remove_rf_model_%'
    OR table_id LIKE 'rf_model_%'
    )
    AND table_id NOT IN ("rf_model_dummy","rf_model_dummy_singular","rf_model_hp","rf_model2")
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
    AND (table_id LIKE 'add_rf_model_%'
    OR table_id LIKE 'gmv_rf_model_%'
    OR table_id LIKE 'remove_rf_model_%'
    OR table_id LIKE 'rf_model_%'
    OR table_id LIKE 'data_date_range_%'
    OR table_id LIKE 'training_data_%'
    OR table_id LIKE 'gmv_training_data_%'
    OR table_id LIKE 'remove_training_data_%'
    OR table_id LIKE 'add_training_data_%'
    OR table_id LIKE 'null_proportion_by_pos_%' -- [v2:3]
    )
    AND table_id NOT IN ("rf_model_dummy","rf_model_dummy_singular","rf_model_hp","rf_model2","rf_model_feature_importance")
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



-- ------------- FEATURE GROUP FOR FEATURE STORE ----------------
-- 14. Feature Group: Hotel Metadata (Entity ID: hotel_id)
-- CREATE OR REPLACE TABLE hotel_sort_order_ml.feature_group_hotel_metadata AS (
-- SELECT *, CURRENT_TIMESTAMP() as feature_timestamp 
-- FROM
--     (SELECT
--     CAST(h.id AS STRING) as hotel_id, h.name_en as hotel_name, brand.chain_id as chain_id, chain_name, h.brand_id as brand_id, brand_name, c.base_name as country, c.code as country_code, l.base_name as location, l.code as location_code, l.id as location_id, star as hotel_star_rating,
--     img.image_count as image_count, trustyou.score as overall_score, trustyou.reviews_count as reviews_count, distance_to_city_centre, h.property_type_id, property_type_name,
--     IF(built_year IS NULL OR SAFE_CAST(built_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(built_year AS INT64) < 1900, NULL, SAFE_CAST(built_year AS INT64)) as built_year, -- Force null for strange years & less than 1900
--     IF(renovated_year IS NULL OR SAFE_CAST(renovated_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(renovated_year AS INT64) < 1900, NULL, SAFE_CAST(renovated_year AS INT64)) as renovated_year, -- Force null for strange years & less than 1900
--     FROM `wego-cloud.hotel_services.hotels` as h
--     LEFT JOIN
--     (SELECT * EXCEPT (rn)
--     FROM (SELECT base_name, code, id, country_id, RANK() OVER (PARTITION BY code ORDER BY updated_at DESC) as rn
--     FROM `wego-cloud.place_services.locations` )
--     WHERE rn = 1) as l on l.code=h.city_code
--     LEFT JOIN `wego-cloud.place_services.countries` as c on c.id=l.country_id
-- --            LEFT JOIN `wego-cloud.hotels.hotel_stats` as st on st.id=h.id
-- --            LEFT JOIN
-- --                (SELECT hotel_id, MAX(score) as score FROM hotel_services.reviews
-- --                WHERE reviewer_group='ALL' GROUP BY hotel_id) as trustyou on trustyou.hotel_id=h.id

--     LEFT JOIN

--     (SELECT hotel_id, sum(1) as image_count
--     FROM `wego-cloud.hotel_services.images`
--     GROUP BY hotel_id) as img

--     ON img.hotel_id=h.id

--     LEFT JOIN

--     (SELECT hotel_id, score, reviews_count,
--     -- IF(reviews_count IS NULL, AVG(reviews_count) OVER (), SAFE_DIVIDE(reviews_count,(MAX(reviews_count) OVER () - MIN(reviews_count) OVER()))) as reviews_count_weighted -- reviews_count/(max_reviews_count-min_reviews_count), otherwise take average.
--     FROM
--         (SELECT hotel_id, MAX(score) as score, MAX(count) as reviews_count
--         FROM hotel_services.reviews
--         WHERE reviewer_group='ALL'
--         GROUP BY hotel_id)
--     ) as trustyou

--     ON trustyou.hotel_id=h.id

--     LEFT JOIN

--     (SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
--     FROM `wego-cloud.hotel_services.brands`) as brand
--     ON h.brand_id = brand.brand_id

--     LEFT JOIN

--     (SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
--     FROM `wego-cloud.hotel_services.chains`) as chain
--     ON chain.chain_id = brand.chain_id

--     LEFT JOIN

--     (SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
--     FROM `hotel_services.property_types`) as property_type
--     on h.property_type_id = property_type.property_type_id

--     )
-- );
{% endraw %}
