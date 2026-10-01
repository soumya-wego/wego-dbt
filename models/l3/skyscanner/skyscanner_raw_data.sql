{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Skyscanner Bidding ML Model
-- Destination: skyscanner_bidding.skyscanner_raw_data  (unchanged)
-- Schedule   : every day 08:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- -- -- -- create table `analysis.skyscanner_bidfile_20250617` as


DECLARE null_proportion FLOAT64;
DECLARE date_shard_suffix STRING;
DECLARE date_shard DATE;
DECLARE processing_date DATE DEFAULT CURRENT_DATE();
DECLARE i INT64 DEFAULT 0;
DECLARE model_count INT64;
DECLARE model_list ARRAY<STRING>;
DECLARE model_name STRING;
DECLARE j INT64 DEFAULT 0;
DECLARE table_count INT64;
DECLARE table_list ARRAY<STRING>;
DECLARE table_name STRING;


SET date_shard = processing_date;
SET date_shard_suffix = FORMAT_DATE("%Y%m%d", date_shard);

------------------------ ORIGINAL PIPELINE ------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.skyscanner_raw_data%s AS (
WITH hotel_details AS
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
provider_count,
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
COUNT(DISTINCT provider_id) as provider_count,
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
on h.property_type_id = property_type.property_type_id),

wego_historical_performance AS 
(SELECT 
* EXCEPT (
  total_clicks, 
  total_conversions, 
  total_clicked_sessions, 
  total_converted_sessions, 
  total_gmv, 
  total_impressions, 
  total_bookings, 
  row_number
),

-- 1-month aliases (current month values)
total_clicks AS clicks_1mth,
total_conversions AS conversions_1mth,
total_clicked_sessions AS clicked_sessions_1mth,
total_converted_sessions AS converted_sessions_1mth,
total_gmv AS gmv_1mth,
total_impressions AS impressions_1mth,
total_bookings AS bookings_1mth,

-- CTRs
ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS ctr_1mth,
ROUND(SAFE_DIVIDE(clicks_2mth, impressions_2mth) * 100, 2) AS ctr_2mth,
ROUND(SAFE_DIVIDE(clicks_3mth, impressions_3mth) * 100, 2) AS ctr_3mth,
ROUND(SAFE_DIVIDE(clicks_6mth, impressions_6mth) * 100, 2) AS ctr_6mth,
ROUND(SAFE_DIVIDE(clicks_12mth, impressions_12mth) * 100, 2) AS ctr_12mth,

-- CVRs
ROUND(SAFE_DIVIDE(total_conversions, total_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, clicks_12mth) * 100, 2) AS cvr_12mth,

-- Converted session rates
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
  total_bookings,

  -- 2-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicked_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS impressions_2mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS bookings_2mth,

  -- 3-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicked_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS impressions_3mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS bookings_3mth,

  -- 6-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicked_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS impressions_6mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS bookings_6mth,

  -- 12-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicked_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS impressions_12mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS bookings_12mth,

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
    (SELECT FORMAT_DATE('%%Y-%%m', month_list) AS month
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH),DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),INTERVAL 1 MONTH)) AS month_list
    )
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
    WHERE DATE(_PARTITIONDATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    GROUP BY 1,2) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT
    CAST(hotel_id AS INT64) as hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', search_created_at) AS month, 
    COUNT(DISTINCT search_id) as total_impressions
    FROM `wego_analytics.hotels_impressions*`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) AND FORMAT_DATE('%%Y%%m%%d', CURRENT_DATE())
    GROUP BY 1,2) as c

    ON a.hotel_id = c.hotel_id AND a.month = c.month

    LEFT JOIN

    (SELECT hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    -- COUNT(DISTINCT IF(attribution_ts_code = '6be92',booking_id, NULL)) as total_skyscanner_bookings,
    -- SUM(IF(attribution_ts_code = '6be92',wego_total_price_usd, NULL)) as total_skyscanner_gmv,
    COUNT(DISTINCT booking_id) as total_bookings
    FROM `wego-cloud.wego_analytics.hotels_bookings` 
    WHERE DATE(created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND attribution_ts_code IS NULL
    GROUP BY 1,2) as d

    ON a.hotel_id = d.hotel_id AND a.month = d.month
    )
  )
  WHERE row_number = 1),

skyscanner_historical_performance AS

(SELECT 
-- campaign_name, 
partner_property_id, 
date, 
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
SAFE_DIVIDE(main_display_hotel_impressions_cumulative,hotel_impressions_cumulative) AS main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
SAFE_DIVIDE(display_rank_impressions_cumulative,hotel_impressions_cumulative) AS avg_display_rank_cumulative,
SAFE_DIVIDE(price_impressions_cumulative,hotel_impressions_cumulative) AS avg_price_cumulative,
SAFE_DIVIDE(absolute_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS absolute_price_difference_cumulative,
SAFE_DIVIDE(pct_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS pct_price_difference_cumulative,
SAFE_DIVIDE(beat_pricing_impressions_cumulative,meet_pricing_impressions_cumulative) AS beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
SAFE_DIVIDE(bid_average_gbp_pence_impressions_cumulative,hotel_impressions_cumulative) AS avg_bid_average_gbp_pence_cumulative
FROM
  (SELECT 
  -- campaign_name, 
  partner_property_id, 
  date, 
  -- clicks as skyscanner_clicks, 
  -- total_bookings as skyscanner_bookings,
  -- hotel_impressions,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  ROW_NUMBER() OVER (PARTITION BY partner_property_id ORDER BY date ) AS date_ranking,
  SUM(clicks) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_clicks_cumulative,
  SUM(total_bookings) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_bookings_cumulative,
  SUM(hotel_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hotel_impressions_cumulative,
  SUM(main_display_hotel_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS main_display_hotel_impressions_cumulative,
  SUM(cost_gbp_pence) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cost_gbp_pence_cumulative,
  SUM(beat_pricing_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS beat_pricing_impressions_cumulative,
  SUM(meet_pricing_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS meet_pricing_impressions_cumulative,
  SUM(display_rank_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS display_rank_impressions_cumulative,
  SUM(price_impressions_cumulative) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS price_impressions_cumulative,
  SUM(absolute_price_difference_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS absolute_price_difference_impressions_cumulative,
  SUM(pct_price_difference_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pct_price_difference_impressions_cumulative,
  MIN(min_bid_average_gbp_pence) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS min_bid_average_gbp_pence_cumulative,
  MAX(max_bid_average_gbp_pence) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_bid_average_gbp_pence_cumulative,
  SUM(bid_average_gbp_pence_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS bid_average_gbp_pence_impressions_cumulative,
  -- SAFE_DIVIDE(SUM(display_rank_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS avg_display_rank_cumulative,
  -- SAFE_DIVIDE(SUM(price_difference_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS price_difference_cumulative,
  -- SUM(clicks) OVER (PARTITION BY partner_property_id) AS total_clicks_overall,
  -- SUM(total_bookings) OVER (PARTITION BY partner_property_id) AS total_bookings_overall,
    FROM
    (SELECT 
    -- campaign_name, 
    -- SPLIT(campaign_name,"-")[SAFE_OFFSET(0)] AS site_code,
    -- SPLIT(campaign_name,"-")[SAFE_OFFSET(1)] AS device_type,
    partner_property_id, 
    date, 
    SUM(clicks) as clicks, 
    SUM(hotel_impressions) as hotel_impressions,
    SUM(CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN hotel_impressions
    WHEN display_rank <= 2 THEN hotel_impressions
    ELSE 0 END) AS  main_display_hotel_impressions,
    SUM(cost_gbp_pence) AS cost_gbp_pence,
    SUM(beat_pricing_impressions) AS beat_pricing_impressions,
    SUM(meet_pricing_impressions) AS meet_pricing_impressions,
    SUM(display_rank*hotel_impressions) AS display_rank_impressions,
    SUM((base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg) * hotel_impressions) AS price_impressions_cumulative,
    SUM(price_difference_gbp_pence_avg * hotel_impressions) AS absolute_price_difference_impressions,
    SUM(SAFE_DIVIDE(100*price_difference_gbp_pence_avg * hotel_impressions,base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) AS pct_price_difference_impressions,
    MIN(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS min_bid_average_gbp_pence,
    MAX(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS max_bid_average_gbp_pence,
    SUM(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) * hotel_impressions) as bid_average_gbp_pence_impressions
    FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
    WHERE display_rank > 0
    GROUP BY 1,2
    ) as a

    LEFT JOIN

    (SELECT
    DATE(created_at) as created_at,
    -- site_code, 
    -- CASE WHEN device_type = "desktop-web" THEN "DESKTOP"
    -- WHEN device_type IN ("android-app", "ios-app") THEN "APP"
    -- WHEN device_type IN ("smartphone-web","tablet-web") THEN "MWEB"
    -- END AS device_type,
    CAST(hotel_id AS STRING) as hotel_id,
    COUNT(DISTINCT booking_id) as total_bookings,
    FROM `wego-cloud.wego_analytics.hotels_bookings` 
    WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
    and attribution_ts_code = '6be92' -- Skyscanner Bookings
    GROUP BY 1,2) as b

    ON a.date = b.created_at AND a.partner_property_id = b.hotel_id
    -- AND a.site_code = b.site_code AND a.device_type = b.device_type 
  )
WHERE date_ranking > 1
),


hotel_list as
(
  select * from(
    select
      partner_property_id
      , date_added
      , country_code
      , campaign_type
    from `wego-cloud.analysis.skyscanner_bid_hotel_list`
    where partner_property_id is not null
  --   union distinct 
  --   select
  --     cast(partner_property_id as string)
  --     , min(date) as date_added
  --     , cast(null as string) as country_code
  --     , 'international' as campaign_type
  --   from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
  --   where split(campaign_name, "-")[offset(0)] in (
  --     'AE','OM','KW','SA','QA','BH','JO','US','CA','UK'
  --     )
  --     and display_rank > 0
  --     and date >= date(current_date() - 7)
  --     and partner_property_id is not null
  -- group by 1, 3, 4
  )
  qualify row_number()over(partition by partner_property_id order by date_added desc) = 1
)

, campaign_list as
(SELECT
country_code,
CONCAT(country_code, '-', device) AS campaign_name
FROM
UNNEST(['OM', 'KW', 'SA', 'QA', 'AE', 'BH', 'JO', 'US', 'CA', 'UK']) AS country_code,
UNNEST(['MWEB', 'APP', 'DESKTOP']) AS device
)

, bid_list as
(
  select distinct
    partner_property_id
    , campaign_name
    , campaign_type
    , hl.country_code as hotel_country_code
    , cl.country_code as campaign_country_code
  from hotel_list hl
  cross join campaign_list cl
    -- on hl.country_code = 
)

, skyscanner as
(
  select 
    partner_property_id
    , campaign_name
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(0)] AS site_code
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(1)] AS device_type
    , hotel_impressions
    , price_difference_gbp_pence_avg
    , taxes_fees_gbp_pence_avg
    , base_price_gbp_pence_avg
    , bid_average_gbp_pence
    , los_multiplier
    , los
    , display_rank
    , CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN 1
      WHEN display_rank <= 2 THEN 1
      ELSE 0 END AS main_display_ind
    , cost_gbp_pence
    , clicks
    , guests
    , check_in_date
    , EXTRACT(YEAR FROM check_in_date) AS check_in_year
    , EXTRACT(MONTH FROM check_in_date) AS check_in_month
    , EXTRACT(DAY FROM check_in_date) AS check_in_day
    , DATE_DIFF(check_in_date, date, DAY) AS leadtime
    , dow
    , tod
    , date
    , EXTRACT(YEAR FROM date) AS created_at_year
    , EXTRACT(MONTH FROM date) AS created_at_month
    , EXTRACT(DAY FROM date) AS created_at_day
    , SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) as avg_bid_current
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg AS total_price_gbp_pence
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg AS next_cheapest_total_price_gbp_pence
    , ROUND(100*SAFE_DIVIDE(price_difference_gbp_pence_avg,base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg),2) AS next_cheapest_total_price_gbp_pence_pct
    , beat_pricing_impressions
    , meet_pricing_impressions
  from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
  left join `wego-cloud.hotel_services.hotels` as b 
    on cast(a.partner_property_id as string) = cast(b.id as string)
  -- where b.city_code in ("DXB", "DWC", "AUH", "SHJ", "RKT", "FJR", "AAN", "HKT", "BKK")
  -- and split(a.campaign_name, "-")[offset(0)] in ("AE", "OM","KW")
  WHERE display_rank > 0
  -- and date >= date(current_date() - 2) -- Take Everything
)

-- , high_click as
-- (
--     select 
--         partner_property_id
--         , name_en
--         , sum(case when date >= (current_date - interval 14 day) then clicks else 0 end) as l14d_click
--         , sum(clicks) as total_click
--         , min(date) as first_live_date
--         , max(date) as last_click_date
--         , count(distinct date) as total_impression_date
--         , sum(hotel_impressions-avail_impressions_missed) as total_impression
--         , sum(cost_gbp_pence) as total_cost_gbp_pence
--     from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
--     left join `wego-cloud.hotel_services.hotels` as b 
--             on cast(a.partner_property_id as string) = cast(b.id as string)
--     where display_rank > 0
--     group by 1,2
--     having (l14d_click > 10
--     and date_diff(current_date,first_live_date,day) >= 14
--     and total_impression_date >= 14)
--     or total_click > 15
-- )

-- , bookings as
-- (
--     select
--       hotel_id
--       , count(distinct booking_id) as total_booking
--     from `wego-cloud.wego_analytics.hotels_bookings` 
--     where date(created_at) >= date(current_date - interval 14 day)
--     and attribution_ts_code = '6be92'
--     group by 1
-- )

-- , exclude_hotel as
-- (
--     select distinct
--         partner_property_id
--         , name_en
--         , first_live_date
--         , last_click_date
--         , total_click
--         , l14d_click
--         , total_impression
--         , total_cost_gbp_pence
--         , total_impression_date
--         , coalesce(total_booking,0) as total_booking
--     from high_click hc
--     left join bookings b
--         on cast(hc.partner_property_id as string) = cast(b.hotel_id as string)
--     where b.hotel_id is null
-- )

-- , property_stats as (
--   select 
--     campaign_name
--     , partner_property_id
--     , sum(display_rank * hotel_impressions) / sum(hotel_impressions) as display_rank
--     , sum((price_difference_gbp_pence_avg * hotel_impressions) 
--           / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
--         / sum(hotel_impressions) as avg_price_diff
--     , round(sum((bid_average_gbp_pence / (los * los_multiplier)) * hotel_impressions) 
--             / sum(hotel_impressions)) as avg_bid_current
--     , sum(hotel_impressions) as impressions
--     , sum(clicks) as clicks
--   from skyscanner
--   group by 1, 2
-- )

-- , prio_list as (
--   select 
--       *
--       , percent_rank()over(partition by campaign_name order by impressions) as impression_perc_rank
--   from (
--     select 
--         campaign_name
--       , partner_property_id
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) 
--           / sum(hotel_impressions) as perc_occurence
--       , sum(hotel_impressions) as impressions
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) avg_price_diff
--       , avg(display_rank) as avg_rank
--       , min(display_rank) as min_rank
--       , max(display_rank) as max_rank
--     from skyscanner
--     group by 1, 2
--   )
--   group by 1, 2, 3, 4, 5, 6, 7,8
-- )

SELECT 
b.partner_property_id as bid_list_partner_property_id,
b.campaign_name as bid_list_campaign_name,
b.campaign_type as bid_list_campaign_type,
b.hotel_country_code as bid_list_hotel_country_code,
b.campaign_country_code as bid_list_campaign_country_code,
IF(a.partner_property_id IS NOT NULL,1,0) AS historical_performance_ind, 
a.*,
hotel_details.*, 
wego_historical_performance.* EXCEPT (hotel_id),
skyscanner_historical_performance.* EXCEPT (partner_property_id, date)
FROM bid_list as b
LEFT JOIN skyscanner as a
ON b.partner_property_id = a.partner_property_id AND b.campaign_name = a.campaign_name
-- LEFT JOIN exclude_hotel c
-- on b.partner_property_id = c.partner_property_id
LEFT JOIN hotel_details
ON b.partner_property_id = hotel_details.hotel_id
LEFT JOIN wego_historical_performance
ON b.partner_property_id = wego_historical_performance.hotel_id
LEFT JOIN skyscanner_historical_performance
ON a.partner_property_id = skyscanner_historical_performance.partner_property_id 
-- AND a.campaign_name = skyscanner_historical_performance.campaign_name 
AND a.date = skyscanner_historical_performance.date 
-- WHERE c.partner_property_id is null
)
"""
,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.training_data%s AS (
SELECT * FROM skyscanner_bidding.skyscanner_raw_data%s
WHERE historical_performance_ind = 1 
)
"""
,date_shard_suffix,date_shard_suffix
);

-- Check Ratio of Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
  SELECT SAFE_DIVIDE(not_null_cases, null_cases)
  FROM (
    SELECT 
    COUNTIF(clicks = 0) AS null_cases,
    COUNTIF(clicks > 0) AS not_null_cases
    FROM `wego-cloud.skyscanner_bidding.training_data%s`
  )
"""
,date_shard_suffix)
INTO null_proportion;

---- Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.training_data_downsample%s AS (
SELECT *, 
-- CASE WHEN PERCENT_RANK() OVER (ORDER BY date) <= 0.8 THEN 'TRAIN'
-- ELSE 'TEST' END AS train_test_split
FROM
  (SELECT * FROM `wego-cloud.skyscanner_bidding.training_data%s`
  WHERE clicks = 0
  AND RAND() < @null_proportion -- * 0.2/(1-0.2)

  UNION ALL 

  SELECT * FROM `wego-cloud.skyscanner_bidding.training_data%s`
  WHERE clicks > 0
  ) 
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix)
USING null_proportion as null_proportion;

----- Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `skyscanner_bidding.rf_model%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['clicks']
  -- DATA_SPLIT_METHOD = 'SEQ',
  -- DATA_SPLIT_COL = 'date'
  ) AS
SELECT
hotel_name,
-- chain_id,
chain_name,
-- brand_id,
brand_name,
-- country,
country_code,
-- location,
location_code,
-- location_id,
-- district_id,
district_name,
hotel_star_rating,
image_count,
overall_score,
reviews_count,
distance_to_city_centre,
-- property_type_id,
property_type_name,
-- built_year,
-- renovated_year,
-- booking_avg_reviews_score,
-- booking_reviews_count,
-- booking_0_5_reviews_count,
-- booking_5_6_reviews_count,
-- booking_6_7_reviews_count,
-- booking_7_8_reviews_count,
-- booking_8_9_reviews_count,
-- booking_9_10_reviews_count,
provider_count,
clicks_1mth,
conversions_1mth,
clicked_sessions_1mth,
converted_sessions_1mth,
gmv_1mth,
impressions_1mth,
bookings_1mth,
conversions_2mth,
clicks_2mth,
converted_sessions_2mth,
clicked_sessions_2mth,
gmv_2mth,
impressions_2mth,
bookings_2mth,
conversions_3mth,
clicks_3mth,
converted_sessions_3mth,
clicked_sessions_3mth,
gmv_3mth,
impressions_3mth,
bookings_3mth,
conversions_6mth,
clicks_6mth,
converted_sessions_6mth,
clicked_sessions_6mth,
gmv_6mth,
impressions_6mth,
bookings_6mth,
conversions_12mth,
clicks_12mth,
converted_sessions_12mth,
clicked_sessions_12mth,
gmv_12mth,
impressions_12mth,
bookings_12mth,
ctr_1mth,
ctr_2mth,
ctr_3mth,
ctr_6mth,
ctr_12mth,
cvr_1mth,
cvr_2mth,
cvr_3mth,
cvr_6mth,
cvr_12mth,
converted_sessions_rate_1mth,
converted_sessions_rate_2mth,
converted_sessions_rate_3mth,
converted_sessions_rate_6mth,
converted_sessions_rate_12mth,
site_code,
device_type,
los,
guests,
created_at_year,
created_at_month,
created_at_day,
check_in_year,
check_in_month,
check_in_day,
leadtime,
dow,
tod,
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
avg_display_rank_cumulative,
avg_price_cumulative,
absolute_price_difference_cumulative,
pct_price_difference_cumulative,
beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
avg_bid_average_gbp_pence_cumulative,
display_rank,
main_display_ind,
price_difference_gbp_pence_avg,
total_price_gbp_pence,
next_cheapest_total_price_gbp_pence,
next_cheapest_total_price_gbp_pence_pct,
-- cost_gbp_pence, -- Might be reflective of clicks, giving the answer
-- beat_pricing_impressions,
-- meet_pricing_impressions,
avg_bid_current, 
IFNULL(clicks,0) as clicks,
-- train_test_split
FROM `skyscanner_bidding.training_data_downsample%s`
"""
,date_shard_suffix,date_shard_suffix
);


----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.rf_model_evaluation%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`skyscanner_bidding.rf_model%s`))
"""
,date_shard_suffix,date_shard_suffix
);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.rf_model_feature_importance%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `skyscanner_bidding.rf_model%s`)
ORDER BY importance_gain DESC
)
"""
,date_shard_suffix,date_shard_suffix
);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.rf_model_global_explain%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `skyscanner_bidding.rf_model%s`)
)
"""
,date_shard_suffix,date_shard_suffix
);

--------- GENERATE MOCK BIDS PER CAMPAIGN, HOTEL ID COMBINATION TO PREDICT AGAINST MODEL ---------
-- Generate Array of Bids from 20 to 135, in steps of 5 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.bid_template%s AS (
SELECT *
FROM
  (SELECT 
  campaign_name,
  partner_property_id,
  -- Model Features --
  hotel_name,
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  clicked_sessions_1mth,
  converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  conversions_2mth,
  clicks_2mth,
  converted_sessions_2mth,
  clicked_sessions_2mth,
  gmv_2mth,
  impressions_2mth,
  bookings_2mth,
  conversions_3mth,
  clicks_3mth,
  converted_sessions_3mth,
  clicked_sessions_3mth,
  gmv_3mth,
  impressions_3mth,
  bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  converted_sessions_6mth,
  clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  converted_sessions_12mth,
  clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  ctr_2mth,
  ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  cvr_2mth,
  cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  converted_sessions_rate_1mth,
  converted_sessions_rate_2mth,
  converted_sessions_rate_3mth,
  converted_sessions_rate_6mth,
  converted_sessions_rate_12mth, 
  -- If it's part of a bid list, but there's no historical skyscanner performance data, take the global average to use as a basis to predict against the model
  IF(historical_performance_ind = 1, a.site_code, b.site_code) AS site_code,
  IF(historical_performance_ind = 1, a.device_type, b.device_type) AS device_type,
  IF(historical_performance_ind = 1, a.los, b.los) AS los,
  IF(historical_performance_ind = 1, a.guests, b.guests) AS guests,
  IF(historical_performance_ind = 1, a.created_at_year, b.created_at_year) AS created_at_year,
  IF(historical_performance_ind = 1, a.created_at_month, b.created_at_month) AS created_at_month,
  IF(historical_performance_ind = 1, a.created_at_day, b.created_at_day) AS created_at_day,
  IF(historical_performance_ind = 1, a.check_in_year, b.check_in_year) AS check_in_year,
  IF(historical_performance_ind = 1, a.check_in_month, b.check_in_month) AS check_in_month,
  IF(historical_performance_ind = 1, a.check_in_day, b.check_in_day) AS check_in_day,
  IF(historical_performance_ind = 1, a.leadtime, b.leadtime) AS leadtime,
  IF(historical_performance_ind = 1, a.dow, b.dow) AS dow,
  IF(historical_performance_ind = 1, a.tod, b.tod) AS tod,
  IF(historical_performance_ind = 1, a.skyscanner_clicks_cumulative, b.skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
  IF(historical_performance_ind = 1, a.skyscanner_bookings_cumulative, b.skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
  IF(historical_performance_ind = 1, a.hotel_impressions_cumulative, b.hotel_impressions_cumulative) AS hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_hotel_impressions_cumulative, b.main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_ratio_cumulative, b.main_display_ratio_cumulative) AS main_display_ratio_cumulative,
  IF(historical_performance_ind = 1, a.cost_gbp_pence_cumulative, b.cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.beat_pricing_impressions_cumulative, b.beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.meet_pricing_impressions_cumulative, b.meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.avg_display_rank_cumulative, b.avg_display_rank_cumulative) AS avg_display_rank_cumulative,
  IF(historical_performance_ind = 1, a.avg_price_cumulative, b.avg_price_cumulative) AS avg_price_cumulative,
  IF(historical_performance_ind = 1, a.absolute_price_difference_cumulative, b.absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.pct_price_difference_cumulative, b.pct_price_difference_cumulative) AS pct_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.beat_meet_pricing_ratio_cumulative, b.beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
  IF(historical_performance_ind = 1, a.min_bid_average_gbp_pence_cumulative, b.min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.max_bid_average_gbp_pence_cumulative, b.max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.avg_bid_average_gbp_pence_cumulative, b.avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.display_rank, b.display_rank) AS display_rank,
  IF(historical_performance_ind = 1, a.main_display_ind, b.main_display_ind) AS main_display_ind,
  IF(historical_performance_ind = 1, a.price_difference_gbp_pence_avg, b.price_difference_gbp_pence_avg) AS price_difference_gbp_pence_avg,
  IF(historical_performance_ind = 1, a.total_price_gbp_pence, b.total_price_gbp_pence) AS total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence, b.next_cheapest_total_price_gbp_pence) AS next_cheapest_total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence_pct, b.next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct
  FROM
    (SELECT 
    COALESCE(bid_list_campaign_name, campaign_name) AS campaign_name,
    COALESCE(bid_list_partner_property_id, partner_property_id) as partner_property_id,
    -- Model Features --
    hotel_name,
    chain_name,
    brand_name,
    country_code,
    location_code,
    district_name,
    hotel_star_rating,
    image_count,
    overall_score,
    reviews_count,
    distance_to_city_centre,
    property_type_name,
    provider_count,
    clicks_1mth,
    conversions_1mth,
    clicked_sessions_1mth,
    converted_sessions_1mth,
    gmv_1mth,
    impressions_1mth,
    bookings_1mth,
    conversions_2mth,
    clicks_2mth,
    converted_sessions_2mth,
    clicked_sessions_2mth,
    gmv_2mth,
    impressions_2mth,
    bookings_2mth,
    conversions_3mth,
    clicks_3mth,
    converted_sessions_3mth,
    clicked_sessions_3mth,
    gmv_3mth,
    impressions_3mth,
    bookings_3mth,
    conversions_6mth,
    clicks_6mth,
    converted_sessions_6mth,
    clicked_sessions_6mth,
    gmv_6mth,
    impressions_6mth,
    bookings_6mth,
    conversions_12mth,
    clicks_12mth,
    converted_sessions_12mth,
    clicked_sessions_12mth,
    gmv_12mth,
    impressions_12mth,
    bookings_12mth,
    ctr_1mth,
    ctr_2mth,
    ctr_3mth,
    ctr_6mth,
    ctr_12mth,
    cvr_1mth,
    cvr_2mth,
    cvr_3mth,
    cvr_6mth,
    cvr_12mth,
    converted_sessions_rate_1mth,
    converted_sessions_rate_2mth,
    converted_sessions_rate_3mth,
    converted_sessions_rate_6mth,
    converted_sessions_rate_12mth,
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(0)], campaign_name) AS site_code,
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(1)], campaign_name) AS device_type,
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    -- Check Indicator of Whether There was Historical Skyscanner Performance
    MAX(historical_performance_ind) AS historical_performance_ind,
    FROM `skyscanner_bidding.skyscanner_raw_data%s`
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67) as a

    CROSS JOIN

    -- Generate One Row of a Global Dummy Average for Hotels without any Historical Performance
    (SELECT 
    MAX("SA") AS site_code,
    MAX("APP") AS device_type,
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    FROM `skyscanner_bidding.skyscanner_raw_data%s`
    WHERE historical_performance_ind = 1) as b 
  ),UNNEST(GENERATE_ARRAY(20,135,5)) as mock_bid
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

-- -- --------- GET PREDICITONS FROM MODEL ---------
--- For each hotel, put in a list of mock bids to predict with the model, then rank by best click prediction 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.training_data_explain_predict%s AS (
SELECT *,
DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value DESC) as prediction_ranking,
-- Less Precise on the Decimal Places of Prediction Value (So that more consideration among lower bid higher predicted click cases)
-- DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value_exponent DESC, ROUND(prediction_value_mantissa,1) DESC) as prediction_ranking_blunt, 
FROM
  (SELECT
  -- input.*,
  -- predoutput.*
  predoutput.* ,
  -- predoutput.predicted_total_itinerary_card_clicks,
  -- predoutput.total_itinerary_card_clicks_probs
  FLOOR(LOG10(predoutput.prediction_value)) AS prediction_value_exponent,
  SAFE_DIVIDE(predoutput.prediction_value,POWER(10, FLOOR(LOG10(predoutput.prediction_value)))) AS prediction_value_mantissa
  FROM
  ML.EXPLAIN_PREDICT(MODEL `skyscanner_bidding.rf_model%s`,
  (
  SELECT
  campaign_name,
  partner_property_id,
  -- Model Features --
  hotel_name,
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  clicked_sessions_1mth,
  converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  conversions_2mth,
  clicks_2mth,
  converted_sessions_2mth,
  clicked_sessions_2mth,
  gmv_2mth,
  impressions_2mth,
  bookings_2mth,
  conversions_3mth,
  clicks_3mth,
  converted_sessions_3mth,
  clicked_sessions_3mth,
  gmv_3mth,
  impressions_3mth,
  bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  converted_sessions_6mth,
  clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  converted_sessions_12mth,
  clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  ctr_2mth,
  ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  cvr_2mth,
  cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  converted_sessions_rate_1mth,
  converted_sessions_rate_2mth,
  converted_sessions_rate_3mth,
  converted_sessions_rate_6mth,
  converted_sessions_rate_12mth,
  site_code,
  device_type,
  los,
  guests,
  created_at_year,
  created_at_month,
  created_at_day,
  check_in_year,
  check_in_month,
  check_in_day,
  leadtime,
  dow,
  tod,
  skyscanner_clicks_cumulative,
  skyscanner_bookings_cumulative,
  hotel_impressions_cumulative,
  main_display_hotel_impressions_cumulative,
  main_display_ratio_cumulative,
  cost_gbp_pence_cumulative,
  beat_pricing_impressions_cumulative,
  meet_pricing_impressions_cumulative,
  avg_display_rank_cumulative,
  avg_price_cumulative,
  absolute_price_difference_cumulative,
  pct_price_difference_cumulative,
  beat_meet_pricing_ratio_cumulative,
  min_bid_average_gbp_pence_cumulative,
  max_bid_average_gbp_pence_cumulative,
  avg_bid_average_gbp_pence_cumulative,
  display_rank,
  main_display_ind,
  price_difference_gbp_pence_avg,
  total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence_pct,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  mock_bid as avg_bid_current,
  FROM `skyscanner_bidding.bid_template%s`
  -- WHERE campaign_name = "AE-APP"	
  -- AND partner_property_id = "3051914"
  ),
  STRUCT(TRUE AS approx_feature_contrib))
  predoutput 
  -- join `skyscanner_bidding.bid_template%s` input 
  -- ON predoutput.campaign_name=input.campaign_name 
  -- AND predoutput.partner_property_id=input.partner_property_id
  -- AND predoutput.avg_bid_current=input.avg_bid_current
)
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);


-- -- --------- SELECT BEST BID BASED ON HIGHEST PREDICTED VALUE (Clicks) ---------
--- Based on each click prediction from the model, if there's a tie for 1st place, take the lowest bid needed to get a max predicted clicks
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.bid_selection%s AS (
SELECT *, LEAST(avg_bid_current,135) as bid,
FROM
  (SELECT campaign_name, partner_property_id, MIN(avg_bid_current) as avg_bid_current
  FROM `skyscanner_bidding.training_data_explain_predict%s`
  WHERE prediction_ranking = 1
  GROUP BY 1,2)
)
"""
,date_shard_suffix,date_shard_suffix
);


EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.union_group%s AS (
SELECT
  partner_property_id
  , campaign_name
  , least(bid,135) as bid
FROM `skyscanner_bidding.bid_selection%s`
UNION ALL
SELECT 
  b.partner_property_id, 
  a.campaign_name, 
  1 AS bid 
FROM (
  SELECT DISTINCT CONCAT(campaign_name, "-", "GROUP") AS campaign_name 
  FROM `skyscanner_bidding.bid_selection%s`
) AS a
JOIN (
  SELECT DISTINCT partner_property_id 
  FROM `skyscanner_bidding.bid_selection%s`
) AS b ON 1=1
ORDER BY campaign_name, partner_property_id
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.bid_file%s AS (
SELECT *
FROM (
  SELECT
    partner_property_id as `Hotel ID`,
    campaign_name,
    bid
  FROM `skyscanner_bidding.union_group%s`
)
PIVOT (
  SUM(bid) FOR campaign_name IN (
    'AE-APP'
    , 'AE-APP-GROUP'
    , 'AE-DESKTOP'
    , 'AE-DESKTOP-GROUP'
    , 'AE-MWEB'
    , 'AE-MWEB-GROUP'
    , 'BH-APP'
    , 'BH-APP-GROUP'
    , 'BH-DESKTOP'
    , 'BH-DESKTOP-GROUP'
    , 'BH-MWEB'
    , 'BH-MWEB-GROUP'
    , 'CA-APP'
    , 'CA-APP-GROUP'
    , 'CA-DESKTOP'
    , 'CA-DESKTOP-GROUP'
    , 'CA-MWEB'
    , 'CA-MWEB-GROUP'
    , 'UK-APP'
    , 'UK-APP-GROUP'
    , 'UK-DESKTOP'
    , 'UK-DESKTOP-GROUP'
    , 'UK-MWEB'
    , 'UK-MWEB-GROUP'
    , 'JO-APP'
    , 'JO-APP-GROUP'
    , 'JO-DESKTOP'
    , 'JO-DESKTOP-GROUP'
    , 'JO-MWEB'
    , 'JO-MWEB-GROUP'
    , 'KW-APP'
    , 'KW-APP-GROUP'
    , 'KW-DESKTOP'
    , 'KW-DESKTOP-GROUP'
    , 'KW-MWEB'
    , 'KW-MWEB-GROUP'
    , 'OM-APP'
    , 'OM-APP-GROUP'
    , 'OM-DESKTOP'
    , 'OM-DESKTOP-GROUP'
    , 'OM-MWEB'
    , 'OM-MWEB-GROUP'
    , 'QA-APP'
    , 'QA-APP-GROUP'
    , 'QA-DESKTOP'
    , 'QA-DESKTOP-GROUP'
    , 'QA-MWEB'
    , 'QA-MWEB-GROUP'
    , 'SA-APP'
    , 'SA-APP-GROUP'
    , 'SA-DESKTOP'
    , 'SA-DESKTOP-GROUP'
    , 'SA-MWEB'
    , 'SA-MWEB-GROUP'
    , 'US-APP'
    , 'US-APP-GROUP'
    , 'US-DESKTOP'
    , 'US-DESKTOP-GROUP'
    , 'US-MWEB'
    , 'US-MWEB-GROUP'
))
)
"""
,date_shard_suffix,date_shard_suffix
);

------------------------ V1 PIPELINE ------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_skyscanner_raw_data%s AS (
WITH hotel_details AS
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
provider_count,
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
COUNT(DISTINCT provider_id) as provider_count,
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
on h.property_type_id = property_type.property_type_id),

wego_historical_performance AS 
(SELECT 
* EXCEPT (
  total_clicks, 
  total_conversions, 
  total_clicked_sessions, 
  total_converted_sessions, 
  total_gmv, 
  total_impressions, 
  total_bookings, 
  row_number
),

-- 1-month aliases (current month values)
total_clicks AS clicks_1mth,
total_conversions AS conversions_1mth,
total_clicked_sessions AS clicked_sessions_1mth,
total_converted_sessions AS converted_sessions_1mth,
total_gmv AS gmv_1mth,
total_impressions AS impressions_1mth,
total_bookings AS bookings_1mth,

-- CTRs
ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS ctr_1mth,
ROUND(SAFE_DIVIDE(clicks_2mth, impressions_2mth) * 100, 2) AS ctr_2mth,
ROUND(SAFE_DIVIDE(clicks_3mth, impressions_3mth) * 100, 2) AS ctr_3mth,
ROUND(SAFE_DIVIDE(clicks_6mth, impressions_6mth) * 100, 2) AS ctr_6mth,
ROUND(SAFE_DIVIDE(clicks_12mth, impressions_12mth) * 100, 2) AS ctr_12mth,

-- CVRs
ROUND(SAFE_DIVIDE(total_conversions, total_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, clicks_12mth) * 100, 2) AS cvr_12mth,

-- Converted session rates
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
  total_bookings,

  -- 2-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicked_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS impressions_2mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS bookings_2mth,

  -- 3-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicked_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS impressions_3mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS bookings_3mth,

  -- 6-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicked_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS impressions_6mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS bookings_6mth,

  -- 12-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicked_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS impressions_12mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS bookings_12mth,

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
    (SELECT FORMAT_DATE('%%Y-%%m', month_list) AS month
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH),DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),INTERVAL 1 MONTH)) AS month_list
    )
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
    WHERE DATE(_PARTITIONDATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    GROUP BY 1,2) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT
    CAST(hotel_id AS INT64) as hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', search_created_at) AS month, 
    COUNT(DISTINCT search_id) as total_impressions
    FROM `wego_analytics.hotels_impressions*`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) AND FORMAT_DATE('%%Y%%m%%d', CURRENT_DATE())
    GROUP BY 1,2) as c

    ON a.hotel_id = c.hotel_id AND a.month = c.month

    LEFT JOIN

    (SELECT hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    -- COUNT(DISTINCT IF(attribution_ts_code = '6be92',booking_id, NULL)) as total_skyscanner_bookings,
    -- SUM(IF(attribution_ts_code = '6be92',wego_total_price_usd, NULL)) as total_skyscanner_gmv,
    COUNT(DISTINCT booking_id) as total_bookings
    FROM `wego-cloud.wego_analytics.hotels_bookings` 
    WHERE DATE(created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND attribution_ts_code IS NULL
    GROUP BY 1,2) as d

    ON a.hotel_id = d.hotel_id AND a.month = d.month
    )
  )
  WHERE row_number = 1),

skyscanner_historical_performance AS

(SELECT 
campaign_name, 
partner_property_id, 
date, 
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
SAFE_DIVIDE(main_display_hotel_impressions_cumulative,hotel_impressions_cumulative) AS main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
SAFE_DIVIDE(display_rank_impressions_cumulative,hotel_impressions_cumulative) AS avg_display_rank_cumulative,
SAFE_DIVIDE(price_impressions_cumulative,hotel_impressions_cumulative) AS avg_price_cumulative,
SAFE_DIVIDE(absolute_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS absolute_price_difference_cumulative,
SAFE_DIVIDE(pct_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS pct_price_difference_cumulative,
SAFE_DIVIDE(beat_pricing_impressions_cumulative,meet_pricing_impressions_cumulative) AS beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
SAFE_DIVIDE(bid_average_gbp_pence_impressions_cumulative,hotel_impressions_cumulative) AS avg_bid_average_gbp_pence_cumulative
FROM
  (SELECT 
  a.campaign_name, 
  a.partner_property_id, 
  a.date, 
  -- clicks as skyscanner_clicks, 
  -- total_bookings as skyscanner_bookings,
  -- hotel_impressions,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  ROW_NUMBER() OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date) AS date_ranking,
  SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_clicks_cumulative,
  SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_bookings_cumulative,
  SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hotel_impressions_cumulative,
  SUM(main_display_hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS main_display_hotel_impressions_cumulative,
  SUM(cost_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cost_gbp_pence_cumulative,
  SUM(beat_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS beat_pricing_impressions_cumulative,
  SUM(meet_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS meet_pricing_impressions_cumulative,
  SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS display_rank_impressions_cumulative,
  SUM(price_impressions_cumulative) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS price_impressions_cumulative,
  SUM(absolute_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS absolute_price_difference_impressions_cumulative,
  SUM(pct_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pct_price_difference_impressions_cumulative,
  MIN(min_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS min_bid_average_gbp_pence_cumulative,
  MAX(max_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_bid_average_gbp_pence_cumulative,
  SUM(bid_average_gbp_pence_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS bid_average_gbp_pence_impressions_cumulative,
  -- SAFE_DIVIDE(SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS avg_display_rank_cumulative,
  -- SAFE_DIVIDE(SUM(price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS price_difference_cumulative,
  -- SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_clicks_overall,
  -- SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_bookings_overall,
    FROM
    (SELECT 
    campaign_name, 
    partner_property_id, 
    date, 
    SUM(clicks) as clicks, 
    SUM(hotel_impressions) as hotel_impressions,
    SUM(CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN hotel_impressions
    WHEN display_rank <= 2 THEN hotel_impressions
    ELSE 0 END) AS  main_display_hotel_impressions,
    SUM(cost_gbp_pence) AS cost_gbp_pence,
    SUM(beat_pricing_impressions) AS beat_pricing_impressions,
    SUM(meet_pricing_impressions) AS meet_pricing_impressions,
    SUM(display_rank*hotel_impressions) AS display_rank_impressions,
    SUM((base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg) * hotel_impressions) AS price_impressions_cumulative,
    SUM(price_difference_gbp_pence_avg * hotel_impressions) AS absolute_price_difference_impressions,
    SUM(SAFE_DIVIDE(100*price_difference_gbp_pence_avg * hotel_impressions,base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) AS pct_price_difference_impressions,
    MIN(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS min_bid_average_gbp_pence,
    MAX(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS max_bid_average_gbp_pence,
    SUM(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) * hotel_impressions) as bid_average_gbp_pence_impressions
    FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
    WHERE display_rank > 0
    GROUP BY 1,2,3
    ) as a

    LEFT JOIN

    (SELECT date, campaign_name, partner_property_id, SUM(bow_conversion) AS total_bookings 
    FROM
      (SELECT date, redirect_id, campaign_name, partner_property_id
      FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
      GROUP BY 1,2,3,4) as a

      LEFT JOIN

      (SELECT 
      session_id, 
      REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
      FORMAT('%%s-%%s-%%s-%%s-%%s',
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
      ) AS decoded_redirect_id FROM 
      `wego-cloud.wego_analytics.sessions`
      WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
      AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
      ) as b

      ON a.redirect_id = b.decoded_redirect_id

      INNER JOIN

      (SELECT  
      session_id, 1 AS bow_conversion
      FROM `wego-cloud.wego_analytics.hotels_bookings` 
      WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
      and attribution_ts_code = '6be92' -- Skyscanner Bookings
      AND conversions_tracked > 0
      GROUP BY 1) as c

      ON b.session_id = c.session_id
    GROUP BY 1,2,3
    ) as b

    ON a.date = b.date AND a.campaign_name = b.campaign_name AND a.partner_property_id = b.partner_property_id

  )
WHERE date_ranking > 1
),


hotel_list as
(
  select * from(
    select
      partner_property_id
      , date_added
      , country_code
      , campaign_type
    from `wego-cloud.analysis.skyscanner_bid_hotel_list`
    where partner_property_id is not null
  --   union distinct 
  --   select
  --     cast(partner_property_id as string)
  --     , min(date) as date_added
  --     , cast(null as string) as country_code
  --     , 'international' as campaign_type
  --   from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
  --   where split(campaign_name, "-")[offset(0)] in (
  --     'AE','OM','KW','SA','QA','BH','JO','US','CA','UK'
  --     )
  --     and display_rank > 0
  --     and date >= date(current_date() - 7)
  --     and partner_property_id is not null
  -- group by 1, 3, 4
  )
  qualify row_number()over(partition by partner_property_id order by date_added desc) = 1
)

, campaign_list as
(SELECT
country_code,
CONCAT(country_code, '-', device) AS campaign_name
FROM
UNNEST(['OM', 'KW', 'SA', 'QA', 'AE', 'BH', 'JO', 'US', 'CA', 'UK']) AS country_code,
UNNEST(['MWEB', 'APP', 'DESKTOP']) AS device
)

, bid_list as
(
  select distinct
    partner_property_id
    , campaign_name
    , campaign_type
    , hl.country_code as hotel_country_code
    , cl.country_code as campaign_country_code
  from hotel_list hl
  cross join campaign_list cl
    -- on hl.country_code = 
)

, skyscanner as
(
  select 
    partner_property_id
    , campaign_name
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(0)] AS site_code
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(1)] AS device_type
    , hotel_impressions
    , price_difference_gbp_pence_avg
    , taxes_fees_gbp_pence_avg
    , base_price_gbp_pence_avg
    , bid_average_gbp_pence
    , los_multiplier
    , los
    , display_rank
    , CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN 1
      WHEN display_rank <= 2 THEN 1
      ELSE 0 END AS main_display_ind
    , cost_gbp_pence
    , clicks
    , guests
    , check_in_date
    , DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date
    , EXTRACT(YEAR FROM check_in_date) AS check_in_year
    , EXTRACT(MONTH FROM check_in_date) AS check_in_month
    , EXTRACT(DAY FROM check_in_date) AS check_in_day
    , DATE_DIFF(check_in_date, date, DAY) AS leadtime
    , dow
    , tod
    , date
    , EXTRACT(YEAR FROM date) AS created_at_year
    , EXTRACT(MONTH FROM date) AS created_at_month
    , EXTRACT(DAY FROM date) AS created_at_day
    , SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) as avg_bid_current
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg AS total_price_gbp_pence
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg AS next_cheapest_total_price_gbp_pence
    , ROUND(100*SAFE_DIVIDE(price_difference_gbp_pence_avg,base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg),2) AS next_cheapest_total_price_gbp_pence_pct
    , beat_pricing_impressions
    , meet_pricing_impressions
  from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
  left join `wego-cloud.hotel_services.hotels` as b 
    on cast(a.partner_property_id as string) = cast(b.id as string)
  -- where b.city_code in ("DXB", "DWC", "AUH", "SHJ", "RKT", "FJR", "AAN", "HKT", "BKK")
  -- and split(a.campaign_name, "-")[offset(0)] in ("AE", "OM","KW")
  WHERE display_rank > 0
  -- and date >= date(current_date() - 2) -- Take Everything
)

, skyscanner_bookings as 
(SELECT date, campaign_name, partner_property_id, check_in_date, check_out_date,
SUM(bow_conversions) AS bow_conversions, 
SUM(wego_markup_amount_usd) AS total_wego_markup_amount_usd,
SUM(wego_total_price_usd) AS total_wego_total_price_usd,
SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
SUM(revenue_in_usd) as total_revenue_usd 
FROM
  (SELECT date, redirect_id, campaign_name, partner_property_id, check_in_date, los, DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date,
  FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
  -- WHERE DATE(_PARTITIONTIME) BETWEEN "2025-04-25" AND "2025-09-11"
  GROUP BY 1,2,3,4,5,6) as a

  LEFT JOIN

  (SELECT 
  session_id, 
  REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
  FORMAT('%%s-%%s-%%s-%%s-%%s',
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
  ) AS decoded_redirect_id 
  FROM `wego-cloud.wego_analytics.sessions`
  WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
  AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
  ) as b

  ON a.redirect_id = b.decoded_redirect_id

  INNER JOIN

  (SELECT  
  session_id, DATE(check_in) as check_in, DATE(check_out) as check_out, trip_duration, 
  COUNT(DISTINCT booking_id) as bow_conversions,
  SUM(wego_base_price_usd) AS total_wego_base_price_usd,
  SUM(wego_markup_amount_usd) AS wego_markup_amount_usd,
  AVG(wego_total_price_usd) AS wego_total_price_usd,
  SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
  SUM(revenue_in_usd) AS revenue_in_usd
  FROM `wego-cloud.wego_analytics.hotels_bookings` 
  WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
  and attribution_ts_code = '6be92' -- Skyscanner Bookings
  AND conversions_tracked > 0
  GROUP BY 1,2,3,4) as c

  ON b.session_id = c.session_id
GROUP BY 1,2,3,4,5
)

-- , high_click as
-- (
--     select 
--         partner_property_id
--         , name_en
--         , sum(case when date >= (current_date - interval 14 day) then clicks else 0 end) as l14d_click
--         , sum(clicks) as total_click
--         , min(date) as first_live_date
--         , max(date) as last_click_date
--         , count(distinct date) as total_impression_date
--         , sum(hotel_impressions-avail_impressions_missed) as total_impression
--         , sum(cost_gbp_pence) as total_cost_gbp_pence
--     from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
--     left join `wego-cloud.hotel_services.hotels` as b 
--             on cast(a.partner_property_id as string) = cast(b.id as string)
--     where display_rank > 0
--     group by 1,2
--     having (l14d_click > 10
--     and date_diff(current_date,first_live_date,day) >= 14
--     and total_impression_date >= 14)
--     or total_click > 15
-- )

-- , bookings as
-- (
--     select
--       hotel_id
--       , count(distinct booking_id) as total_booking
--     from `wego-cloud.wego_analytics.hotels_bookings` 
--     where date(created_at) >= date(current_date - interval 14 day)
--     and attribution_ts_code = '6be92'
--     group by 1
-- )

-- , exclude_hotel as
-- (
--     select distinct
--         partner_property_id
--         , name_en
--         , first_live_date
--         , last_click_date
--         , total_click
--         , l14d_click
--         , total_impression
--         , total_cost_gbp_pence
--         , total_impression_date
--         , coalesce(total_booking,0) as total_booking
--     from high_click hc
--     left join bookings b
--         on cast(hc.partner_property_id as string) = cast(b.hotel_id as string)
--     where b.hotel_id is null
-- )

-- , property_stats as (
--   select 
--     campaign_name
--     , partner_property_id
--     , sum(display_rank * hotel_impressions) / sum(hotel_impressions) as display_rank
--     , sum((price_difference_gbp_pence_avg * hotel_impressions) 
--           / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
--         / sum(hotel_impressions) as avg_price_diff
--     , round(sum((bid_average_gbp_pence / (los * los_multiplier)) * hotel_impressions) 
--             / sum(hotel_impressions)) as avg_bid_current
--     , sum(hotel_impressions) as impressions
--     , sum(clicks) as clicks
--   from skyscanner
--   group by 1, 2
-- )

-- , prio_list as (
--   select 
--       *
--       , percent_rank()over(partition by campaign_name order by impressions) as impression_perc_rank
--   from (
--     select 
--         campaign_name
--       , partner_property_id
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) 
--           / sum(hotel_impressions) as perc_occurence
--       , sum(hotel_impressions) as impressions
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) avg_price_diff
--       , avg(display_rank) as avg_rank
--       , min(display_rank) as min_rank
--       , max(display_rank) as max_rank
--     from skyscanner
--     group by 1, 2
--   )
--   group by 1, 2, 3, 4, 5, 6, 7,8
-- )

SELECT 
b.partner_property_id as bid_list_partner_property_id,
b.campaign_name as bid_list_campaign_name,
b.campaign_type as bid_list_campaign_type,
b.hotel_country_code as bid_list_hotel_country_code,
b.campaign_country_code as bid_list_campaign_country_code,
IF(a.partner_property_id IS NOT NULL,1,0) AS historical_performance_ind, 
IF(b.partner_property_id IS NOT NULL,1,0) AS bid_list_ind, 
a.*,
hotel_details.*, 
wego_historical_performance.* EXCEPT (hotel_id),
skyscanner_historical_performance.* EXCEPT (partner_property_id, campaign_name, date),
sb.* EXCEPT (date, partner_property_id, campaign_name, check_in_date, check_out_date)
FROM bid_list as b
FULL OUTER JOIN skyscanner as a
ON b.partner_property_id = a.partner_property_id AND b.campaign_name = a.campaign_name
LEFT JOIN skyscanner_bookings as sb
ON sb.date = a.date AND sb.campaign_name = a.campaign_name AND sb.check_in_date = a.check_in_date AND sb.check_out_date = a.check_out_date AND sb.partner_property_id = a.partner_property_id
-- LEFT JOIN exclude_hotel c
-- on b.partner_property_id = c.partner_property_id
LEFT JOIN hotel_details
ON COALESCE(a.partner_property_id,b.partner_property_id) = hotel_details.hotel_id
LEFT JOIN wego_historical_performance
ON COALESCE(a.partner_property_id,b.partner_property_id) = wego_historical_performance.hotel_id
LEFT JOIN skyscanner_historical_performance
ON a.partner_property_id = skyscanner_historical_performance.partner_property_id 
AND a.campaign_name = skyscanner_historical_performance.campaign_name 
AND a.date = skyscanner_historical_performance.date 
-- WHERE c.partner_property_id is null
)
"""
,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_training_data%s AS (
SELECT * FROM skyscanner_bidding.v1_skyscanner_raw_data%s
WHERE historical_performance_ind = 1 
)
"""
,date_shard_suffix,date_shard_suffix
);

-- Check Ratio of Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
  SELECT SAFE_DIVIDE(not_null_cases, null_cases)
  FROM (
    SELECT 
    COUNTIF(clicks = 0) AS null_cases,
    COUNTIF(clicks > 0) AS not_null_cases
    FROM `wego-cloud.skyscanner_bidding.v1_training_data%s`
  )
"""
,date_shard_suffix)
INTO null_proportion;

---- Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_training_data_downsample%s AS (
SELECT *, 
-- CASE WHEN PERCENT_RANK() OVER (ORDER BY date) <= 0.8 THEN 'TRAIN'
-- ELSE 'TEST' END AS train_test_split
FROM
  (SELECT * FROM `wego-cloud.skyscanner_bidding.v1_training_data%s`
  WHERE clicks = 0
  AND RAND() < @null_proportion -- * 0.2/(1-0.2)

  UNION ALL 

  SELECT * FROM `wego-cloud.skyscanner_bidding.v1_training_data%s`
  WHERE clicks > 0
  ) 
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix)
USING null_proportion as null_proportion;

----- Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `skyscanner_bidding.v1_rf_model%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['clicks'],
  DATA_SPLIT_METHOD = 'SEQ',
  DATA_SPLIT_COL = 'date'
  ) AS
SELECT
date,
-- hotel_name,
-- chain_id,
chain_name,
-- brand_id,
brand_name,
-- country,
country_code,
-- location,
location_code,
-- location_id,
-- district_id,
district_name,
hotel_star_rating,
image_count,
overall_score,
reviews_count,
distance_to_city_centre,
-- property_type_id,
property_type_name,
-- built_year,
-- renovated_year,
-- booking_avg_reviews_score,
-- booking_reviews_count,
-- booking_0_5_reviews_count,
-- booking_5_6_reviews_count,
-- booking_6_7_reviews_count,
-- booking_7_8_reviews_count,
-- booking_8_9_reviews_count,
-- booking_9_10_reviews_count,
provider_count,
clicks_1mth,
conversions_1mth,
-- clicked_sessions_1mth,
-- converted_sessions_1mth,
gmv_1mth,
impressions_1mth,
bookings_1mth,
-- conversions_2mth,
-- clicks_2mth,
-- converted_sessions_2mth,
-- clicked_sessions_2mth,
-- gmv_2mth,
-- impressions_2mth,
-- bookings_2mth,
-- conversions_3mth,
-- clicks_3mth,
-- converted_sessions_3mth,
-- clicked_sessions_3mth,
-- gmv_3mth,
-- impressions_3mth,
-- bookings_3mth,
conversions_6mth,
clicks_6mth,
-- converted_sessions_6mth,
-- clicked_sessions_6mth,
gmv_6mth,
impressions_6mth,
bookings_6mth,
conversions_12mth,
clicks_12mth,
-- converted_sessions_12mth,
-- clicked_sessions_12mth,
gmv_12mth,
impressions_12mth,
bookings_12mth,
ctr_1mth,
-- ctr_2mth,
-- ctr_3mth,
ctr_6mth,
ctr_12mth,
cvr_1mth,
-- cvr_2mth,
-- cvr_3mth,
cvr_6mth,
cvr_12mth,
-- converted_sessions_rate_1mth,
-- converted_sessions_rate_2mth,
-- converted_sessions_rate_3mth,
-- converted_sessions_rate_6mth,
-- converted_sessions_rate_12mth,
site_code,
device_type,
los,
guests,
created_at_year,
created_at_month,
created_at_day,
check_in_year,
check_in_month,
check_in_day,
leadtime,
dow,
tod,
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
avg_display_rank_cumulative,
avg_price_cumulative,
absolute_price_difference_cumulative,
pct_price_difference_cumulative,
beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
avg_bid_average_gbp_pence_cumulative,
display_rank,
main_display_ind,
price_difference_gbp_pence_avg,
total_price_gbp_pence,
next_cheapest_total_price_gbp_pence,
next_cheapest_total_price_gbp_pence_pct,
-- cost_gbp_pence, -- Might be reflective of clicks, giving the answer
-- beat_pricing_impressions,
-- meet_pricing_impressions,
avg_bid_current, 
IFNULL(clicks,0) as clicks,
-- train_test_split
FROM `skyscanner_bidding.v1_training_data_downsample%s`
"""
,date_shard_suffix,date_shard_suffix
);


----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_rf_model_evaluation%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`skyscanner_bidding.v1_rf_model%s`))
"""
,date_shard_suffix,date_shard_suffix
);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_rf_model_feature_importance%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `skyscanner_bidding.v1_rf_model%s`)
ORDER BY importance_gain DESC
)
"""
,date_shard_suffix,date_shard_suffix
);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_rf_model_global_explain%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `skyscanner_bidding.v1_rf_model%s`)
)
"""
,date_shard_suffix,date_shard_suffix
);

--------- GENERATE MOCK BIDS PER CAMPAIGN, HOTEL ID COMBINATION TO PREDICT AGAINST MODEL ---------
-- Generate Array of Bids from 20 to 135, in steps of 5 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_bid_template%s AS (
SELECT *
FROM
  (SELECT 
  campaign_name,
  partner_property_id,
  hotel_name,
  -- Model Features --
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  -- clicked_sessions_1mth,
  -- converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  -- conversions_2mth,
  -- clicks_2mth,
  -- converted_sessions_2mth,
  -- clicked_sessions_2mth,
  -- gmv_2mth,
  -- impressions_2mth,
  -- bookings_2mth,
  -- conversions_3mth,
  -- clicks_3mth,
  -- converted_sessions_3mth,
  -- clicked_sessions_3mth,
  -- gmv_3mth,
  -- impressions_3mth,
  -- bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  -- converted_sessions_6mth,
  -- clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  -- converted_sessions_12mth,
  -- clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  -- ctr_2mth,
  -- ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  -- cvr_2mth,
  -- cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  -- converted_sessions_rate_1mth,
  -- converted_sessions_rate_2mth,
  -- converted_sessions_rate_3mth,
  -- converted_sessions_rate_6mth,
  -- converted_sessions_rate_12mth, 
  -- If it's part of a bid list, but there's no historical skyscanner performance data, take the global average to use as a basis to predict against the model
  a.site_code,
  a.device_type,
  IF(historical_performance_ind = 1, a.los, b.los) AS los,
  IF(historical_performance_ind = 1, a.guests, b.guests) AS guests,
  IF(historical_performance_ind = 1, a.created_at_year, b.created_at_year) AS created_at_year,
  IF(historical_performance_ind = 1, a.created_at_month, b.created_at_month) AS created_at_month,
  IF(historical_performance_ind = 1, a.created_at_day, b.created_at_day) AS created_at_day,
  IF(historical_performance_ind = 1, a.check_in_year, b.check_in_year) AS check_in_year,
  IF(historical_performance_ind = 1, a.check_in_month, b.check_in_month) AS check_in_month,
  IF(historical_performance_ind = 1, a.check_in_day, b.check_in_day) AS check_in_day,
  IF(historical_performance_ind = 1, a.leadtime, b.leadtime) AS leadtime,
  IF(historical_performance_ind = 1, a.dow, b.dow) AS dow,
  IF(historical_performance_ind = 1, a.tod, b.tod) AS tod,
  IF(historical_performance_ind = 1, a.skyscanner_clicks_cumulative, b.skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
  IF(historical_performance_ind = 1, a.skyscanner_bookings_cumulative, b.skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
  IF(historical_performance_ind = 1, a.hotel_impressions_cumulative, b.hotel_impressions_cumulative) AS hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_hotel_impressions_cumulative, b.main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_ratio_cumulative, b.main_display_ratio_cumulative) AS main_display_ratio_cumulative,
  IF(historical_performance_ind = 1, a.cost_gbp_pence_cumulative, b.cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.beat_pricing_impressions_cumulative, b.beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.meet_pricing_impressions_cumulative, b.meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.avg_display_rank_cumulative, b.avg_display_rank_cumulative) AS avg_display_rank_cumulative,
  IF(historical_performance_ind = 1, a.avg_price_cumulative, b.avg_price_cumulative) AS avg_price_cumulative,
  IF(historical_performance_ind = 1, a.absolute_price_difference_cumulative, b.absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.pct_price_difference_cumulative, b.pct_price_difference_cumulative) AS pct_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.beat_meet_pricing_ratio_cumulative, b.beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
  IF(historical_performance_ind = 1, a.min_bid_average_gbp_pence_cumulative, b.min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.max_bid_average_gbp_pence_cumulative, b.max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.avg_bid_average_gbp_pence_cumulative, b.avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.display_rank, b.display_rank) AS display_rank,
  IF(historical_performance_ind = 1, a.main_display_ind, b.main_display_ind) AS main_display_ind,
  IF(historical_performance_ind = 1, a.price_difference_gbp_pence_avg, b.price_difference_gbp_pence_avg) AS price_difference_gbp_pence_avg,
  IF(historical_performance_ind = 1, a.total_price_gbp_pence, b.total_price_gbp_pence) AS total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence, b.next_cheapest_total_price_gbp_pence) AS next_cheapest_total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence_pct, b.next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct
  FROM
    (SELECT 
    bid_list_campaign_name AS campaign_name,
    bid_list_partner_property_id AS partner_property_id,
    hotel_name,
    -- Model Features --
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(0)], campaign_name) AS site_code,
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(1)], campaign_name) AS device_type,
    chain_name,
    brand_name,
    country_code,
    location_code,
    district_name,
    hotel_star_rating,
    image_count,
    overall_score,
    reviews_count,
    distance_to_city_centre,
    property_type_name,
    provider_count,
    clicks_1mth,
    conversions_1mth,
    -- clicked_sessions_1mth,
    -- converted_sessions_1mth,
    gmv_1mth,
    impressions_1mth,
    bookings_1mth,
    -- conversions_2mth,
    -- clicks_2mth,
    -- converted_sessions_2mth,
    -- clicked_sessions_2mth,
    -- gmv_2mth,
    -- impressions_2mth,
    -- bookings_2mth,
    -- conversions_3mth,
    -- clicks_3mth,
    -- converted_sessions_3mth,
    -- clicked_sessions_3mth,
    -- gmv_3mth,
    -- impressions_3mth,
    -- bookings_3mth,
    conversions_6mth,
    clicks_6mth,
    -- converted_sessions_6mth,
    -- clicked_sessions_6mth,
    gmv_6mth,
    impressions_6mth,
    bookings_6mth,
    conversions_12mth,
    clicks_12mth,
    -- converted_sessions_12mth,
    -- clicked_sessions_12mth,
    gmv_12mth,
    impressions_12mth,
    bookings_12mth,
    ctr_1mth,
    -- ctr_2mth,
    -- ctr_3mth,
    ctr_6mth,
    ctr_12mth,
    cvr_1mth,
    -- cvr_2mth,
    -- cvr_3mth,
    cvr_6mth,
    cvr_12mth,
    -- converted_sessions_rate_1mth,
    -- converted_sessions_rate_2mth,
    -- converted_sessions_rate_3mth,
    -- converted_sessions_rate_6mth,
    -- converted_sessions_rate_12mth,
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    -- Check Indicator of Whether There was Historical Skyscanner Performance
    MAX(historical_performance_ind) AS historical_performance_ind,
    FROM `skyscanner_bidding.v1_skyscanner_raw_data%s`
    WHERE bid_list_ind = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38) as a

    CROSS JOIN

    -- Generate One Row of a Global Dummy Average for Hotels without any Historical Performance
    (SELECT 
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    FROM `skyscanner_bidding.v1_skyscanner_raw_data%s`
    WHERE historical_performance_ind = 1) as b 
  ),UNNEST(GENERATE_ARRAY(20,135,5)) as mock_bid
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

-- -- --------- GET PREDICITONS FROM MODEL ---------
--- For each hotel, put in a list of mock bids to predict with the model, then rank by best click prediction 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_training_data_explain_predict%s AS (
SELECT *,
DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value DESC) as prediction_ranking,
-- Less Precise on the Decimal Places of Prediction Value (So that more consideration among lower bid higher predicted click cases)
-- DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value_exponent DESC, ROUND(prediction_value_mantissa,1) DESC) as prediction_ranking_blunt, 
FROM
  (SELECT
  -- input.*,
  -- predoutput.*
  predoutput.* ,
  -- predoutput.predicted_total_itinerary_card_clicks,
  -- predoutput.total_itinerary_card_clicks_probs
  FLOOR(LOG10(predoutput.prediction_value)) AS prediction_value_exponent,
  SAFE_DIVIDE(predoutput.prediction_value,POWER(10, FLOOR(LOG10(predoutput.prediction_value)))) AS prediction_value_mantissa
  FROM
  ML.EXPLAIN_PREDICT(MODEL `skyscanner_bidding.v1_rf_model%s`,
  (
  SELECT
  campaign_name,
  partner_property_id,
  hotel_name,
  -- Model Features --
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  -- clicked_sessions_1mth,
  -- converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  -- conversions_2mth,
  -- clicks_2mth,
  -- converted_sessions_2mth,
  -- clicked_sessions_2mth,
  -- gmv_2mth,
  -- impressions_2mth,
  -- bookings_2mth,
  -- conversions_3mth,
  -- clicks_3mth,
  -- converted_sessions_3mth,
  -- clicked_sessions_3mth,
  -- gmv_3mth,
  -- impressions_3mth,
  -- bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  -- converted_sessions_6mth,
  -- clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  -- converted_sessions_12mth,
  -- clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  -- ctr_2mth,
  -- ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  -- cvr_2mth,
  -- cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  -- converted_sessions_rate_1mth,
  -- converted_sessions_rate_2mth,
  -- converted_sessions_rate_3mth,
  -- converted_sessions_rate_6mth,
  -- converted_sessions_rate_12mth,
  site_code,
  device_type,
  los,
  guests,
  created_at_year,
  created_at_month,
  created_at_day,
  check_in_year,
  check_in_month,
  check_in_day,
  leadtime,
  dow,
  tod,
  skyscanner_clicks_cumulative,
  skyscanner_bookings_cumulative,
  hotel_impressions_cumulative,
  main_display_hotel_impressions_cumulative,
  main_display_ratio_cumulative,
  cost_gbp_pence_cumulative,
  beat_pricing_impressions_cumulative,
  meet_pricing_impressions_cumulative,
  avg_display_rank_cumulative,
  avg_price_cumulative,
  absolute_price_difference_cumulative,
  pct_price_difference_cumulative,
  beat_meet_pricing_ratio_cumulative,
  min_bid_average_gbp_pence_cumulative,
  max_bid_average_gbp_pence_cumulative,
  avg_bid_average_gbp_pence_cumulative,
  display_rank,
  main_display_ind,
  price_difference_gbp_pence_avg,
  total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence_pct,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  mock_bid as avg_bid_current,
  FROM `skyscanner_bidding.v1_bid_template%s`
  -- WHERE campaign_name = "AE-APP"	
  -- AND partner_property_id = "3051914"
  ),
  STRUCT(TRUE AS approx_feature_contrib))
  predoutput 
  -- join `skyscanner_bidding.v1_bid_template%s` input 
  -- ON predoutput.campaign_name=input.campaign_name 
  -- AND predoutput.partner_property_id=input.partner_property_id
  -- AND predoutput.avg_bid_current=input.avg_bid_current
)
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);


-- -- --------- SELECT BEST BID BASED ON HIGHEST PREDICTED VALUE (Clicks) ---------
--- Based on each click prediction from the model, if there's a tie for 1st place, take the lowest bid needed to get a max predicted clicks
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_bid_selection%s AS (
SELECT *, LEAST(avg_bid_current,135) as bid,
FROM
  (SELECT campaign_name, partner_property_id, MIN(avg_bid_current) as avg_bid_current
  FROM `skyscanner_bidding.v1_training_data_explain_predict%s`
  WHERE prediction_ranking = 1
  GROUP BY 1,2)
)
"""
,date_shard_suffix,date_shard_suffix
);


EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_union_group%s AS (
SELECT
  partner_property_id
  , campaign_name
  , least(bid,135) as bid
FROM `skyscanner_bidding.v1_bid_selection%s`
UNION ALL
SELECT 
  b.partner_property_id, 
  a.campaign_name, 
  1 AS bid 
FROM (
  SELECT DISTINCT CONCAT(campaign_name, "-", "GROUP") AS campaign_name 
  FROM `skyscanner_bidding.v1_bid_selection%s`
) AS a
JOIN (
  SELECT DISTINCT partner_property_id 
  FROM `skyscanner_bidding.v1_bid_selection%s`
) AS b ON 1=1
ORDER BY campaign_name, partner_property_id
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v1_bid_file%s AS (
SELECT *
FROM (
  SELECT
    partner_property_id as `Hotel ID`,
    campaign_name,
    bid
  FROM `skyscanner_bidding.v1_union_group%s`
)
PIVOT (
  SUM(bid) FOR campaign_name IN (
    'AE-APP'
    , 'AE-APP-GROUP'
    , 'AE-DESKTOP'
    , 'AE-DESKTOP-GROUP'
    , 'AE-MWEB'
    , 'AE-MWEB-GROUP'
    , 'BH-APP'
    , 'BH-APP-GROUP'
    , 'BH-DESKTOP'
    , 'BH-DESKTOP-GROUP'
    , 'BH-MWEB'
    , 'BH-MWEB-GROUP'
    , 'CA-APP'
    , 'CA-APP-GROUP'
    , 'CA-DESKTOP'
    , 'CA-DESKTOP-GROUP'
    , 'CA-MWEB'
    , 'CA-MWEB-GROUP'
    , 'UK-APP'
    , 'UK-APP-GROUP'
    , 'UK-DESKTOP'
    , 'UK-DESKTOP-GROUP'
    , 'UK-MWEB'
    , 'UK-MWEB-GROUP'
    , 'JO-APP'
    , 'JO-APP-GROUP'
    , 'JO-DESKTOP'
    , 'JO-DESKTOP-GROUP'
    , 'JO-MWEB'
    , 'JO-MWEB-GROUP'
    , 'KW-APP'
    , 'KW-APP-GROUP'
    , 'KW-DESKTOP'
    , 'KW-DESKTOP-GROUP'
    , 'KW-MWEB'
    , 'KW-MWEB-GROUP'
    , 'OM-APP'
    , 'OM-APP-GROUP'
    , 'OM-DESKTOP'
    , 'OM-DESKTOP-GROUP'
    , 'OM-MWEB'
    , 'OM-MWEB-GROUP'
    , 'QA-APP'
    , 'QA-APP-GROUP'
    , 'QA-DESKTOP'
    , 'QA-DESKTOP-GROUP'
    , 'QA-MWEB'
    , 'QA-MWEB-GROUP'
    , 'SA-APP'
    , 'SA-APP-GROUP'
    , 'SA-DESKTOP'
    , 'SA-DESKTOP-GROUP'
    , 'SA-MWEB'
    , 'SA-MWEB-GROUP'
    , 'US-APP'
    , 'US-APP-GROUP'
    , 'US-DESKTOP'
    , 'US-DESKTOP-GROUP'
    , 'US-MWEB'
    , 'US-MWEB-GROUP'
))
)
"""
,date_shard_suffix,date_shard_suffix
);

------------------------ V2 PIPELINE ------------------------
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_skyscanner_raw_data%s AS (
WITH hotel_details AS
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
provider_count,
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
COUNT(DISTINCT provider_id) as provider_count,
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
on h.property_type_id = property_type.property_type_id),

wego_historical_performance AS 
(SELECT 
* EXCEPT (
  total_clicks, 
  total_conversions, 
  total_clicked_sessions, 
  total_converted_sessions, 
  total_gmv, 
  total_impressions, 
  total_bookings, 
  row_number
),

-- 1-month aliases (current month values)
total_clicks AS clicks_1mth,
total_conversions AS conversions_1mth,
total_clicked_sessions AS clicked_sessions_1mth,
total_converted_sessions AS converted_sessions_1mth,
total_gmv AS gmv_1mth,
total_impressions AS impressions_1mth,
total_bookings AS bookings_1mth,

-- CTRs
ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS ctr_1mth,
ROUND(SAFE_DIVIDE(clicks_2mth, impressions_2mth) * 100, 2) AS ctr_2mth,
ROUND(SAFE_DIVIDE(clicks_3mth, impressions_3mth) * 100, 2) AS ctr_3mth,
ROUND(SAFE_DIVIDE(clicks_6mth, impressions_6mth) * 100, 2) AS ctr_6mth,
ROUND(SAFE_DIVIDE(clicks_12mth, impressions_12mth) * 100, 2) AS ctr_12mth,

-- CVRs
ROUND(SAFE_DIVIDE(total_conversions, total_clicks) * 100, 2) AS cvr_1mth,
ROUND(SAFE_DIVIDE(conversions_2mth, clicks_2mth) * 100, 2) AS cvr_2mth,
ROUND(SAFE_DIVIDE(conversions_3mth, clicks_3mth) * 100, 2) AS cvr_3mth,
ROUND(SAFE_DIVIDE(conversions_6mth, clicks_6mth) * 100, 2) AS cvr_6mth,
ROUND(SAFE_DIVIDE(conversions_12mth, clicks_12mth) * 100, 2) AS cvr_12mth,

-- Converted session rates
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
  total_bookings,

  -- 2-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicks_2mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicked_sessions_2mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS impressions_2mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS bookings_2mth,

  -- 3-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicks_3mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicked_sessions_3mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS impressions_3mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS bookings_3mth,

  -- 6-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicks_6mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicked_sessions_6mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS impressions_6mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS bookings_6mth,

  -- 12-month window
  SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
  SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicks_12mth,
  SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
  SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicked_sessions_12mth,
  SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
  SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS impressions_12mth,
  SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS bookings_12mth,

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
    (SELECT FORMAT_DATE('%%Y-%%m', month_list) AS month
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH),DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),INTERVAL 1 MONTH)) AS month_list
    )
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
    WHERE DATE(_PARTITIONDATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    GROUP BY 1,2) as b
    ON a.hotel_id = b.hotel_id AND a.month = b.month

    LEFT JOIN

    (SELECT
    CAST(hotel_id AS INT64) as hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', search_created_at) AS month, 
    COUNT(DISTINCT search_id) as total_impressions
    FROM `wego_analytics.hotels_impressions*`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) AND FORMAT_DATE('%%Y%%m%%d', CURRENT_DATE())
    GROUP BY 1,2) as c

    ON a.hotel_id = c.hotel_id AND a.month = c.month

    LEFT JOIN

    (SELECT hotel_id,
    FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
    -- COUNT(DISTINCT IF(attribution_ts_code = '6be92',booking_id, NULL)) as total_skyscanner_bookings,
    -- SUM(IF(attribution_ts_code = '6be92',wego_total_price_usd, NULL)) as total_skyscanner_gmv,
    COUNT(DISTINCT booking_id) as total_bookings
    FROM `wego-cloud.wego_analytics.hotels_bookings` 
    WHERE DATE(created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
    AND attribution_ts_code IS NULL
    GROUP BY 1,2) as d

    ON a.hotel_id = d.hotel_id AND a.month = d.month
    )
  )
  WHERE row_number = 1),

skyscanner_historical_performance AS

(SELECT 
campaign_name, 
partner_property_id, 
date, 
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
SAFE_DIVIDE(main_display_hotel_impressions_cumulative,hotel_impressions_cumulative) AS main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
SAFE_DIVIDE(display_rank_impressions_cumulative,hotel_impressions_cumulative) AS avg_display_rank_cumulative,
SAFE_DIVIDE(price_impressions_cumulative,hotel_impressions_cumulative) AS avg_price_cumulative,
SAFE_DIVIDE(absolute_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS absolute_price_difference_cumulative,
SAFE_DIVIDE(pct_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS pct_price_difference_cumulative,
SAFE_DIVIDE(beat_pricing_impressions_cumulative,meet_pricing_impressions_cumulative) AS beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
SAFE_DIVIDE(bid_average_gbp_pence_impressions_cumulative,hotel_impressions_cumulative) AS avg_bid_average_gbp_pence_cumulative
FROM
  (SELECT 
  a.campaign_name, 
  a.partner_property_id, 
  a.date, 
  -- clicks as skyscanner_clicks, 
  -- total_bookings as skyscanner_bookings,
  -- hotel_impressions,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  ROW_NUMBER() OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date) AS date_ranking,
  SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_clicks_cumulative,
  SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_bookings_cumulative,
  SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hotel_impressions_cumulative,
  SUM(main_display_hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS main_display_hotel_impressions_cumulative,
  SUM(cost_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cost_gbp_pence_cumulative,
  SUM(beat_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS beat_pricing_impressions_cumulative,
  SUM(meet_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS meet_pricing_impressions_cumulative,
  SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS display_rank_impressions_cumulative,
  SUM(price_impressions_cumulative) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS price_impressions_cumulative,
  SUM(absolute_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS absolute_price_difference_impressions_cumulative,
  SUM(pct_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pct_price_difference_impressions_cumulative,
  MIN(min_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS min_bid_average_gbp_pence_cumulative,
  MAX(max_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_bid_average_gbp_pence_cumulative,
  SUM(bid_average_gbp_pence_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS bid_average_gbp_pence_impressions_cumulative,
  -- SAFE_DIVIDE(SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS avg_display_rank_cumulative,
  -- SAFE_DIVIDE(SUM(price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS price_difference_cumulative,
  -- SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_clicks_overall,
  -- SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_bookings_overall,
    FROM
    (SELECT 
    campaign_name, 
    partner_property_id, 
    date, 
    SUM(clicks) as clicks, 
    SUM(hotel_impressions) as hotel_impressions,
    SUM(CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN hotel_impressions
    WHEN display_rank <= 2 THEN hotel_impressions
    ELSE 0 END) AS  main_display_hotel_impressions,
    SUM(cost_gbp_pence) AS cost_gbp_pence,
    SUM(beat_pricing_impressions) AS beat_pricing_impressions,
    SUM(meet_pricing_impressions) AS meet_pricing_impressions,
    SUM(display_rank*hotel_impressions) AS display_rank_impressions,
    SUM((base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg) * hotel_impressions) AS price_impressions_cumulative,
    SUM(price_difference_gbp_pence_avg * hotel_impressions) AS absolute_price_difference_impressions,
    SUM(SAFE_DIVIDE(100*price_difference_gbp_pence_avg * hotel_impressions,base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) AS pct_price_difference_impressions,
    MIN(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS min_bid_average_gbp_pence,
    MAX(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS max_bid_average_gbp_pence,
    SUM(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) * hotel_impressions) as bid_average_gbp_pence_impressions
    FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
    WHERE display_rank > 0
    GROUP BY 1,2,3
    ) as a

    LEFT JOIN

    (SELECT date, campaign_name, partner_property_id, SUM(bow_conversion) AS total_bookings 
    FROM
      (SELECT date, redirect_id, campaign_name, partner_property_id
      FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
      GROUP BY 1,2,3,4) as a

      LEFT JOIN

      (SELECT 
      session_id, 
      REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
      FORMAT('%%s-%%s-%%s-%%s-%%s',
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
      SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
      ) AS decoded_redirect_id FROM 
      `wego-cloud.wego_analytics.sessions`
      WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
      AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
      ) as b

      ON a.redirect_id = b.decoded_redirect_id

      INNER JOIN

      (SELECT  
      session_id, 1 AS bow_conversion
      FROM `wego-cloud.wego_analytics.hotels_bookings` 
      WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
      and attribution_ts_code = '6be92' -- Skyscanner Bookings
      AND conversions_tracked > 0
      GROUP BY 1) as c

      ON b.session_id = c.session_id
    GROUP BY 1,2,3
    ) as b

    ON a.date = b.date AND a.campaign_name = b.campaign_name AND a.partner_property_id = b.partner_property_id

  )
WHERE date_ranking > 1
),


hotel_list as
(
  select * from(
    select
      partner_property_id
      , date_added
      , country_code
      , campaign_type
    from `wego-cloud.analysis.skyscanner_bid_hotel_list`
    where partner_property_id is not null
  --   union distinct 
  --   select
  --     cast(partner_property_id as string)
  --     , min(date) as date_added
  --     , cast(null as string) as country_code
  --     , 'international' as campaign_type
  --   from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
  --   where split(campaign_name, "-")[offset(0)] in (
  --     'AE','OM','KW','SA','QA','BH','JO','US','CA','UK'
  --     )
  --     and display_rank > 0
  --     and date >= date(current_date() - 7)
  --     and partner_property_id is not null
  -- group by 1, 3, 4
  )
  qualify row_number()over(partition by partner_property_id order by date_added desc) = 1
)

, campaign_list as
(SELECT
country_code,
CONCAT(country_code, '-', device) AS campaign_name
FROM
UNNEST(['OM', 'KW', 'SA', 'QA', 'AE', 'BH', 'JO', 'US', 'CA', 'UK']) AS country_code,
UNNEST(['MWEB', 'APP', 'DESKTOP']) AS device
)

, bid_list as
(
  select distinct
    partner_property_id
    , campaign_name
    , campaign_type
    , hl.country_code as hotel_country_code
    , cl.country_code as campaign_country_code
  from hotel_list hl
  cross join campaign_list cl
    -- on hl.country_code = 
)

, skyscanner as
(
  select 
    partner_property_id
    , campaign_name
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(0)] AS site_code
    , SPLIT(campaign_name,"-")[SAFE_OFFSET(1)] AS device_type
    , hotel_impressions
    , price_difference_gbp_pence_avg
    , taxes_fees_gbp_pence_avg
    , base_price_gbp_pence_avg
    , bid_average_gbp_pence
    , los_multiplier
    , los
    , display_rank
    , CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN 1
      WHEN display_rank <= 2 THEN 1
      ELSE 0 END AS main_display_ind
    , cost_gbp_pence
    , clicks
    , guests
    , check_in_date
    , DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date
    , EXTRACT(YEAR FROM check_in_date) AS check_in_year
    , EXTRACT(MONTH FROM check_in_date) AS check_in_month
    , EXTRACT(DAY FROM check_in_date) AS check_in_day
    , DATE_DIFF(check_in_date, date, DAY) AS leadtime
    , dow
    , tod
    , date
    , EXTRACT(YEAR FROM date) AS created_at_year
    , EXTRACT(MONTH FROM date) AS created_at_month
    , EXTRACT(DAY FROM date) AS created_at_day
    , SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) as avg_bid_current
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg AS total_price_gbp_pence
    , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg AS next_cheapest_total_price_gbp_pence
    , ROUND(100*SAFE_DIVIDE(price_difference_gbp_pence_avg,base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg),2) AS next_cheapest_total_price_gbp_pence_pct
    , beat_pricing_impressions
    , meet_pricing_impressions
  from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
  left join `wego-cloud.hotel_services.hotels` as b 
    on cast(a.partner_property_id as string) = cast(b.id as string)
  -- where b.city_code in ("DXB", "DWC", "AUH", "SHJ", "RKT", "FJR", "AAN", "HKT", "BKK")
  -- and split(a.campaign_name, "-")[offset(0)] in ("AE", "OM","KW")
  WHERE display_rank > 0
  -- and date >= date(current_date() - 2) -- Take Everything
)

, skyscanner_bookings as 
(SELECT date, campaign_name, partner_property_id, check_in_date, check_out_date, tod,
SUM(bow_conversions) AS bow_conversions, 
SUM(wego_markup_amount_usd) AS total_wego_markup_amount_usd,
SUM(wego_total_price_usd) AS total_wego_total_price_usd,
SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
SUM(revenue_in_usd) as total_revenue_usd 
FROM
  (SELECT date, redirect_id, campaign_name, partner_property_id, check_in_date, los, tod, DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date,
  FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
  -- WHERE DATE(_PARTITIONTIME) BETWEEN "2025-04-25" AND "2025-09-11"
  GROUP BY 1,2,3,4,5,6,7) as a

  LEFT JOIN

  (SELECT 
  session_id, 
  REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
  FORMAT('%%s-%%s-%%s-%%s-%%s',
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
  SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
  ) AS decoded_redirect_id 
  FROM `wego-cloud.wego_analytics.sessions`
  WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
  AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
  ) as b

  ON a.redirect_id = b.decoded_redirect_id

  INNER JOIN

  (SELECT  
  session_id, DATE(check_in) as check_in, DATE(check_out) as check_out, trip_duration, 
  COUNT(DISTINCT booking_id) as bow_conversions,
  SUM(wego_base_price_usd) AS total_wego_base_price_usd,
  SUM(wego_markup_amount_usd) AS wego_markup_amount_usd,
  AVG(wego_total_price_usd) AS wego_total_price_usd,
  SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
  SUM(revenue_in_usd) AS revenue_in_usd
  FROM `wego-cloud.wego_analytics.hotels_bookings` 
  WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
  and attribution_ts_code = '6be92' -- Skyscanner Bookings
  AND conversions_tracked > 0
  GROUP BY 1,2,3,4) as c

  ON b.session_id = c.session_id
GROUP BY 1,2,3,4,5,6
)

-- , high_click as
-- (
--     select 
--         partner_property_id
--         , name_en
--         , sum(case when date >= (current_date - interval 14 day) then clicks else 0 end) as l14d_click
--         , sum(clicks) as total_click
--         , min(date) as first_live_date
--         , max(date) as last_click_date
--         , count(distinct date) as total_impression_date
--         , sum(hotel_impressions-avail_impressions_missed) as total_impression
--         , sum(cost_gbp_pence) as total_cost_gbp_pence
--     from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
--     left join `wego-cloud.hotel_services.hotels` as b 
--             on cast(a.partner_property_id as string) = cast(b.id as string)
--     where display_rank > 0
--     group by 1,2
--     having (l14d_click > 10
--     and date_diff(current_date,first_live_date,day) >= 14
--     and total_impression_date >= 14)
--     or total_click > 15
-- )

-- , bookings as
-- (
--     select
--       hotel_id
--       , count(distinct booking_id) as total_booking
--     from `wego-cloud.wego_analytics.hotels_bookings` 
--     where date(created_at) >= date(current_date - interval 14 day)
--     and attribution_ts_code = '6be92'
--     group by 1
-- )

-- , exclude_hotel as
-- (
--     select distinct
--         partner_property_id
--         , name_en
--         , first_live_date
--         , last_click_date
--         , total_click
--         , l14d_click
--         , total_impression
--         , total_cost_gbp_pence
--         , total_impression_date
--         , coalesce(total_booking,0) as total_booking
--     from high_click hc
--     left join bookings b
--         on cast(hc.partner_property_id as string) = cast(b.hotel_id as string)
--     where b.hotel_id is null
-- )

-- , property_stats as (
--   select 
--     campaign_name
--     , partner_property_id
--     , sum(display_rank * hotel_impressions) / sum(hotel_impressions) as display_rank
--     , sum((price_difference_gbp_pence_avg * hotel_impressions) 
--           / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
--         / sum(hotel_impressions) as avg_price_diff
--     , round(sum((bid_average_gbp_pence / (los * los_multiplier)) * hotel_impressions) 
--             / sum(hotel_impressions)) as avg_bid_current
--     , sum(hotel_impressions) as impressions
--     , sum(clicks) as clicks
--   from skyscanner
--   group by 1, 2
-- )

-- , prio_list as (
--   select 
--       *
--       , percent_rank()over(partition by campaign_name order by impressions) as impression_perc_rank
--   from (
--     select 
--         campaign_name
--       , partner_property_id
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) 
--           / sum(hotel_impressions) as perc_occurence
--       , sum(hotel_impressions) as impressions
--       , sum(case when (price_difference_gbp_pence_avg * 100) 
--                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
--                    and display_rank >= 3 
--                  then hotel_impressions end) avg_price_diff
--       , avg(display_rank) as avg_rank
--       , min(display_rank) as min_rank
--       , max(display_rank) as max_rank
--     from skyscanner
--     group by 1, 2
--   )
--   group by 1, 2, 3, 4, 5, 6, 7,8
-- )

SELECT 
b.partner_property_id as bid_list_partner_property_id,
b.campaign_name as bid_list_campaign_name,
b.campaign_type as bid_list_campaign_type,
b.hotel_country_code as bid_list_hotel_country_code,
b.campaign_country_code as bid_list_campaign_country_code,
IF(a.partner_property_id IS NOT NULL,1,0) AS historical_performance_ind, 
IF(b.partner_property_id IS NOT NULL,1,0) AS bid_list_ind, 
a.*,
hotel_details.*, 
wego_historical_performance.* EXCEPT (hotel_id),
skyscanner_historical_performance.* EXCEPT (partner_property_id, campaign_name, date),
sb.* EXCEPT (date, partner_property_id, campaign_name, check_in_date, check_out_date, bow_conversions, tod),
IFNULL(bow_conversions,0) as bow_conversions
FROM bid_list as b
FULL OUTER JOIN skyscanner as a
ON b.partner_property_id = a.partner_property_id AND b.campaign_name = a.campaign_name
LEFT JOIN skyscanner_bookings as sb
ON sb.date = a.date AND sb.campaign_name = a.campaign_name AND sb.check_in_date = a.check_in_date AND sb.check_out_date = a.check_out_date AND sb.partner_property_id = a.partner_property_id AND sb.tod = a.tod
-- LEFT JOIN exclude_hotel c
-- on b.partner_property_id = c.partner_property_id
LEFT JOIN hotel_details
ON COALESCE(a.partner_property_id,b.partner_property_id) = hotel_details.hotel_id
LEFT JOIN wego_historical_performance
ON COALESCE(a.partner_property_id,b.partner_property_id) = wego_historical_performance.hotel_id
LEFT JOIN skyscanner_historical_performance
ON a.partner_property_id = skyscanner_historical_performance.partner_property_id 
AND a.campaign_name = skyscanner_historical_performance.campaign_name 
AND a.date = skyscanner_historical_performance.date 
-- WHERE c.partner_property_id is null
)
"""
,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_training_data%s AS (
SELECT * FROM skyscanner_bidding.v2_skyscanner_raw_data%s
WHERE historical_performance_ind = 1 
)
"""
,date_shard_suffix,date_shard_suffix
);

-- Check Ratio of Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
  SELECT SAFE_DIVIDE(not_null_cases, null_cases)
  FROM (
    SELECT 
    COUNTIF(bow_conversions = 0) AS null_cases,
    COUNTIF(bow_conversions > 0) AS not_null_cases
    FROM `wego-cloud.skyscanner_bidding.v2_training_data%s`
  )
"""
,date_shard_suffix)
INTO null_proportion;

---- Downsample to 50:50 Not Null:Null Cases -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_training_data_downsample%s AS (
SELECT *, 
-- CASE WHEN PERCENT_RANK() OVER (ORDER BY date) <= 0.8 THEN 'TRAIN'
-- ELSE 'TEST' END AS train_test_split
FROM
  (SELECT * FROM `wego-cloud.skyscanner_bidding.v2_training_data%s`
  WHERE bow_conversions = 0
  AND RAND() < @null_proportion * 3 -- * 0.2/(1-0.2)

  UNION ALL 

  SELECT * FROM `wego-cloud.skyscanner_bidding.v2_training_data%s`
  WHERE bow_conversions > 0
  ) 
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix)
USING null_proportion as null_proportion;

----- Train Random Forest Model -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE MODEL `skyscanner_bidding.v2_rf_model%s`
OPTIONS
( model_type='RANDOM_FOREST_REGRESSOR',
  ENABLE_GLOBAL_EXPLAIN = TRUE,
  input_label_cols=['bow_conversions'],
  DATA_SPLIT_METHOD = 'SEQ',
  DATA_SPLIT_COL = 'date'
  ) AS
SELECT
date,
-- hotel_name,
-- chain_id,
chain_name,
-- brand_id,
brand_name,
-- country,
country_code,
-- location,
location_code,
-- location_id,
-- district_id,
district_name,
hotel_star_rating,
image_count,
overall_score,
reviews_count,
distance_to_city_centre,
-- property_type_id,
property_type_name,
-- built_year,
-- renovated_year,
-- booking_avg_reviews_score,
-- booking_reviews_count,
-- booking_0_5_reviews_count,
-- booking_5_6_reviews_count,
-- booking_6_7_reviews_count,
-- booking_7_8_reviews_count,
-- booking_8_9_reviews_count,
-- booking_9_10_reviews_count,
provider_count,
clicks_1mth,
conversions_1mth,
clicked_sessions_1mth,
converted_sessions_1mth,
gmv_1mth,
impressions_1mth,
bookings_1mth,
conversions_2mth,
clicks_2mth,
converted_sessions_2mth,
clicked_sessions_2mth,
gmv_2mth,
impressions_2mth,
bookings_2mth,
conversions_3mth,
clicks_3mth,
converted_sessions_3mth,
clicked_sessions_3mth,
gmv_3mth,
impressions_3mth,
bookings_3mth,
conversions_6mth,
clicks_6mth,
converted_sessions_6mth,
clicked_sessions_6mth,
gmv_6mth,
impressions_6mth,
bookings_6mth,
conversions_12mth,
clicks_12mth,
converted_sessions_12mth,
clicked_sessions_12mth,
gmv_12mth,
impressions_12mth,
bookings_12mth,
ctr_1mth,
ctr_2mth,
ctr_3mth,
ctr_6mth,
ctr_12mth,
cvr_1mth,
cvr_2mth,
cvr_3mth,
cvr_6mth,
cvr_12mth,
converted_sessions_rate_1mth,
converted_sessions_rate_2mth,
converted_sessions_rate_3mth,
converted_sessions_rate_6mth,
converted_sessions_rate_12mth,
site_code,
device_type,
los,
guests,
created_at_year,
created_at_month,
created_at_day,
check_in_year,
check_in_month,
check_in_day,
leadtime,
dow,
tod,
skyscanner_clicks_cumulative,
skyscanner_bookings_cumulative,
hotel_impressions_cumulative,
main_display_hotel_impressions_cumulative,
main_display_ratio_cumulative,
cost_gbp_pence_cumulative,
beat_pricing_impressions_cumulative,
meet_pricing_impressions_cumulative,
avg_display_rank_cumulative,
avg_price_cumulative,
absolute_price_difference_cumulative,
pct_price_difference_cumulative,
beat_meet_pricing_ratio_cumulative,
min_bid_average_gbp_pence_cumulative,
max_bid_average_gbp_pence_cumulative,
avg_bid_average_gbp_pence_cumulative,
display_rank,
main_display_ind,
price_difference_gbp_pence_avg,
total_price_gbp_pence,
next_cheapest_total_price_gbp_pence,
next_cheapest_total_price_gbp_pence_pct,
-- cost_gbp_pence, -- Might be reflective of clicks, giving the answer
-- beat_pricing_impressions,
-- meet_pricing_impressions,
avg_bid_current, 
IFNULL(bow_conversions,0) as bow_conversions,
-- train_test_split
FROM `skyscanner_bidding.v2_training_data_downsample%s`
"""
,date_shard_suffix,date_shard_suffix
);


----- 4. Model Evaluation Metrics -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_rf_model_evaluation%s AS (
SELECT * FROM
ML.EVALUATE(MODEL`skyscanner_bidding.v2_rf_model%s`))
"""
,date_shard_suffix,date_shard_suffix
);

--- 5. Model Feature Importance -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_rf_model_feature_importance%s AS (
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `skyscanner_bidding.v2_rf_model%s`)
ORDER BY importance_gain DESC
)
"""
,date_shard_suffix,date_shard_suffix
);

----- 6. Model Global Explain -----
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_rf_model_global_explain%s AS (
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `skyscanner_bidding.v2_rf_model%s`)
)
"""
,date_shard_suffix,date_shard_suffix
);

--------- GENERATE MOCK BIDS PER CAMPAIGN, HOTEL ID COMBINATION TO PREDICT AGAINST MODEL ---------
-- Generate Array of Bids from 20 to 135, in steps of 5 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_bid_template%s AS (
SELECT *
FROM
  (SELECT 
  campaign_name,
  partner_property_id,
  hotel_name,
  -- Model Features --
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  clicked_sessions_1mth,
  converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  conversions_2mth,
  clicks_2mth,
  converted_sessions_2mth,
  clicked_sessions_2mth,
  gmv_2mth,
  impressions_2mth,
  bookings_2mth,
  conversions_3mth,
  clicks_3mth,
  converted_sessions_3mth,
  clicked_sessions_3mth,
  gmv_3mth,
  impressions_3mth,
  bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  converted_sessions_6mth,
  clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  converted_sessions_12mth,
  clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  ctr_2mth,
  ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  cvr_2mth,
  cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  converted_sessions_rate_1mth,
  converted_sessions_rate_2mth,
  converted_sessions_rate_3mth,
  converted_sessions_rate_6mth,
  converted_sessions_rate_12mth, 
  -- If it's part of a bid list, but there's no historical skyscanner performance data, take the global average to use as a basis to predict against the model
  a.site_code,
  a.device_type,
  IF(historical_performance_ind = 1, a.los, b.los) AS los,
  IF(historical_performance_ind = 1, a.guests, b.guests) AS guests,
  IF(historical_performance_ind = 1, a.created_at_year, b.created_at_year) AS created_at_year,
  IF(historical_performance_ind = 1, a.created_at_month, b.created_at_month) AS created_at_month,
  IF(historical_performance_ind = 1, a.created_at_day, b.created_at_day) AS created_at_day,
  IF(historical_performance_ind = 1, a.check_in_year, b.check_in_year) AS check_in_year,
  IF(historical_performance_ind = 1, a.check_in_month, b.check_in_month) AS check_in_month,
  IF(historical_performance_ind = 1, a.check_in_day, b.check_in_day) AS check_in_day,
  IF(historical_performance_ind = 1, a.leadtime, b.leadtime) AS leadtime,
  IF(historical_performance_ind = 1, a.dow, b.dow) AS dow,
  IF(historical_performance_ind = 1, a.tod, b.tod) AS tod,
  IF(historical_performance_ind = 1, a.skyscanner_clicks_cumulative, b.skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
  IF(historical_performance_ind = 1, a.skyscanner_bookings_cumulative, b.skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
  IF(historical_performance_ind = 1, a.hotel_impressions_cumulative, b.hotel_impressions_cumulative) AS hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_hotel_impressions_cumulative, b.main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
  IF(historical_performance_ind = 1, a.main_display_ratio_cumulative, b.main_display_ratio_cumulative) AS main_display_ratio_cumulative,
  IF(historical_performance_ind = 1, a.cost_gbp_pence_cumulative, b.cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.beat_pricing_impressions_cumulative, b.beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.meet_pricing_impressions_cumulative, b.meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
  IF(historical_performance_ind = 1, a.avg_display_rank_cumulative, b.avg_display_rank_cumulative) AS avg_display_rank_cumulative,
  IF(historical_performance_ind = 1, a.avg_price_cumulative, b.avg_price_cumulative) AS avg_price_cumulative,
  IF(historical_performance_ind = 1, a.absolute_price_difference_cumulative, b.absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.pct_price_difference_cumulative, b.pct_price_difference_cumulative) AS pct_price_difference_cumulative,
  IF(historical_performance_ind = 1, a.beat_meet_pricing_ratio_cumulative, b.beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
  IF(historical_performance_ind = 1, a.min_bid_average_gbp_pence_cumulative, b.min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.max_bid_average_gbp_pence_cumulative, b.max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.avg_bid_average_gbp_pence_cumulative, b.avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
  IF(historical_performance_ind = 1, a.display_rank, b.display_rank) AS display_rank,
  IF(historical_performance_ind = 1, a.main_display_ind, b.main_display_ind) AS main_display_ind,
  IF(historical_performance_ind = 1, a.price_difference_gbp_pence_avg, b.price_difference_gbp_pence_avg) AS price_difference_gbp_pence_avg,
  IF(historical_performance_ind = 1, a.total_price_gbp_pence, b.total_price_gbp_pence) AS total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence, b.next_cheapest_total_price_gbp_pence) AS next_cheapest_total_price_gbp_pence,
  IF(historical_performance_ind = 1, a.next_cheapest_total_price_gbp_pence_pct, b.next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct
  FROM
    (SELECT 
    bid_list_campaign_name AS campaign_name,
    bid_list_partner_property_id AS partner_property_id,
    hotel_name,
    -- Model Features --
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(0)], campaign_name) AS site_code,
    COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(1)], campaign_name) AS device_type,
    chain_name,
    brand_name,
    country_code,
    location_code,
    district_name,
    hotel_star_rating,
    image_count,
    overall_score,
    reviews_count,
    distance_to_city_centre,
    property_type_name,
    provider_count,
    clicks_1mth,
    conversions_1mth,
    clicked_sessions_1mth,
    converted_sessions_1mth,
    gmv_1mth,
    impressions_1mth,
    bookings_1mth,
    conversions_2mth,
    clicks_2mth,
    converted_sessions_2mth,
    clicked_sessions_2mth,
    gmv_2mth,
    impressions_2mth,
    bookings_2mth,
    conversions_3mth,
    clicks_3mth,
    converted_sessions_3mth,
    clicked_sessions_3mth,
    gmv_3mth,
    impressions_3mth,
    bookings_3mth,
    conversions_6mth,
    clicks_6mth,
    converted_sessions_6mth,
    clicked_sessions_6mth,
    gmv_6mth,
    impressions_6mth,
    bookings_6mth,
    conversions_12mth,
    clicks_12mth,
    converted_sessions_12mth,
    clicked_sessions_12mth,
    gmv_12mth,
    impressions_12mth,
    bookings_12mth,
    ctr_1mth,
    ctr_2mth,
    ctr_3mth,
    ctr_6mth,
    ctr_12mth,
    cvr_1mth,
    cvr_2mth,
    cvr_3mth,
    cvr_6mth,
    cvr_12mth,
    converted_sessions_rate_1mth,
    converted_sessions_rate_2mth,
    converted_sessions_rate_3mth,
    converted_sessions_rate_6mth,
    converted_sessions_rate_12mth,
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    -- Check Indicator of Whether There was Historical Skyscanner Performance
    MAX(historical_performance_ind) AS historical_performance_ind,
    FROM `skyscanner_bidding.v2_skyscanner_raw_data%s`
    WHERE bid_list_ind = 1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67) as a

    CROSS JOIN

    -- Generate One Row of a Global Dummy Average for Hotels without any Historical Performance
    (SELECT 
    -- Take AVG of the general search 
    CAST(AVG(los) AS INT64) AS los,
    CAST(AVG(guests) AS INT64) AS guests,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
    MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
    MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
    CAST(AVG(check_in_year) AS INT64) AS check_in_year,
    CAST(AVG(check_in_month) AS INT64) AS check_in_month,
    CAST(AVG(check_in_day) AS INT64) AS check_in_day,
    CAST(AVG(leadtime) AS INT64) AS leadtime,
    CAST(AVG(dow) AS INT64) AS dow,
    CAST(AVG(tod) AS INT64) AS tod,
    -- MAX to take the latest performance to predict forward
    MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
    MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
    MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
    MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
    MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
    MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
    MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
    MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
    MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
    MAX(avg_price_cumulative) AS avg_price_cumulative,
    MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
    MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
    MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
    MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
    MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
    MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
    -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
    CAST(AVG(display_rank) AS INT64) AS display_rank,
    1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
    CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
    CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
    CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
    AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
    -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
    -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
    -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
    FROM `skyscanner_bidding.v2_skyscanner_raw_data%s`
    WHERE historical_performance_ind = 1) as b 
  ),UNNEST(GENERATE_ARRAY(20,135,5)) as mock_bid
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

-- -- --------- GET PREDICITONS FROM MODEL ---------
--- For each hotel, put in a list of mock bids to predict with the model, then rank by best booking prediction 
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_training_data_explain_predict%s AS (
SELECT *,
DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value DESC) as prediction_ranking,
-- Less Precise on the Decimal Places of Prediction Value (So that more consideration among lower bid higher predicted click cases)
-- DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value_exponent DESC, ROUND(prediction_value_mantissa,1) DESC) as prediction_ranking_blunt, 
FROM
  (SELECT
  -- input.*,
  -- predoutput.*
  predoutput.* ,
  -- predoutput.predicted_total_itinerary_card_clicks,
  -- predoutput.total_itinerary_card_clicks_probs
  FLOOR(LOG10(predoutput.prediction_value)) AS prediction_value_exponent,
  SAFE_DIVIDE(predoutput.prediction_value,POWER(10, FLOOR(LOG10(predoutput.prediction_value)))) AS prediction_value_mantissa
  FROM
  ML.EXPLAIN_PREDICT(MODEL `skyscanner_bidding.v2_rf_model%s`,
  (
  SELECT
  campaign_name,
  partner_property_id,
  hotel_name,
  -- Model Features --
  chain_name,
  brand_name,
  country_code,
  location_code,
  district_name,
  hotel_star_rating,
  image_count,
  overall_score,
  reviews_count,
  distance_to_city_centre,
  property_type_name,
  provider_count,
  clicks_1mth,
  conversions_1mth,
  clicked_sessions_1mth,
  converted_sessions_1mth,
  gmv_1mth,
  impressions_1mth,
  bookings_1mth,
  conversions_2mth,
  clicks_2mth,
  converted_sessions_2mth,
  clicked_sessions_2mth,
  gmv_2mth,
  impressions_2mth,
  bookings_2mth,
  conversions_3mth,
  clicks_3mth,
  converted_sessions_3mth,
  clicked_sessions_3mth,
  gmv_3mth,
  impressions_3mth,
  bookings_3mth,
  conversions_6mth,
  clicks_6mth,
  converted_sessions_6mth,
  clicked_sessions_6mth,
  gmv_6mth,
  impressions_6mth,
  bookings_6mth,
  conversions_12mth,
  clicks_12mth,
  converted_sessions_12mth,
  clicked_sessions_12mth,
  gmv_12mth,
  impressions_12mth,
  bookings_12mth,
  ctr_1mth,
  ctr_2mth,
  ctr_3mth,
  ctr_6mth,
  ctr_12mth,
  cvr_1mth,
  cvr_2mth,
  cvr_3mth,
  cvr_6mth,
  cvr_12mth,
  converted_sessions_rate_1mth,
  converted_sessions_rate_2mth,
  converted_sessions_rate_3mth,
  converted_sessions_rate_6mth,
  converted_sessions_rate_12mth,
  site_code,
  device_type,
  los,
  guests,
  created_at_year,
  created_at_month,
  created_at_day,
  check_in_year,
  check_in_month,
  check_in_day,
  leadtime,
  dow,
  tod,
  skyscanner_clicks_cumulative,
  skyscanner_bookings_cumulative,
  hotel_impressions_cumulative,
  main_display_hotel_impressions_cumulative,
  main_display_ratio_cumulative,
  cost_gbp_pence_cumulative,
  beat_pricing_impressions_cumulative,
  meet_pricing_impressions_cumulative,
  avg_display_rank_cumulative,
  avg_price_cumulative,
  absolute_price_difference_cumulative,
  pct_price_difference_cumulative,
  beat_meet_pricing_ratio_cumulative,
  min_bid_average_gbp_pence_cumulative,
  max_bid_average_gbp_pence_cumulative,
  avg_bid_average_gbp_pence_cumulative,
  display_rank,
  main_display_ind,
  price_difference_gbp_pence_avg,
  total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence,
  next_cheapest_total_price_gbp_pence_pct,
  -- cost_gbp_pence,
  -- beat_pricing_impressions,
  -- meet_pricing_impressions,
  mock_bid as avg_bid_current,
  FROM `skyscanner_bidding.v2_bid_template%s`
  -- WHERE campaign_name = "AE-APP"	
  -- AND partner_property_id = "3051914"
  ),
  STRUCT(TRUE AS approx_feature_contrib))
  predoutput 
  -- join `skyscanner_bidding.v2_bid_template%s` input 
  -- ON predoutput.campaign_name=input.campaign_name 
  -- AND predoutput.partner_property_id=input.partner_property_id
  -- AND predoutput.avg_bid_current=input.avg_bid_current
)
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);


-- -- --------- SELECT BEST BID BASED ON HIGHEST PREDICTED VALUE (Clicks) ---------
--- Based on each click prediction from the model, if there's a tie for 1st place, take the lowest bid needed to get a max predicted clicks
EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_bid_selection%s AS (
SELECT *, LEAST(avg_bid_current,135) as bid,
FROM
  (SELECT campaign_name, partner_property_id, MIN(avg_bid_current) as avg_bid_current
  FROM `skyscanner_bidding.v2_training_data_explain_predict%s`
  WHERE prediction_ranking = 1
  GROUP BY 1,2)
)
"""
,date_shard_suffix,date_shard_suffix
);


EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_union_group%s AS (
SELECT
  partner_property_id
  , campaign_name
  , least(bid,135) as bid
FROM `skyscanner_bidding.v2_bid_selection%s`
UNION ALL
SELECT 
  b.partner_property_id, 
  a.campaign_name, 
  1 AS bid 
FROM (
  SELECT DISTINCT CONCAT(campaign_name, "-", "GROUP") AS campaign_name 
  FROM `skyscanner_bidding.v2_bid_selection%s`
) AS a
JOIN (
  SELECT DISTINCT partner_property_id 
  FROM `skyscanner_bidding.v2_bid_selection%s`
) AS b ON 1=1
ORDER BY campaign_name, partner_property_id
)
"""
,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE skyscanner_bidding.v2_bid_file%s AS (
SELECT *
FROM (
  SELECT
    partner_property_id as `Hotel ID`,
    campaign_name,
    bid
  FROM `skyscanner_bidding.v2_union_group%s`
)
PIVOT (
  SUM(bid) FOR campaign_name IN (
    'AE-APP'
    , 'AE-APP-GROUP'
    , 'AE-DESKTOP'
    , 'AE-DESKTOP-GROUP'
    , 'AE-MWEB'
    , 'AE-MWEB-GROUP'
    , 'BH-APP'
    , 'BH-APP-GROUP'
    , 'BH-DESKTOP'
    , 'BH-DESKTOP-GROUP'
    , 'BH-MWEB'
    , 'BH-MWEB-GROUP'
    , 'CA-APP'
    , 'CA-APP-GROUP'
    , 'CA-DESKTOP'
    , 'CA-DESKTOP-GROUP'
    , 'CA-MWEB'
    , 'CA-MWEB-GROUP'
    , 'UK-APP'
    , 'UK-APP-GROUP'
    , 'UK-DESKTOP'
    , 'UK-DESKTOP-GROUP'
    , 'UK-MWEB'
    , 'UK-MWEB-GROUP'
    , 'JO-APP'
    , 'JO-APP-GROUP'
    , 'JO-DESKTOP'
    , 'JO-DESKTOP-GROUP'
    , 'JO-MWEB'
    , 'JO-MWEB-GROUP'
    , 'KW-APP'
    , 'KW-APP-GROUP'
    , 'KW-DESKTOP'
    , 'KW-DESKTOP-GROUP'
    , 'KW-MWEB'
    , 'KW-MWEB-GROUP'
    , 'OM-APP'
    , 'OM-APP-GROUP'
    , 'OM-DESKTOP'
    , 'OM-DESKTOP-GROUP'
    , 'OM-MWEB'
    , 'OM-MWEB-GROUP'
    , 'QA-APP'
    , 'QA-APP-GROUP'
    , 'QA-DESKTOP'
    , 'QA-DESKTOP-GROUP'
    , 'QA-MWEB'
    , 'QA-MWEB-GROUP'
    , 'SA-APP'
    , 'SA-APP-GROUP'
    , 'SA-DESKTOP'
    , 'SA-DESKTOP-GROUP'
    , 'SA-MWEB'
    , 'SA-MWEB-GROUP'
    , 'US-APP'
    , 'US-APP-GROUP'
    , 'US-DESKTOP'
    , 'US-DESKTOP-GROUP'
    , 'US-MWEB'
    , 'US-MWEB-GROUP'
))
)
"""
,date_shard_suffix,date_shard_suffix
);

------------------------ DELETE OLDER MODELS ------------------------
---------- Maintain the past 60 models, delete the rest -------------
SET (model_list, model_count) = (
SELECT AS STRUCT ARRAY_AGG(table_id) as model_list, COUNT(*) as model_count
FROM (
  SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY base_table_id ORDER BY creation_time DESC) AS rn FROM
    (SELECT *, 
    REGEXP_REPLACE(table_id, r'[0-9]{8}$', '') AS base_table_id,
    FROM `wego-cloud.skyscanner_bidding.__TABLES__`
    WHERE type = 4
    AND (table_id LIKE 'rf_model%'
    OR table_id LIKE 'v1_%'
    OR table_id LIKE 'v2_%')
    )
  )
  WHERE rn > 60
  ORDER BY table_id, creation_time
  )
);  

LOOP
  IF i >= model_count THEN
    LEAVE;
  END IF;
    SET model_name = model_list[OFFSET(i)];
    --RAISE USING MESSAGE = FORMAT("""Model to drop: %s""", model_name);
    EXECUTE IMMEDIATE FORMAT("""DROP MODEL `wego-cloud.skyscanner_bidding.%s`""", model_name);
    SET i = i + 1;
END LOOP;


-- 2026-06-10: Commented V3 pipeline since its not used yet + its expensive

-- ------------------------ V3 PIPELINE ------------------------
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_skyscanner_raw_data%s AS (
-- WITH hotel_details AS
-- (SELECT
-- CAST(h.id AS STRING) as hotel_id, h.name_en as hotel_name, brand.chain_id as chain_id, chain_name, h.brand_id as brand_id, brand_name, c.base_name as country, c.code as country_code, l.base_name as location, l.code as location_code, l.id as location_id, h.district_id as district_id, dt.district_name as district_name, star as hotel_star_rating,
-- img.image_count as image_count, trustyou.score as overall_score, trustyou.reviews_count as reviews_count, distance_to_city_centre, h.property_type_id, property_type_name,
-- IF(built_year IS NULL OR SAFE_CAST(built_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(built_year AS INT64) < 1900, NULL, SAFE_CAST(built_year AS INT64)) as built_year, -- Force null for strange years & less than 1900
-- IF(renovated_year IS NULL OR SAFE_CAST(renovated_year AS INT64) > EXTRACT(YEAR FROM CURRENT_DATE()) OR SAFE_CAST(renovated_year AS INT64) < 1900, NULL, SAFE_CAST(renovated_year AS INT64)) as renovated_year, -- Force null for strange years & less than 1900
-- booking_avg_reviews_score,
-- booking_reviews_count,
-- booking_0_5_reviews_count,
-- booking_5_6_reviews_count,  
-- booking_6_7_reviews_count,  
-- booking_7_8_reviews_count,  
-- booking_8_9_reviews_count,  
-- booking_9_10_reviews_count, 
-- provider_count,
-- FROM `wego-cloud.hotel_services.hotels` as h
-- LEFT JOIN
-- (SELECT * EXCEPT (rn)
-- FROM (SELECT base_name, code, id, country_id, RANK() OVER (PARTITION BY code ORDER BY updated_at DESC) as rn
-- FROM `wego-cloud.place_services.locations` )
-- WHERE rn = 1) as l on l.code=h.city_code
-- LEFT JOIN `wego-cloud.place_services.countries` as c on c.id=l.country_id
-- LEFT JOIN
-- (SELECT id, ANY_VALUE(base_name) as district_name 
-- FROM `wego-cloud.place_services.districts` 
-- GROUP BY 1) as dt on h.district_id = dt.id
-- --            LEFT JOIN `wego-cloud.hotels.hotel_stats` as st on st.id=h.id
-- --            LEFT JOIN
-- --                (SELECT hotel_id, MAX(score) as score FROM hotel_services.reviews
-- --                WHERE reviewer_group='ALL' GROUP BY hotel_id) as trustyou on trustyou.hotel_id=h.id

-- LEFT JOIN

-- (SELECT hotel_id,
-- COUNT(DISTINCT provider_id) as provider_count,
-- FROM `wego-cloud.hotels.provider_hotels`
-- GROUP BY 1) as ph

-- ON h.id = ph.hotel_id 

-- LEFT JOIN

-- (SELECT hotel_id, sum(1) as image_count
-- FROM `wego-cloud.hotel_services.images`
-- GROUP BY hotel_id) as img

-- ON img.hotel_id=h.id

-- LEFT JOIN

-- (SELECT hotel_id, score, reviews_count,
-- -- IF(reviews_count IS NULL, AVG(reviews_count) OVER (), SAFE_DIVIDE(reviews_count,(MAX(reviews_count) OVER () - MIN(reviews_count) OVER()))) as reviews_count_weighted -- reviews_count/(max_reviews_count-min_reviews_count), otherwise take average.
-- FROM
-- (SELECT hotel_id, MAX(score) as score, MAX(count) as reviews_count
-- FROM hotel_services.reviews
-- WHERE reviewer_group='ALL'
-- GROUP BY hotel_id)
-- ) as trustyou

-- ON trustyou.hotel_id=h.id

-- LEFT JOIN

-- (SELECT 
-- hotel_id,
-- AVG(rating) as booking_avg_reviews_score,
-- COUNT(*) as booking_reviews_count,
-- COUNTIF(rating >= 0 AND rating < 5) as booking_0_5_reviews_count,
-- COUNTIF(rating >= 5 AND rating < 6) as booking_5_6_reviews_count,  
-- COUNTIF(rating >= 6 AND rating < 7) as booking_6_7_reviews_count,  
-- COUNTIF(rating >= 7 AND rating < 8) as booking_7_8_reviews_count,  
-- COUNTIF(rating >= 8 AND rating < 9) as booking_8_9_reviews_count,  
-- COUNTIF(rating >= 9 AND rating <= 10) as booking_9_10_reviews_count,  
-- FROM `wego-cloud.hotel_services.provider_reviews`
-- GROUP BY 1) as booking

-- ON booking.hotel_id=h.id

-- LEFT JOIN

-- (SELECT id as brand_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as brand_name, chain_id, is_chain
-- FROM `wego-cloud.hotel_services.brands`) as brand
-- ON h.brand_id = brand.brand_id

-- LEFT JOIN

-- (SELECT id as chain_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as chain_name
-- FROM `wego-cloud.hotel_services.chains`) as chain
-- ON chain.chain_id = brand.chain_id

-- LEFT JOIN

-- (SELECT id as property_type_id,  REGEXP_EXTRACT(name, r'en\":\"(.*?)\"') as property_type_name
-- FROM `hotel_services.property_types`) as property_type
-- on h.property_type_id = property_type.property_type_id),

-- wego_historical_performance AS 
-- (SELECT 
-- * EXCEPT (
--   total_clicks, 
--   total_conversions, 
--   total_clicked_sessions, 
--   total_converted_sessions, 
--   total_gmv, 
--   total_impressions, 
--   total_bookings, 
--   row_number
-- ),

-- -- 1-month aliases (current month values)
-- total_clicks AS clicks_1mth,
-- total_conversions AS conversions_1mth,
-- total_clicked_sessions AS clicked_sessions_1mth,
-- total_converted_sessions AS converted_sessions_1mth,
-- total_gmv AS gmv_1mth,
-- total_impressions AS impressions_1mth,
-- total_bookings AS bookings_1mth,

-- -- CTRs
-- ROUND(SAFE_DIVIDE(total_clicks, total_impressions) * 100, 2) AS ctr_1mth,
-- ROUND(SAFE_DIVIDE(clicks_2mth, impressions_2mth) * 100, 2) AS ctr_2mth,
-- ROUND(SAFE_DIVIDE(clicks_3mth, impressions_3mth) * 100, 2) AS ctr_3mth,
-- ROUND(SAFE_DIVIDE(clicks_6mth, impressions_6mth) * 100, 2) AS ctr_6mth,
-- ROUND(SAFE_DIVIDE(clicks_12mth, impressions_12mth) * 100, 2) AS ctr_12mth,

-- -- CVRs
-- ROUND(SAFE_DIVIDE(total_conversions, total_clicks) * 100, 2) AS cvr_1mth,
-- ROUND(SAFE_DIVIDE(conversions_2mth, clicks_2mth) * 100, 2) AS cvr_2mth,
-- ROUND(SAFE_DIVIDE(conversions_3mth, clicks_3mth) * 100, 2) AS cvr_3mth,
-- ROUND(SAFE_DIVIDE(conversions_6mth, clicks_6mth) * 100, 2) AS cvr_6mth,
-- ROUND(SAFE_DIVIDE(conversions_12mth, clicks_12mth) * 100, 2) AS cvr_12mth,

-- -- Converted session rates
-- ROUND(SAFE_DIVIDE(total_converted_sessions, total_clicked_sessions) * 100, 2) AS converted_sessions_rate_1mth,
-- ROUND(SAFE_DIVIDE(converted_sessions_2mth, clicked_sessions_2mth) * 100, 2) AS converted_sessions_rate_2mth,
-- ROUND(SAFE_DIVIDE(converted_sessions_3mth, clicked_sessions_3mth) * 100, 2) AS converted_sessions_rate_3mth,
-- ROUND(SAFE_DIVIDE(converted_sessions_6mth, clicked_sessions_6mth) * 100, 2) AS converted_sessions_rate_6mth,
-- ROUND(SAFE_DIVIDE(converted_sessions_12mth, clicked_sessions_12mth) * 100, 2) AS converted_sessions_rate_12mth,
-- FROM 
--   (SELECT 
--   CAST(hotel_id AS STRING) as hotel_id, 
--   month,
--   total_clicks,
--   total_conversions,
--   total_clicked_sessions,
--   total_converted_sessions,
--   total_gmv,
--   total_impressions,
--   total_bookings,

--   -- 2-month window
--   SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS conversions_2mth,
--   SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicks_2mth,
--   SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS converted_sessions_2mth,
--   SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS clicked_sessions_2mth,
--   SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS gmv_2mth,
--   SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS impressions_2mth,
--   SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS bookings_2mth,

--   -- 3-month window
--   SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS conversions_3mth,
--   SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicks_3mth,
--   SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS converted_sessions_3mth,
--   SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS clicked_sessions_3mth,
--   SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS gmv_3mth,
--   SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS impressions_3mth,
--   SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) AS bookings_3mth,

--   -- 6-month window
--   SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS conversions_6mth,
--   SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicks_6mth,
--   SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS converted_sessions_6mth,
--   SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS clicked_sessions_6mth,
--   SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS gmv_6mth,
--   SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS impressions_6mth,
--   SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING) AS bookings_6mth,

--   -- 12-month window
--   SUM(total_conversions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS conversions_12mth,
--   SUM(total_clicks) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicks_12mth,
--   SUM(total_converted_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS converted_sessions_12mth,
--   SUM(total_clicked_sessions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS clicked_sessions_12mth,
--   SUM(total_gmv) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS gmv_12mth,
--   SUM(total_impressions) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS impressions_12mth,
--   SUM(total_bookings) OVER (PARTITION BY hotel_id ORDER BY row_number RANGE BETWEEN CURRENT ROW AND 11 FOLLOWING) AS bookings_12mth,

--   row_number
--   FROM
--   (SELECT a.hotel_id as hotel_id,
--   a.month as month,
--   * EXCEPT (hotel_id, month), 
--   ROW_NUMBER() OVER (PARTITION BY a.hotel_id ORDER BY a.month DESC) AS row_number
--   FROM

--     (SELECT id as hotel_id, month 
--     FROM `wego-cloud.hotel_services.hotels` 
--     CROSS JOIN 
--     (SELECT FORMAT_DATE('%%Y-%%m', month_list) AS month
--     FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH),DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH),INTERVAL 1 MONTH)) AS month_list
--     )
--     ) as a

--     LEFT JOIN

--     (SELECT
--     hotel_id, 
--     FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
--     SUM(conversions_tracked) AS total_conversions, 
--     COUNT(click_id) AS total_clicks,
--     COUNT(DISTINCT session_id) AS total_clicked_sessions,
--     COUNT(DISTINCT IF(conversions_tracked > 0, session_id, NULL)) as total_converted_sessions,
--     SUM(booking_value_usd) as total_gmv
--     FROM `wego-cloud.wego_analytics.hotels_clicks`
--     WHERE DATE(_PARTITIONDATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
--     GROUP BY 1,2) as b
--     ON a.hotel_id = b.hotel_id AND a.month = b.month

--     LEFT JOIN

--     (SELECT
--     CAST(hotel_id AS INT64) as hotel_id,
--     FORMAT_TIMESTAMP('%%Y-%%m', search_created_at) AS month, 
--     COUNT(DISTINCT search_id) as total_impressions
--     FROM `wego_analytics.hotels_impressions*`
--     WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)) AND FORMAT_DATE('%%Y%%m%%d', CURRENT_DATE())
--     GROUP BY 1,2) as c

--     ON a.hotel_id = c.hotel_id AND a.month = c.month

--     LEFT JOIN

--     (SELECT hotel_id,
--     FORMAT_TIMESTAMP('%%Y-%%m', created_at) AS month, 
--     -- COUNT(DISTINCT IF(attribution_ts_code = '6be92',booking_id, NULL)) as total_skyscanner_bookings,
--     -- SUM(IF(attribution_ts_code = '6be92',wego_total_price_usd, NULL)) as total_skyscanner_gmv,
--     COUNT(DISTINCT booking_id) as total_bookings
--     FROM `wego-cloud.wego_analytics.hotels_bookings` 
--     WHERE DATE(created_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
--     AND attribution_ts_code IS NULL
--     GROUP BY 1,2) as d

--     ON a.hotel_id = d.hotel_id AND a.month = d.month
--     )
--   )
--   WHERE row_number = 1),

-- skyscanner_historical_performance AS

-- (SELECT 
-- campaign_name, 
-- partner_property_id, 
-- date, 
-- skyscanner_clicks_cumulative,
-- skyscanner_bookings_cumulative,
-- hotel_impressions_cumulative,
-- main_display_hotel_impressions_cumulative,
-- SAFE_DIVIDE(main_display_hotel_impressions_cumulative,hotel_impressions_cumulative) AS main_display_ratio_cumulative,
-- cost_gbp_pence_cumulative,
-- beat_pricing_impressions_cumulative,
-- meet_pricing_impressions_cumulative,
-- SAFE_DIVIDE(display_rank_impressions_cumulative,hotel_impressions_cumulative) AS avg_display_rank_cumulative,
-- SAFE_DIVIDE(price_impressions_cumulative,hotel_impressions_cumulative) AS avg_price_cumulative,
-- SAFE_DIVIDE(absolute_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS absolute_price_difference_cumulative,
-- SAFE_DIVIDE(pct_price_difference_impressions_cumulative,hotel_impressions_cumulative) AS pct_price_difference_cumulative,
-- SAFE_DIVIDE(beat_pricing_impressions_cumulative,meet_pricing_impressions_cumulative) AS beat_meet_pricing_ratio_cumulative,
-- min_bid_average_gbp_pence_cumulative,
-- max_bid_average_gbp_pence_cumulative,
-- SAFE_DIVIDE(bid_average_gbp_pence_impressions_cumulative,hotel_impressions_cumulative) AS avg_bid_average_gbp_pence_cumulative
-- FROM
--   (SELECT 
--   a.campaign_name, 
--   a.partner_property_id, 
--   a.date, 
--   -- clicks as skyscanner_clicks, 
--   -- total_bookings as skyscanner_bookings,
--   -- hotel_impressions,
--   -- cost_gbp_pence,
--   -- beat_pricing_impressions,
--   -- meet_pricing_impressions,
--   ROW_NUMBER() OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date) AS date_ranking,
--   SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_clicks_cumulative,
--   SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS skyscanner_bookings_cumulative,
--   SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS hotel_impressions_cumulative,
--   SUM(main_display_hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS main_display_hotel_impressions_cumulative,
--   SUM(cost_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cost_gbp_pence_cumulative,
--   SUM(beat_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS beat_pricing_impressions_cumulative,
--   SUM(meet_pricing_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS meet_pricing_impressions_cumulative,
--   SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS display_rank_impressions_cumulative,
--   SUM(price_impressions_cumulative) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS price_impressions_cumulative,
--   SUM(absolute_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS absolute_price_difference_impressions_cumulative,
--   SUM(pct_price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS pct_price_difference_impressions_cumulative,
--   MIN(min_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS min_bid_average_gbp_pence_cumulative,
--   MAX(max_bid_average_gbp_pence) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_bid_average_gbp_pence_cumulative,
--   SUM(bid_average_gbp_pence_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS bid_average_gbp_pence_impressions_cumulative,
--   -- SAFE_DIVIDE(SUM(display_rank_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS avg_display_rank_cumulative,
--   -- SAFE_DIVIDE(SUM(price_difference_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY a.date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), SUM(hotel_impressions) OVER (PARTITION BY a.campaign_name, a.partner_property_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS price_difference_cumulative,
--   -- SUM(clicks) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_clicks_overall,
--   -- SUM(total_bookings) OVER (PARTITION BY a.campaign_name, a.partner_property_id) AS total_bookings_overall,
--     FROM
--     (SELECT 
--     campaign_name, 
--     partner_property_id, 
--     date, 
--     SUM(clicks) as clicks, 
--     SUM(hotel_impressions) as hotel_impressions,
--     SUM(CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN hotel_impressions
--     WHEN display_rank <= 2 THEN hotel_impressions
--     ELSE 0 END) AS  main_display_hotel_impressions,
--     SUM(cost_gbp_pence) AS cost_gbp_pence,
--     SUM(beat_pricing_impressions) AS beat_pricing_impressions,
--     SUM(meet_pricing_impressions) AS meet_pricing_impressions,
--     SUM(display_rank*hotel_impressions) AS display_rank_impressions,
--     SUM((base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg) * hotel_impressions) AS price_impressions_cumulative,
--     SUM(price_difference_gbp_pence_avg * hotel_impressions) AS absolute_price_difference_impressions,
--     SUM(SAFE_DIVIDE(100*price_difference_gbp_pence_avg * hotel_impressions,base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) AS pct_price_difference_impressions,
--     MIN(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS min_bid_average_gbp_pence,
--     MAX(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier))) AS max_bid_average_gbp_pence,
--     SUM(SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) * hotel_impressions) as bid_average_gbp_pence_impressions
--     FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
--     WHERE display_rank > 0
--     GROUP BY 1,2,3
--     ) as a

--     LEFT JOIN

--     (SELECT date, campaign_name, partner_property_id, SUM(bow_conversion) AS total_bookings 
--     FROM
--       (SELECT date, redirect_id, campaign_name, partner_property_id
--       FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
--       GROUP BY 1,2,3,4) as a

--       LEFT JOIN

--       (SELECT 
--       session_id, 
--       REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
--       FORMAT('%%s-%%s-%%s-%%s-%%s',
--       SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
--       SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
--       SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
--       SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
--       SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
--       ) AS decoded_redirect_id FROM 
--       `wego-cloud.wego_analytics.sessions`
--       WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
--       AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
--       ) as b

--       ON a.redirect_id = b.decoded_redirect_id

--       INNER JOIN

--       (SELECT  
--       session_id, 1 AS bow_conversion
--       FROM `wego-cloud.wego_analytics.hotels_bookings` 
--       WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
--       and attribution_ts_code = '6be92' -- Skyscanner Bookings
--       AND conversions_tracked > 0
--       GROUP BY 1) as c

--       ON b.session_id = c.session_id
--     GROUP BY 1,2,3
--     ) as b

--     ON a.date = b.date AND a.campaign_name = b.campaign_name AND a.partner_property_id = b.partner_property_id

--   )
-- WHERE date_ranking > 1
-- ),


-- hotel_list as
-- (
--   select * from(
--     select
--       partner_property_id
--       , date_added
--       , country_code
--       , campaign_type
--     from `wego-cloud.analysis.skyscanner_bid_hotel_list`
--     where partner_property_id is not null
--   --   union distinct 
--   --   select
--   --     cast(partner_property_id as string)
--   --     , min(date) as date_added
--   --     , cast(null as string) as country_code
--   --     , 'international' as campaign_type
--   --   from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` 
--   --   where split(campaign_name, "-")[offset(0)] in (
--   --     'AE','OM','KW','SA','QA','BH','JO','US','CA','UK'
--   --     )
--   --     and display_rank > 0
--   --     and date >= date(current_date() - 7)
--   --     and partner_property_id is not null
--   -- group by 1, 3, 4
--   )
--   qualify row_number()over(partition by partner_property_id order by date_added desc) = 1
-- )

-- , campaign_list as
-- (SELECT
-- country_code,
-- CONCAT(country_code, '-', device) AS campaign_name
-- FROM
-- UNNEST(['OM', 'KW', 'SA', 'QA', 'AE', 'BH', 'JO', 'US', 'CA', 'UK']) AS country_code,
-- UNNEST(['MWEB', 'APP', 'DESKTOP']) AS device
-- )

-- , bid_list as
-- (
--   select distinct
--     partner_property_id
--     , campaign_name
--     , campaign_type
--     , hl.country_code as hotel_country_code
--     , cl.country_code as campaign_country_code
--   from hotel_list hl
--   cross join campaign_list cl
--     -- on hl.country_code = 
-- )

-- , skyscanner as
-- (
--   select 
--     partner_property_id
--     , campaign_name
--     , SPLIT(campaign_name,"-")[SAFE_OFFSET(0)] AS site_code
--     , SPLIT(campaign_name,"-")[SAFE_OFFSET(1)] AS device_type
--     , hotel_impressions
--     , price_difference_gbp_pence_avg
--     , taxes_fees_gbp_pence_avg
--     , base_price_gbp_pence_avg
--     , bid_average_gbp_pence
--     , los_multiplier
--     , los
--     , display_rank
--     , CASE WHEN display_rank <= 4 AND campaign_name LIKE "%%APP%%" THEN 1
--       WHEN display_rank <= 2 THEN 1
--       ELSE 0 END AS main_display_ind
--     , cost_gbp_pence
--     , clicks
--     , guests
--     , check_in_date
--     , DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date
--     , EXTRACT(YEAR FROM check_in_date) AS check_in_year
--     , EXTRACT(MONTH FROM check_in_date) AS check_in_month
--     , EXTRACT(DAY FROM check_in_date) AS check_in_day
--     , DATE_DIFF(check_in_date, date, DAY) AS leadtime
--     , dow
--     , tod
--     , date
--     , EXTRACT(YEAR FROM date) AS created_at_year
--     , EXTRACT(MONTH FROM date) AS created_at_month
--     , EXTRACT(DAY FROM date) AS created_at_day
--     , SAFE_DIVIDE(bid_average_gbp_pence,(los * los_multiplier)) as avg_bid_current
--     , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg AS total_price_gbp_pence
--     , base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg AS next_cheapest_total_price_gbp_pence
--     , ROUND(100*SAFE_DIVIDE(price_difference_gbp_pence_avg,base_price_gbp_pence_avg	+ taxes_fees_gbp_pence_avg),2) AS next_cheapest_total_price_gbp_pence_pct
--     , beat_pricing_impressions
--     , meet_pricing_impressions
--     , SAFE_DIVIDE(SUM(cost_gbp_pence) OVER (PARTITION BY campaign_name, partner_property_id ORDER BY date) ,SUM(clicks) OVER (PARTITION BY campaign_name, partner_property_id ORDER BY date) * SUM(los) OVER (PARTITION BY campaign_name, partner_property_id ORDER BY date)) as ecpc
--     , SAFE_DIVIDE(SUM(cost_gbp_pence) OVER (PARTITION BY partner_property_id ORDER BY date) ,SUM(clicks) OVER (PARTITION BY partner_property_id ORDER BY date) * SUM(los) OVER (PARTITION BY partner_property_id ORDER BY date)) as ecpc_partner_property
--     , IF(date = MAX(date) OVER (PARTITION BY campaign_name, partner_property_id),1,0) AS latest_ind
--   from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
--   left join `wego-cloud.hotel_services.hotels` as b 
--     on cast(a.partner_property_id as string) = cast(b.id as string)
--   -- where b.city_code in ("DXB", "DWC", "AUH", "SHJ", "RKT", "FJR", "AAN", "HKT", "BKK")
--   -- and split(a.campaign_name, "-")[offset(0)] in ("AE", "OM","KW")
--   WHERE display_rank > 0
--   -- and date >= date(current_date() - 2) -- Take Everything
-- )

-- , skyscanner_bookings as 
-- (SELECT date, campaign_name, partner_property_id, check_in_date, check_out_date, tod,
-- SUM(bow_conversions) AS bow_conversions, 
-- SUM(wego_markup_amount_usd) AS total_wego_markup_amount_usd,
-- SUM(wego_total_price_usd) AS total_wego_total_price_usd,
-- SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
-- SUM(revenue_in_usd) as total_revenue_usd 
-- FROM
--   (SELECT date, redirect_id, campaign_name, partner_property_id, check_in_date, los, tod, DATE_ADD(check_in_date, INTERVAL los DAY) AS check_out_date,
--   FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` 
--   -- WHERE DATE(_PARTITIONTIME) BETWEEN "2025-04-25" AND "2025-09-11"
--   GROUP BY 1,2,3,4,5,6,7) as a

--   LEFT JOIN

--   (SELECT 
--   session_id, 
--   REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
--   FORMAT('%%s-%%s-%%s-%%s-%%s',
--   SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
--   SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
--   SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
--   SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
--   SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
--   ) AS decoded_redirect_id 
--   FROM `wego-cloud.wego_analytics.sessions`
--   WHERE DATE(_PARTITIONTIME) >= "2025-04-25"
--   AND landing_url LIKE "%%skyscanner%%" and ts_code = "6be92"
--   ) as b

--   ON a.redirect_id = b.decoded_redirect_id

--   INNER JOIN

--   (SELECT  
--   session_id, DATE(check_in) as check_in, DATE(check_out) as check_out, trip_duration, 
--   COUNT(DISTINCT booking_id) as bow_conversions,
--   SUM(wego_base_price_usd) AS total_wego_base_price_usd,
--   SUM(wego_markup_amount_usd) AS wego_markup_amount_usd,
--   AVG(wego_total_price_usd) AS wego_total_price_usd,
--   SUM(total_cost_of_sales_usd) AS total_cost_of_sales_usd,
--   SUM(revenue_in_usd) AS revenue_in_usd
--   FROM `wego-cloud.wego_analytics.hotels_bookings` 
--   WHERE DATE(created_at) >= "2025-04-25" -- Minimum Date to Match when Skyscanner Report Started
--   and attribution_ts_code = '6be92' -- Skyscanner Bookings
--   AND conversions_tracked > 0
--   GROUP BY 1,2,3,4) as c

--   ON b.session_id = c.session_id
-- GROUP BY 1,2,3,4,5,6
-- ),

-- -- , high_click as
-- -- (
-- --     select 
-- --         partner_property_id
-- --         , name_en
-- --         , sum(case when date >= (current_date - interval 14 day) then clicks else 0 end) as l14d_click
-- --         , sum(clicks) as total_click
-- --         , min(date) as first_live_date
-- --         , max(date) as last_click_date
-- --         , count(distinct date) as total_impression_date
-- --         , sum(hotel_impressions-avail_impressions_missed) as total_impression
-- --         , sum(cost_gbp_pence) as total_cost_gbp_pence
-- --     from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` as a
-- --     left join `wego-cloud.hotel_services.hotels` as b 
-- --             on cast(a.partner_property_id as string) = cast(b.id as string)
-- --     where display_rank > 0
-- --     group by 1,2
-- --     having (l14d_click > 10
-- --     and date_diff(current_date,first_live_date,day) >= 14
-- --     and total_impression_date >= 14)
-- --     or total_click > 15
-- -- )

-- -- , bookings as
-- -- (
-- --     select
-- --       hotel_id
-- --       , count(distinct booking_id) as total_booking
-- --     from `wego-cloud.wego_analytics.hotels_bookings` 
-- --     where date(created_at) >= date(current_date - interval 14 day)
-- --     and attribution_ts_code = '6be92'
-- --     group by 1
-- -- )

-- -- , exclude_hotel as
-- -- (
-- --     select distinct
-- --         partner_property_id
-- --         , name_en
-- --         , first_live_date
-- --         , last_click_date
-- --         , total_click
-- --         , l14d_click
-- --         , total_impression
-- --         , total_cost_gbp_pence
-- --         , total_impression_date
-- --         , coalesce(total_booking,0) as total_booking
-- --     from high_click hc
-- --     left join bookings b
-- --         on cast(hc.partner_property_id as string) = cast(b.hotel_id as string)
-- --     where b.hotel_id is null
-- -- ),

-- main_table as
-- (SELECT 
-- b.partner_property_id as bid_list_partner_property_id,
-- b.campaign_name as bid_list_campaign_name,
-- b.campaign_type as bid_list_campaign_type,
-- b.hotel_country_code as bid_list_hotel_country_code,
-- b.campaign_country_code as bid_list_campaign_country_code,
-- IF(a.partner_property_id IS NOT NULL,1,0) AS historical_performance_ind, 
-- IF(b.partner_property_id IS NOT NULL,1,0) AS bid_list_ind, 
-- a.*,
-- hotel_details.*, 
-- wego_historical_performance.* EXCEPT (hotel_id),
-- skyscanner_historical_performance.* EXCEPT (partner_property_id, campaign_name, date),
-- sb.* EXCEPT (date, partner_property_id, campaign_name, check_in_date, check_out_date, bow_conversions, tod),
-- IFNULL(bow_conversions,0) as bow_conversions
-- FROM bid_list as b
-- FULL OUTER JOIN skyscanner as a
-- ON b.partner_property_id = a.partner_property_id AND b.campaign_name = a.campaign_name
-- LEFT JOIN skyscanner_bookings as sb
-- ON sb.date = a.date AND sb.campaign_name = a.campaign_name AND sb.check_in_date = a.check_in_date AND sb.check_out_date = a.check_out_date AND sb.partner_property_id = a.partner_property_id AND sb.tod = a.tod
-- -- LEFT JOIN exclude_hotel c
-- -- on b.partner_property_id = c.partner_property_id
-- LEFT JOIN hotel_details
-- ON COALESCE(a.partner_property_id,b.partner_property_id) = hotel_details.hotel_id
-- LEFT JOIN wego_historical_performance
-- ON COALESCE(a.partner_property_id,b.partner_property_id) = wego_historical_performance.hotel_id
-- LEFT JOIN skyscanner_historical_performance
-- ON a.partner_property_id = skyscanner_historical_performance.partner_property_id 
-- AND a.campaign_name = skyscanner_historical_performance.campaign_name 
-- AND a.date = skyscanner_historical_performance.date 
-- -- WHERE c.partner_property_id is null
-- )


-- -- , property_stats as (
-- --   select 
-- --     campaign_name
-- --     , partner_property_id
-- --     , sum(display_rank * hotel_impressions) / sum(hotel_impressions) as display_rank
-- --     , sum((price_difference_gbp_pence_avg * hotel_impressions) 
-- --           / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
-- --         / sum(hotel_impressions) as avg_price_diff
-- --     , round(sum((bid_average_gbp_pence / (los * los_multiplier)) * hotel_impressions) 
-- --             / sum(hotel_impressions)) as avg_bid_current
-- --     , sum(hotel_impressions) as impressions
-- --     , sum(clicks) as clicks
-- --   from skyscanner
-- --   group by 1, 2
-- -- )

-- -- , prio_list as (
-- --   select 
-- --       *
-- --       , percent_rank()over(partition by campaign_name order by impressions) as impression_perc_rank
-- --   from (
-- --     select 
-- --         campaign_name
-- --       , partner_property_id
-- --       , sum(case when (price_difference_gbp_pence_avg * 100) 
-- --                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
-- --                    and display_rank >= 3 
-- --                  then hotel_impressions end) 
-- --           / sum(hotel_impressions) as perc_occurence
-- --       , sum(hotel_impressions) as impressions
-- --       , sum(case when (price_difference_gbp_pence_avg * 100) 
-- --                    / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg) <= 0 
-- --                    and display_rank >= 3 
-- --                  then hotel_impressions end) avg_price_diff
-- --       , avg(display_rank) as avg_rank
-- --       , min(display_rank) as min_rank
-- --       , max(display_rank) as max_rank
-- --     from skyscanner
-- --     group by 1, 2
-- --   )
-- --   group by 1, 2, 3, 4, 5, 6, 7,8
-- -- )

-- SELECT * EXCEPT (total_price_gbp_pence),
-- total_price_gbp_pence,
-- (1 - SAFE_DIVIDE(1,1+pricing_rules_markup_selected)) * total_price_gbp_pence AS markup_amount_gpb_pence,
-- SAFE_DIVIDE((1 - SAFE_DIVIDE(1,1+pricing_rules_markup_selected)) * total_price_gbp_pence,los) AS markup_amount_gpb_pence_nightly,
-- (1 - SAFE_DIVIDE(1,1+pricing_rules_markup_selected)) * base_price_gbp_pence_avg AS base_markup_amount_gpb_pence,
-- SAFE_DIVIDE((1 - SAFE_DIVIDE(1,1+pricing_rules_markup_selected)) * base_price_gbp_pence_avg,los) AS base_markup_amount_gpb_pence_nightly,
-- SAFE_DIVIDE(base_price_gbp_pence_avg,los) AS base_price_gbp_pence_avg_nightly
-- FROM
--   (SELECT *,
--   CASE WHEN pricing_engine_markup_pct_selected BETWEEN pricing_rules_first_min_markup AND pricing_rules_first_max_markup THEN pricing_engine_markup_pct_selected
--   WHEN pricing_engine_markup_pct_selected < pricing_rules_first_min_markup THEN pricing_rules_first_min_markup
--   WHEN pricing_engine_markup_pct_selected > pricing_rules_first_max_markup THEN pricing_rules_first_max_markup
--   ELSE COALESCE(pricing_engine_markup_pct_selected,pricing_rules_first_min_markup)
--   END AS pricing_rules_markup_selected,

--   CASE WHEN pricing_engine_markup_pct_selected BETWEEN pricing_rules_first_min_markup AND pricing_rules_first_max_markup THEN "NO_ACTION"
--   WHEN pricing_engine_markup_pct_selected < pricing_rules_first_min_markup THEN "MARKUP"
--   WHEN pricing_engine_markup_pct_selected > pricing_rules_first_max_markup THEN "MARKDOWN"
--   WHEN pricing_engine_markup_pct_selected IS NULL THEN "NO_PRICING_ENGINE"
--   ELSE "NO_PRICING_RULE"
--   END AS pricing_rules_action_ind,

--   CASE WHEN pricing_engine_markup_pct_selected BETWEEN pricing_rules_first_min_markup AND pricing_rules_first_max_markup THEN NULL
--   WHEN pricing_engine_markup_pct_selected < pricing_rules_first_min_markup THEN pricing_rules_first_min_markup - pricing_engine_markup_pct_selected
--   WHEN pricing_engine_markup_pct_selected > pricing_rules_first_max_markup THEN pricing_engine_markup_pct_selected - pricing_rules_first_max_markup
--   END AS pricing_engine_rules_markup_delta,
--   FROM
--     (SELECT a.* EXCEPT(pricing_rules_first_min_markup, pricing_rules_first_max_markup),
--     b.markup_percentage as markup_percentage_hotel, 
--     c.markup_percentage as markup_percentage_city, 
--     d.markup_percentage as markup_percentage_country,
--     COALESCE(b.markup_percentage,c.markup_percentage,d.markup_percentage) as pricing_engine_markup_pct_selected,
--     CAST(pricing_rules_first_min_markup AS FLOAT64) as pricing_rules_first_min_markup,
--     CAST(pricing_rules_first_max_markup AS FLOAT64) as pricing_rules_first_max_markup,
--     -- ROW_NUMBER() OVER (PARTITION BY unique_skyscanner_rn) as skyscanner_row_counter,
--     -- COUNT(*) OVER (PARTITION BY unique_skyscanner_rn) as skyscanner_total_rows,
--     FROM

--     (SELECT *,
--     -- ROW_NUMBER() OVER () AS unique_skyscanner_rn,
--     CASE WHEN device_type = "MOBILE_APP" THEN "APP"
--     WHEN device_type IN ("DESKTOP","MOBILE_WEB") THEN "WEB" END AS device_type_join 
--     FROM 
--       (SELECT *,
--       -- (SELECT MIN(CAST(JSON_VALUE(rule, '$.max_markup') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule) AS min_markup,
--       -- (SELECT MAX(CAST(JSON_VALUE(rule, '$.max_markup') AS FLOAT64)) FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule) AS max_markup,
--       -- (SELECT STRING_AGG(JSON_VALUE(rule, '$.min_markup')) FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule) AS list_min_markup,
--       -- (SELECT STRING_AGG(JSON_VALUE(rule, '$.max_markup')) FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule) AS list_max_markup,
--       -- (SELECT CAST(JSON_VALUE((SELECT ARRAY(SELECT r FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS r)[SAFE_OFFSET(0)]), '$.min_markup') AS FLOAT64)) AS pricing_rules_first_min_markup,
--       -- (SELECT CAST(JSON_VALUE((SELECT ARRAY(SELECT r FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS r)[SAFE_OFFSET(0)]), '$.max_markup') AS FLOAT64)) AS pricing_rules_first_max_markup
--       (SELECT JSON_VALUE(rule, '$.min_markup') FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule LIMIT 1) AS pricing_rules_first_min_markup,
--       (SELECT JSON_VALUE(rule, '$.max_markup') FROM UNNEST(JSON_QUERY_ARRAY(rule_struct.markup_ranges)) AS rule LIMIT 1) AS pricing_rules_first_max_markup
--       FROM
--         (SELECT 
--         bid_list_partner_property_id,
--         bid_list_campaign_name,
--         bid_list_campaign_type,
--         bid_list_hotel_country_code,
--         bid_list_campaign_country_code,
--         historical_performance_ind,
--         latest_ind,
--         bid_list_ind,
--         partner_property_id,
--         campaign_name,
--         site_code,
--         device_type,
--         hotel_impressions,
--         price_difference_gbp_pence_avg,
--         taxes_fees_gbp_pence_avg,
--         base_price_gbp_pence_avg,
--         bid_average_gbp_pence,
--         los_multiplier,
--         los,
--         display_rank,
--         main_display_ind,
--         cost_gbp_pence,
--         clicks,
--         guests,
--         check_in_date,
--         check_out_date,
--         check_in_year,
--         check_in_month,
--         check_in_day,
--         leadtime,
--         dow,
--         tod,
--         date,
--         created_at_year,
--         created_at_month,
--         created_at_day,
--         avg_bid_current,
--         total_price_gbp_pence,
--         next_cheapest_total_price_gbp_pence,
--         next_cheapest_total_price_gbp_pence_pct,
--         beat_pricing_impressions,
--         meet_pricing_impressions,
--         ecpc,
--         ecpc_partner_property,
--         hotel_id,
--         hotel_name,
--         chain_id,
--         chain_name,
--         brand_id,
--         brand_name,
--         country,
--         country_code,
--         location,
--         location_code,
--         location_id,
--         district_id,
--         district_name,
--         hotel_star_rating,
--         image_count,
--         overall_score,
--         reviews_count,
--         distance_to_city_centre,
--         property_type_id,
--         property_type_name,
--         built_year,
--         renovated_year,
--         booking_avg_reviews_score,
--         booking_reviews_count,
--         booking_0_5_reviews_count,
--         booking_5_6_reviews_count,
--         booking_6_7_reviews_count,
--         booking_7_8_reviews_count,
--         booking_8_9_reviews_count,
--         booking_9_10_reviews_count,
--         provider_count,
--         month,
--         conversions_2mth,
--         clicks_2mth,
--         converted_sessions_2mth,
--         clicked_sessions_2mth,
--         gmv_2mth,
--         impressions_2mth,
--         bookings_2mth,
--         conversions_3mth,
--         clicks_3mth,
--         converted_sessions_3mth,
--         clicked_sessions_3mth,
--         gmv_3mth,
--         impressions_3mth,
--         bookings_3mth,
--         conversions_6mth,
--         clicks_6mth,
--         converted_sessions_6mth,
--         clicked_sessions_6mth,
--         gmv_6mth,
--         impressions_6mth,
--         bookings_6mth,
--         conversions_12mth,
--         clicks_12mth,
--         converted_sessions_12mth,
--         clicked_sessions_12mth,
--         gmv_12mth,
--         impressions_12mth,
--         bookings_12mth,
--         clicks_1mth,
--         conversions_1mth,
--         clicked_sessions_1mth,
--         converted_sessions_1mth,
--         gmv_1mth,
--         impressions_1mth,
--         bookings_1mth,
--         ctr_1mth,
--         ctr_2mth,
--         ctr_3mth,
--         ctr_6mth,
--         ctr_12mth,
--         cvr_1mth,
--         cvr_2mth,
--         cvr_3mth,
--         cvr_6mth,
--         cvr_12mth,
--         converted_sessions_rate_1mth,
--         converted_sessions_rate_2mth,
--         converted_sessions_rate_3mth,
--         converted_sessions_rate_6mth,
--         converted_sessions_rate_12mth,
--         skyscanner_clicks_cumulative,
--         skyscanner_bookings_cumulative,
--         hotel_impressions_cumulative,
--         main_display_hotel_impressions_cumulative,
--         main_display_ratio_cumulative,
--         cost_gbp_pence_cumulative,
--         beat_pricing_impressions_cumulative,
--         meet_pricing_impressions_cumulative,
--         avg_display_rank_cumulative,
--         avg_price_cumulative,
--         absolute_price_difference_cumulative,
--         pct_price_difference_cumulative,
--         beat_meet_pricing_ratio_cumulative,
--         min_bid_average_gbp_pence_cumulative,
--         max_bid_average_gbp_pence_cumulative,
--         avg_bid_average_gbp_pence_cumulative,
--         total_wego_markup_amount_usd,
--         total_wego_total_price_usd,
--         total_cost_of_sales_usd,
--         total_revenue_usd,
--         bow_conversions,
--         trip_category,
--         -- MAX(rn) as rn,
--         MIN(priority) AS min_priority,
--         STRING_AGG(CAST(id AS STRING)) AS rule_id_list,
--         STRING_AGG(CAST(priority AS STRING)) AS rule_priority_list,
--         COUNT(priority) AS pricing_rules_count,
--         ARRAY_AGG(STRUCT(id, priority, markup_ranges) ORDER BY priority ASC LIMIT 1)[OFFSET(0)] AS rule_struct,
--         FROM
--           (SELECT *, 
--           -- COUNT(*) OVER (PARTITION BY rn) as block_count
--           FROM 
--             (SELECT *,
--             FROM
--             (SELECT * 
--             EXCEPT(device_type),
--             CASE WHEN device_type = "MWEB" THEN "MOBILE_WEB"
--             WHEN device_type = "APP" THEN "MOBILE_APP"
--             WHEN device_type = "DESKTOP" THEN "DESKTOP" END as device_type,
--             IF(country_code = site_code, "DOMESTIC", "INTERNATIONAL") as trip_category, 
--             -- ROW_NUMBER() OVER () as rn 
--             FROM main_table
--             ) as a
--             LEFT JOIN `wego-cloud.hotel_services.pricing_rules` as b
--             ON ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.device_types_included,'["*"]'))) OR a.device_type IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.device_types_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.device_types_excluded)) OR a.device_type NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.device_types_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.site_codes_included,'["*"]'))) OR a.site_code IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.site_codes_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.site_codes_excluded)) OR a.site_code NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.site_codes_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.user_country_codes_included,'["*"]'))) OR a.site_code IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.user_country_codes_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.user_country_codes_excluded)) OR a.site_code NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.user_country_codes_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.city_codes_included,'["*"]'))) OR a.location_code IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.city_codes_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.city_codes_excluded)) OR a.location_code NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.city_codes_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.brand_ids_included,'["*"]'))) OR CAST(a.brand_id AS STRING) IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.brand_ids_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.brand_ids_excluded)) OR CAST(a.brand_id AS STRING) NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.brand_ids_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.hotel_ids_included,'["*"]'))) OR CAST(a.hotel_id AS STRING) IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.hotel_ids_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.hotel_ids_excluded)) OR CAST(a.hotel_id AS STRING) NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.hotel_ids_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.ts_codes_included,'["*"]'))) OR '6be92' IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.ts_codes_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.ts_codes_excluded)) OR '6be92' NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.ts_codes_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.stars,'["*"]'))) OR b.stars IS NULL OR CAST(a.hotel_star_rating AS STRING) IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.stars,'["*"]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.destination_country_codes_included,'["*"]'))) OR a.country_code IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.destination_country_codes_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.destination_country_codes_excluded)) OR a.country_code NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.destination_country_codes_excluded,'[]'))))
--             AND ("*" IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.trip_categories_included,'["*"]'))) OR a.trip_category IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.trip_categories_included,'["*"]'))))
--             AND (NULL IN UNNEST(JSON_VALUE_ARRAY(b.trip_categories_excluded)) OR a.trip_category NOT IN UNNEST(JSON_VALUE_ARRAY(IFNULL(b.trip_categories_excluded,'[]'))))
--             AND (lead_time_range_from IS NULL OR (a.leadtime BETWEEN lead_time_range_from AND lead_time_range_to))
--             AND (travel_duration_range_included_from IS NULL OR (a.los BETWEEN travel_duration_range_included_from AND travel_duration_range_included_to))
--             AND (rule_validity_time_range_from IS NULL OR (a.date BETWEEN DATE(rule_validity_time_range_from) AND DATE(rule_validity_time_range_to)))
--             AND (booking_time_range_included_from IS NULL OR (a.date BETWEEN DATE(booking_time_range_included_from) AND DATE(booking_time_range_included_to)))
--             AND (created_at IS NULL OR a.date >= DATE(created_at))
--             AND (adults_count_min IS NULL OR (a.guests BETWEEN adults_count_min AND adults_count_max))
--             -- WHERE bid_list_partner_property_id = "120756" AND site_code = "AE"
--             -- AND enabled IS true
--             AND user_logged_in IS false
--             AND historical_performance_ind = 1
--             )
--           )
--         GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148
--         )
--       )
--     ) AS a

--     LEFT JOIN

--     (SELECT * EXCEPT (created_at),
--     DATE(created_at) as created_at,
--     IFNULL(LEAD(DATE(created_at)) OVER (PARTITION BY hotel_id, hotel_city_code, hotel_country_code, user_country_code, device_type ORDER BY DATE(created_at)), CURRENT_DATETIME()) AS lead_created_at,
--     CASE WHEN device_type = "android-app" THEN "APP"
--     WHEN device_type = "desktop-web" THEN "WEB" END AS device_type_join  
--     FROM `wego-cloud.pricing_engine.hotels_pricing_engine_distribution` 
--     WHERE ts_code = '6be92' AND hotel_id IS NOT NULL AND markup_percentage IS NOT NULL) AS b

--     ON a.partner_property_id = b.hotel_id AND a.site_code = b.user_country_code AND a.device_type_join = b.device_type_join AND a.date >= DATE(b.created_at) AND a.date < DATE(b.lead_created_at) 

--     LEFT JOIN

--     (SELECT * EXCEPT (created_at),
--     DATE(created_at) as created_at,
--     IFNULL(LEAD(DATE(created_at)) OVER (PARTITION BY hotel_id, hotel_city_code, hotel_country_code, user_country_code, device_type ORDER BY DATE(created_at)), CURRENT_DATETIME()) AS lead_created_at,
--     CASE WHEN device_type = "android-app" THEN "APP"
--     WHEN device_type = "desktop-web" THEN "WEB" END AS device_type_join    
--     FROM `wego-cloud.pricing_engine.hotels_pricing_engine_distribution` 
--     WHERE ts_code = '6be92' AND hotel_id IS NULL AND hotel_city_code IS NOT NULL) AS c

--     ON a.location_code = c.hotel_city_code AND a.site_code = c.user_country_code AND a.device_type_join = c.device_type_join AND a.date >= DATE(c.created_at) AND a.date < DATE(c.lead_created_at) 

--     LEFT JOIN

--     (SELECT * EXCEPT (created_at),
--     DATE(created_at) as created_at,
--     IFNULL(LEAD(DATE(created_at)) OVER (PARTITION BY hotel_id, hotel_city_code, hotel_country_code, user_country_code, device_type ORDER BY DATE(created_at)), CURRENT_DATETIME()) AS lead_created_at,
--     CASE WHEN device_type = "android-app" THEN "APP"
--     WHEN device_type = "desktop-web" THEN "WEB" END AS device_type_join    
--     FROM `wego-cloud.pricing_engine.hotels_pricing_engine_distribution` 
--     WHERE ts_code = '6be92' AND hotel_id IS NULL AND hotel_city_code IS NULL AND hotel_country_code IS NOT NULL) AS d

--     ON a.country_code = d.hotel_country_code AND a.site_code = d.user_country_code AND a.device_type_join = d.device_type_join AND a.date >= DATE(d.created_at) AND a.date < DATE(d.lead_created_at) 
--     )
--   )

-- )
-- """
-- ,date_shard_suffix
-- );


-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_bid_template%s 
-- PARTITION BY processing_date
-- CLUSTER BY campaign_name, partner_property_id
-- AS (
-- SELECT *
-- FROM
--   (SELECT 
--   a.campaign_name,
--   a.partner_property_id,
--   hotel_name,
--   -- Model Features --
--   chain_name,
--   brand_name,
--   country_code,
--   location_code,
--   district_name,
--   hotel_star_rating,
--   image_count,
--   overall_score,
--   reviews_count,
--   distance_to_city_centre,
--   property_type_name,
--   provider_count,
--   clicks_1mth,
--   conversions_1mth,
--   clicked_sessions_1mth,
--   converted_sessions_1mth,
--   gmv_1mth,
--   impressions_1mth,
--   bookings_1mth,
--   conversions_2mth,
--   clicks_2mth,
--   converted_sessions_2mth,
--   clicked_sessions_2mth,
--   gmv_2mth,
--   impressions_2mth,
--   bookings_2mth,
--   conversions_3mth,
--   clicks_3mth,
--   converted_sessions_3mth,
--   clicked_sessions_3mth,
--   gmv_3mth,
--   impressions_3mth,
--   bookings_3mth,
--   conversions_6mth,
--   clicks_6mth,
--   converted_sessions_6mth,
--   clicked_sessions_6mth,
--   gmv_6mth,
--   impressions_6mth,
--   bookings_6mth,
--   conversions_12mth,
--   clicks_12mth,
--   converted_sessions_12mth,
--   clicked_sessions_12mth,
--   gmv_12mth,
--   impressions_12mth,
--   bookings_12mth,
--   ctr_1mth,
--   ctr_2mth,
--   ctr_3mth,
--   ctr_6mth,
--   ctr_12mth,
--   cvr_1mth,
--   cvr_2mth,
--   cvr_3mth,
--   cvr_6mth,
--   cvr_12mth,
--   converted_sessions_rate_1mth,
--   converted_sessions_rate_2mth,
--   converted_sessions_rate_3mth,
--   converted_sessions_rate_6mth,
--   converted_sessions_rate_12mth, 
--   -- If it's part of a bid list, but there's no historical skyscanner performance data, take the global average to use as a basis to predict against the model
--   -- Priority a = historical performance, c = campaign level performance fallback, b = global performance fallback
--   a.site_code,
--   a.device_type,
--   COALESCE(a.los, c.los, b.los) AS los,
--   COALESCE(a.guests, c.guests, b.guests) AS guests,
--   COALESCE(a.created_at_year, c.created_at_year, b.created_at_year) AS created_at_year,
--   COALESCE(a.created_at_month, c.created_at_month, b.created_at_month) AS created_at_month,
--   COALESCE(a.created_at_day, c.created_at_day, b.created_at_day) AS created_at_day,
--   COALESCE(a.check_in_year, c.check_in_year, b.check_in_year) AS check_in_year,
--   COALESCE(a.check_in_month, c.check_in_month, b.check_in_month) AS check_in_month,
--   COALESCE(a.check_in_day, c.check_in_day, b.check_in_day) AS check_in_day,
--   COALESCE(a.leadtime, c.leadtime, b.leadtime) AS leadtime,
--   COALESCE(a.dow, c.dow, b.dow) AS dow,
--   COALESCE(a.tod, c.tod, b.tod) AS tod,
--   COALESCE(a.skyscanner_clicks_cumulative, c.skyscanner_clicks_cumulative, b.skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
--   COALESCE(a.skyscanner_bookings_cumulative, c.skyscanner_bookings_cumulative, b.skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
--   COALESCE(a.hotel_impressions_cumulative, c.hotel_impressions_cumulative, b.hotel_impressions_cumulative) AS hotel_impressions_cumulative,
--   COALESCE(a.main_display_hotel_impressions_cumulative, c.main_display_hotel_impressions_cumulative, b.main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
--   COALESCE(a.main_display_ratio_cumulative, c.main_display_ratio_cumulative, b.main_display_ratio_cumulative) AS main_display_ratio_cumulative,
--   COALESCE(a.cost_gbp_pence_cumulative, c.cost_gbp_pence_cumulative, b.cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
--   COALESCE(a.beat_pricing_impressions_cumulative, c.beat_pricing_impressions_cumulative, b.beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
--   COALESCE(a.meet_pricing_impressions_cumulative, c.meet_pricing_impressions_cumulative, b.meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
--   COALESCE(a.avg_display_rank_cumulative, c.avg_display_rank_cumulative, b.avg_display_rank_cumulative) AS avg_display_rank_cumulative,
--   COALESCE(a.avg_price_cumulative, c.avg_price_cumulative, b.avg_price_cumulative) AS avg_price_cumulative,
--   COALESCE(a.absolute_price_difference_cumulative, c.absolute_price_difference_cumulative, b.absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
--   COALESCE(a.pct_price_difference_cumulative, c.pct_price_difference_cumulative, b.pct_price_difference_cumulative) AS pct_price_difference_cumulative,
--   COALESCE(a.beat_meet_pricing_ratio_cumulative, c.beat_meet_pricing_ratio_cumulative, b.beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
--   COALESCE(a.min_bid_average_gbp_pence_cumulative, c.min_bid_average_gbp_pence_cumulative, b.min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
--   COALESCE(a.max_bid_average_gbp_pence_cumulative, c.max_bid_average_gbp_pence_cumulative, b.max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
--   COALESCE(a.avg_bid_average_gbp_pence_cumulative, c.avg_bid_average_gbp_pence_cumulative, b.avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
--   COALESCE(a.display_rank, c.display_rank, b.display_rank) AS display_rank,
--   COALESCE(a.main_display_ind, c.main_display_ind, b.main_display_ind) AS main_display_ind,
--   COALESCE(a.price_difference_gbp_pence_avg, c.price_difference_gbp_pence_avg, b.price_difference_gbp_pence_avg) AS price_difference_gbp_pence_avg,
--   COALESCE(a.total_price_gbp_pence, c.total_price_gbp_pence, b.total_price_gbp_pence) AS total_price_gbp_pence,
--   COALESCE(a.next_cheapest_total_price_gbp_pence, c.next_cheapest_total_price_gbp_pence, b.next_cheapest_total_price_gbp_pence) AS next_cheapest_total_price_gbp_pence,
--   COALESCE(a.next_cheapest_total_price_gbp_pence_pct, c.next_cheapest_total_price_gbp_pence_pct, b.next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
--   COALESCE(a.ecpc, c.ecpc, b.ecpc) AS ecpc,
--   COALESCE(a.ecpc_partner_property, c.ecpc_partner_property, b.ecpc_partner_property) AS ecpc_partner_property,
--   COALESCE(a.pricing_rules_markup_selected, c.pricing_rules_markup_selected, b.pricing_rules_markup_selected) AS pricing_rules_markup_selected,
--   COALESCE(a.markup_amount_gpb_pence, c.markup_amount_gpb_pence, b.markup_amount_gpb_pence) AS markup_amount_gpb_pence,
--   COALESCE(a.markup_amount_gpb_pence_nightly, c.markup_amount_gpb_pence_nightly, b.markup_amount_gpb_pence_nightly) AS markup_amount_gpb_pence_nightly,
--   COALESCE(a.base_markup_amount_gpb_pence, c.base_markup_amount_gpb_pence, b.base_markup_amount_gpb_pence) AS base_markup_amount_gpb_pence,
--   COALESCE(a.base_markup_amount_gpb_pence_nightly, c.base_markup_amount_gpb_pence_nightly, b.base_markup_amount_gpb_pence_nightly) AS base_markup_amount_gpb_pence_nightly,
--   COALESCE(a.base_price_gbp_pence_avg_nightly, c.base_price_gbp_pence_avg_nightly, b.base_price_gbp_pence_avg_nightly) AS base_price_gbp_pence_avg_nightly,
--   COALESCE(a.latest_search_pattern_count, c.latest_search_pattern_count, b.latest_search_pattern_count) AS latest_search_pattern_count,
--   COALESCE(a.avg_los, c.avg_los, b.avg_los) AS avg_los,
--   COALESCE(a.med_los, c.med_los, b.med_los) AS med_los,
--   CURRENT_DATE() AS processing_date
--   FROM
--     (SELECT 
--     COALESCE(bid_list_campaign_name, campaign_name) AS campaign_name,
--     COALESCE(bid_list_partner_property_id, partner_property_id) AS partner_property_id,
--     hotel_name,
--     -- Model Features --
--     COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(0)], campaign_name) AS site_code,
--     COALESCE(SPLIT(bid_list_campaign_name,"-")[SAFE_OFFSET(1)], campaign_name) AS device_type,
--     chain_name,
--     brand_name,
--     country_code,
--     location_code,
--     district_name,
--     hotel_star_rating,
--     image_count,
--     overall_score,
--     reviews_count,
--     distance_to_city_centre,
--     property_type_name,
--     provider_count,
--     clicks_1mth,
--     conversions_1mth,
--     clicked_sessions_1mth,
--     converted_sessions_1mth,
--     gmv_1mth,
--     impressions_1mth,
--     bookings_1mth,
--     conversions_2mth,
--     clicks_2mth,
--     converted_sessions_2mth,
--     clicked_sessions_2mth,
--     gmv_2mth,
--     impressions_2mth,
--     bookings_2mth,
--     conversions_3mth,
--     clicks_3mth,
--     converted_sessions_3mth,
--     clicked_sessions_3mth,
--     gmv_3mth,
--     impressions_3mth,
--     bookings_3mth,
--     conversions_6mth,
--     clicks_6mth,
--     converted_sessions_6mth,
--     clicked_sessions_6mth,
--     gmv_6mth,
--     impressions_6mth,
--     bookings_6mth,
--     conversions_12mth,
--     clicks_12mth,
--     converted_sessions_12mth,
--     clicked_sessions_12mth,
--     gmv_12mth,
--     impressions_12mth,
--     bookings_12mth,
--     ctr_1mth,
--     ctr_2mth,
--     ctr_3mth,
--     ctr_6mth,
--     ctr_12mth,
--     cvr_1mth,
--     cvr_2mth,
--     cvr_3mth,
--     cvr_6mth,
--     cvr_12mth,
--     converted_sessions_rate_1mth,
--     converted_sessions_rate_2mth,
--     converted_sessions_rate_3mth,
--     converted_sessions_rate_6mth,
--     converted_sessions_rate_12mth,
--     -- Take AVG of the general search 
--     CAST(AVG(los) AS INT64) AS los,
--     CAST(AVG(guests) AS INT64) AS guests,
--     MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
--     MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
--     MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
--     CAST(AVG(check_in_year) AS INT64) AS check_in_year,
--     CAST(AVG(check_in_month) AS INT64) AS check_in_month,
--     CAST(AVG(check_in_day) AS INT64) AS check_in_day,
--     CAST(AVG(leadtime) AS INT64) AS leadtime,
--     CAST(AVG(dow) AS INT64) AS dow,
--     CAST(AVG(tod) AS INT64) AS tod,
--     -- MAX to take the latest performance to predict forward
--     MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
--     MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
--     MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
--     MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
--     MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
--     MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
--     MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
--     MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
--     MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
--     MAX(avg_price_cumulative) AS avg_price_cumulative,
--     MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
--     MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
--     MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
--     MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
--     MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
--     MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
--     -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
--     CAST(AVG(display_rank) AS INT64) AS display_rank,
--     1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
--     CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
--     CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
--     CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
--     AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
--     -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
--     -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
--     -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
--     -- Check Indicator of Whether There was Historical Skyscanner Performance
--     MAX(historical_performance_ind) AS historical_performance_ind,
--     AVG(IF(latest_ind = 1, ecpc, NULL)) AS ecpc,
--     AVG(IF(latest_ind = 1, ecpc_partner_property, NULL)) AS ecpc_partner_property,
--     AVG(IF(latest_ind = 1, pricing_rules_markup_selected, NULL)) AS pricing_rules_markup_selected,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence, NULL)) AS markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence_nightly, NULL)) AS markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence, NULL)) AS base_markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence_nightly, NULL)) AS base_markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_price_gbp_pence_avg_nightly, NULL)) AS base_price_gbp_pence_avg_nightly,
--     COUNTIF(latest_ind = 1) as latest_search_pattern_count,
--     AVG(los) as avg_los,
--     APPROX_QUANTILES(los,2)[SAFE_OFFSET(1)] as med_los
--     FROM `skyscanner_bidding.v3_skyscanner_raw_data%s`
--     WHERE bid_list_ind = 1
--     GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67) as a

--     CROSS JOIN

--     -- Generate One Row of a Global Dummy Average for Hotels without any Historical Performance
--     (SELECT 
--     -- Take AVG of the general search 
--     CAST(AVG(los) AS INT64) AS los,
--     CAST(AVG(guests) AS INT64) AS guests,
--     MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
--     MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
--     MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
--     CAST(AVG(check_in_year) AS INT64) AS check_in_year,
--     CAST(AVG(check_in_month) AS INT64) AS check_in_month,
--     CAST(AVG(check_in_day) AS INT64) AS check_in_day,
--     CAST(AVG(leadtime) AS INT64) AS leadtime,
--     CAST(AVG(dow) AS INT64) AS dow,
--     CAST(AVG(tod) AS INT64) AS tod,
--     -- MAX to take the latest performance to predict forward
--     MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
--     MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
--     MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
--     MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
--     MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
--     MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
--     MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
--     MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
--     MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
--     MAX(avg_price_cumulative) AS avg_price_cumulative,
--     MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
--     MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
--     MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
--     MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
--     MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
--     MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
--     -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
--     CAST(AVG(display_rank) AS INT64) AS display_rank,
--     1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
--     CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
--     CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
--     CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
--     AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
--     -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
--     -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
--     -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
--     AVG(IF(latest_ind = 1, ecpc, NULL)) AS ecpc,
--     AVG(IF(latest_ind = 1, ecpc_partner_property, NULL)) AS ecpc_partner_property,
--     AVG(IF(latest_ind = 1, pricing_rules_markup_selected, NULL)) AS pricing_rules_markup_selected,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence, NULL)) AS markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence_nightly, NULL)) AS markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence, NULL)) AS base_markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence_nightly, NULL)) AS base_markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_price_gbp_pence_avg_nightly, NULL)) AS base_price_gbp_pence_avg_nightly,
--     COUNTIF(latest_ind = 1) as latest_search_pattern_count,
--     AVG(los) as avg_los,
--     APPROX_QUANTILES(los,2)[SAFE_OFFSET(1)] as med_los
--     FROM `skyscanner_bidding.v3_skyscanner_raw_data%s`
--     WHERE historical_performance_ind = 1) as b 

--     LEFT JOIN

--     -- Generate One Row of a Campaign Name Level Dummy Average for Hotels without any Historical Performance
--     (SELECT 
--     COALESCE(bid_list_campaign_name, campaign_name) as campaign_name,
--     -- Take AVG of the general search 
--     CAST(AVG(los) AS INT64) AS los,
--     CAST(AVG(guests) AS INT64) AS guests,
--     MAX(EXTRACT(YEAR FROM CURRENT_DATE())) AS created_at_year,
--     MAX(EXTRACT(MONTH FROM CURRENT_DATE())) AS created_at_month,
--     MAX(EXTRACT(DAY FROM CURRENT_DATE())) AS created_at_day,
--     CAST(AVG(check_in_year) AS INT64) AS check_in_year,
--     CAST(AVG(check_in_month) AS INT64) AS check_in_month,
--     CAST(AVG(check_in_day) AS INT64) AS check_in_day,
--     CAST(AVG(leadtime) AS INT64) AS leadtime,
--     CAST(AVG(dow) AS INT64) AS dow,
--     CAST(AVG(tod) AS INT64) AS tod,
--     -- MAX to take the latest performance to predict forward
--     MAX(skyscanner_clicks_cumulative) AS skyscanner_clicks_cumulative,
--     MAX(skyscanner_bookings_cumulative) AS skyscanner_bookings_cumulative,
--     MAX(hotel_impressions_cumulative) AS hotel_impressions_cumulative,
--     MAX(main_display_hotel_impressions_cumulative) AS main_display_hotel_impressions_cumulative,
--     MAX(main_display_ratio_cumulative) AS main_display_ratio_cumulative,
--     MAX(cost_gbp_pence_cumulative) AS cost_gbp_pence_cumulative,
--     MAX(beat_pricing_impressions_cumulative) AS beat_pricing_impressions_cumulative,
--     MAX(meet_pricing_impressions_cumulative) AS meet_pricing_impressions_cumulative,
--     MAX(avg_display_rank_cumulative) AS avg_display_rank_cumulative,
--     MAX(avg_price_cumulative) AS avg_price_cumulative,
--     MAX(absolute_price_difference_cumulative) AS absolute_price_difference_cumulative,
--     MAX(pct_price_difference_cumulative) AS pct_price_difference_cumulative,
--     MAX(beat_meet_pricing_ratio_cumulative) AS beat_meet_pricing_ratio_cumulative,
--     MAX(min_bid_average_gbp_pence_cumulative) AS min_bid_average_gbp_pence_cumulative,
--     MAX(max_bid_average_gbp_pence_cumulative) AS max_bid_average_gbp_pence_cumulative,
--     MAX(avg_bid_average_gbp_pence_cumulative) AS avg_bid_average_gbp_pence_cumulative,
--     -- Use the AVG for all these input features that can vary, as you won't know precisely for each scenario to bid for 
--     CAST(AVG(display_rank) AS INT64) AS display_rank,
--     1 AS main_display_ind, -- Want to predict results based on that it made it to the main display (not in view more)
--     CAST(AVG(price_difference_gbp_pence_avg) AS INT64) AS price_difference_gbp_pence_avg,
--     CAST(AVG(total_price_gbp_pence) AS INT64) AS total_price_gbp_pence,
--     CAST(AVG(next_cheapest_total_price_gbp_pence) AS INT64) AS next_cheapest_total_price_gbp_pence,
--     AVG(next_cheapest_total_price_gbp_pence_pct) AS next_cheapest_total_price_gbp_pence_pct,
--     -- CAST(AVG(cost_gbp_pence) AS INT64) AS cost_gbp_pence,
--     -- CAST(AVG(beat_pricing_impressions) AS INT64) AS beat_pricing_impressions,
--     -- CAST(AVG(meet_pricing_impressions) AS INT64) AS meet_pricing_impressions,
--     AVG(IF(latest_ind = 1, ecpc, NULL)) AS ecpc,
--     AVG(IF(latest_ind = 1, ecpc_partner_property, NULL)) AS ecpc_partner_property,
--     AVG(IF(latest_ind = 1, pricing_rules_markup_selected, NULL)) AS pricing_rules_markup_selected,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence, NULL)) AS markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, markup_amount_gpb_pence_nightly, NULL)) AS markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence, NULL)) AS base_markup_amount_gpb_pence,
--     AVG(IF(latest_ind = 1, base_markup_amount_gpb_pence_nightly, NULL)) AS base_markup_amount_gpb_pence_nightly,
--     AVG(IF(latest_ind = 1, base_price_gbp_pence_avg_nightly, NULL)) AS base_price_gbp_pence_avg_nightly,
--     COUNTIF(latest_ind = 1) as latest_search_pattern_count,
--     AVG(los) as avg_los,
--     APPROX_QUANTILES(los,2)[SAFE_OFFSET(1)] as med_los
--     FROM `skyscanner_bidding.v3_skyscanner_raw_data%s`
--     WHERE historical_performance_ind = 1
--     GROUP BY 1
--     ) as c

--     ON a.campaign_name = c.campaign_name

--   ),UNNEST(GENERATE_ARRAY(20,135,5)) as mock_bid
-- )
-- """
-- ,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
-- );

-- -- Daisy Chain: Get Predictions from Click Model, then from the Conversions Model
-- -- Profit = (predicted_conversions[Conversions Model] * total_price_gbp_pence * markup_percentage) - (predicted_clicks[Click Model] * ecpc + 0.023[Estimated Blended Gateway Fee] * total_price_gbp_pence)
-- -- No Guarantee that predicted conversions is less than predicted clicks, as it's from two separate models. Use LEAST(predicted_bow_conversions,predicted_clicks) to ensure that predicted conversions is within predicted clicks, CVR <= 100%
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_training_data_explain_predict%s 
-- PARTITION BY processing_date
-- CLUSTER BY campaign_name, partner_property_id
-- AS (
-- SELECT *,
-- DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY profit DESC) as prediction_ranking,
-- -- Less Precise on the Decimal Places of Prediction Value (So that more consideration among lower bid higher predicted click cases)
-- -- DENSE_RANK() OVER (PARTITION BY campaign_name, partner_property_id ORDER BY prediction_value_exponent DESC, ROUND(prediction_value_mantissa,1) DESC) as prediction_ranking_blunt, 
-- CURRENT_DATE() AS processing_date
-- FROM
-- (SELECT *,
-- (IFNULL(LEAST(predicted_bow_conversions,predicted_clicks),0) * IFNULL(base_markup_amount_gpb_pence_nightly,0)) - (IFNULL(predicted_clicks,0) * IFNULL(ecpc,0) + 0.025 * IFNULL(base_price_gbp_pence_avg_nightly,0)) AS profit,
-- ((IFNULL(LEAST(predicted_bow_conversions,predicted_clicks),0) * IFNULL(base_markup_amount_gpb_pence_nightly,0) * IFNULL(avg_los,0)) - (IFNULL(predicted_clicks,0) * IFNULL(ecpc,0) + 0.025 * IFNULL(base_price_gbp_pence_avg_nightly,0) * IFNULL(avg_los,0))) * IFNULL(latest_search_pattern_count,0) AS volume_profit
-- FROM
--   (SELECT * 
--   FROM ML.PREDICT(MODEL `skyscanner_bidding.v2_rf_model%s`,
--   (SELECT
--   campaign_name,
--   partner_property_id,
--   hotel_name,
--   ecpc,
--   ecpc_partner_property,
--   pricing_rules_markup_selected,
--   markup_amount_gpb_pence,
--   markup_amount_gpb_pence_nightly,
--   base_markup_amount_gpb_pence,
--   base_markup_amount_gpb_pence_nightly,
--   base_price_gbp_pence_avg_nightly,
--   latest_search_pattern_count,
--   avg_los,
--   med_los,
--   predicted_clicks,
--   -- Model Features --
--   chain_name,
--   brand_name,
--   country_code,
--   location_code,
--   district_name,
--   hotel_star_rating,
--   image_count,
--   overall_score,
--   reviews_count,
--   distance_to_city_centre,
--   property_type_name,
--   provider_count,
--   clicks_1mth,
--   conversions_1mth,
--   clicked_sessions_1mth,
--   converted_sessions_1mth,
--   gmv_1mth,
--   impressions_1mth,
--   bookings_1mth,
--   conversions_2mth,
--   clicks_2mth,
--   converted_sessions_2mth,
--   clicked_sessions_2mth,
--   gmv_2mth,
--   impressions_2mth,
--   bookings_2mth,
--   conversions_3mth,
--   clicks_3mth,
--   converted_sessions_3mth,
--   clicked_sessions_3mth,
--   gmv_3mth,
--   impressions_3mth,
--   bookings_3mth,
--   conversions_6mth,
--   clicks_6mth,
--   converted_sessions_6mth,
--   clicked_sessions_6mth,
--   gmv_6mth,
--   impressions_6mth,
--   bookings_6mth,
--   conversions_12mth,
--   clicks_12mth,
--   converted_sessions_12mth,
--   clicked_sessions_12mth,
--   gmv_12mth,
--   impressions_12mth,
--   bookings_12mth,
--   ctr_1mth,
--   ctr_2mth,
--   ctr_3mth,
--   ctr_6mth,
--   ctr_12mth,
--   cvr_1mth,
--   cvr_2mth,
--   cvr_3mth,
--   cvr_6mth,
--   cvr_12mth,
--   converted_sessions_rate_1mth,
--   converted_sessions_rate_2mth,
--   converted_sessions_rate_3mth,
--   converted_sessions_rate_6mth,
--   converted_sessions_rate_12mth,
--   site_code,
--   device_type,
--   los,
--   guests,
--   created_at_year,
--   created_at_month,
--   created_at_day,
--   check_in_year,
--   check_in_month,
--   check_in_day,
--   leadtime,
--   dow,
--   tod,
--   skyscanner_clicks_cumulative,
--   skyscanner_bookings_cumulative,
--   hotel_impressions_cumulative,
--   main_display_hotel_impressions_cumulative,
--   main_display_ratio_cumulative,
--   cost_gbp_pence_cumulative,
--   beat_pricing_impressions_cumulative,
--   meet_pricing_impressions_cumulative,
--   avg_display_rank_cumulative,
--   avg_price_cumulative,
--   absolute_price_difference_cumulative,
--   pct_price_difference_cumulative,
--   beat_meet_pricing_ratio_cumulative,
--   min_bid_average_gbp_pence_cumulative,
--   max_bid_average_gbp_pence_cumulative,
--   avg_bid_average_gbp_pence_cumulative,
--   display_rank,
--   main_display_ind,
--   price_difference_gbp_pence_avg,
--   total_price_gbp_pence,
--   next_cheapest_total_price_gbp_pence,
--   next_cheapest_total_price_gbp_pence_pct,
--   -- cost_gbp_pence,
--   -- beat_pricing_impressions,
--   -- meet_pricing_impressions,
--   avg_bid_current,
--     FROM
--     (SELECT
--     -- input.*,
--     -- predoutput.*
--     predoutput.* ,
--     -- predoutput.predicted_total_itinerary_card_clicks,
--     -- predoutput.total_itinerary_card_clicks_probs
--     -- FLOOR(LOG10(predicted_clicks)) AS predicted_clicks_exponent,
--     -- SAFE_DIVIDE(predicted_clicks,POWER(10, FLOOR(LOG10(predicted_clicks)))) AS predicted_clicks_mantissa
--     FROM
--     ML.PREDICT(MODEL `skyscanner_bidding.v1_rf_model%s`,
--     (SELECT
--     campaign_name,
--     partner_property_id,
--     hotel_name,
--     ecpc,
--     ecpc_partner_property,
--     pricing_rules_markup_selected,
--     markup_amount_gpb_pence,
--     markup_amount_gpb_pence_nightly,
--     base_markup_amount_gpb_pence,
--     base_markup_amount_gpb_pence_nightly,
--     base_price_gbp_pence_avg_nightly,
--     latest_search_pattern_count,
--     avg_los,
--     med_los,
--     -- Model Features --
--     chain_name,
--     brand_name,
--     country_code,
--     location_code,
--     district_name,
--     hotel_star_rating,
--     image_count,
--     overall_score,
--     reviews_count,
--     distance_to_city_centre,
--     property_type_name,
--     provider_count,
--     clicks_1mth,
--     conversions_1mth,
--     clicked_sessions_1mth,
--     converted_sessions_1mth,
--     gmv_1mth,
--     impressions_1mth,
--     bookings_1mth,
--     conversions_2mth,
--     clicks_2mth,
--     converted_sessions_2mth,
--     clicked_sessions_2mth,
--     gmv_2mth,
--     impressions_2mth,
--     bookings_2mth,
--     conversions_3mth,
--     clicks_3mth,
--     converted_sessions_3mth,
--     clicked_sessions_3mth,
--     gmv_3mth,
--     impressions_3mth,
--     bookings_3mth,
--     conversions_6mth,
--     clicks_6mth,
--     converted_sessions_6mth,
--     clicked_sessions_6mth,
--     gmv_6mth,
--     impressions_6mth,
--     bookings_6mth,
--     conversions_12mth,
--     clicks_12mth,
--     converted_sessions_12mth,
--     clicked_sessions_12mth,
--     gmv_12mth,
--     impressions_12mth,
--     bookings_12mth,
--     ctr_1mth,
--     ctr_2mth,
--     ctr_3mth,
--     ctr_6mth,
--     ctr_12mth,
--     cvr_1mth,
--     cvr_2mth,
--     cvr_3mth,
--     cvr_6mth,
--     cvr_12mth,
--     converted_sessions_rate_1mth,
--     converted_sessions_rate_2mth,
--     converted_sessions_rate_3mth,
--     converted_sessions_rate_6mth,
--     converted_sessions_rate_12mth,
--     site_code,
--     device_type,
--     los,
--     guests,
--     created_at_year,
--     created_at_month,
--     created_at_day,
--     check_in_year,
--     check_in_month,
--     check_in_day,
--     leadtime,
--     dow,
--     tod,
--     skyscanner_clicks_cumulative,
--     skyscanner_bookings_cumulative,
--     hotel_impressions_cumulative,
--     main_display_hotel_impressions_cumulative,
--     main_display_ratio_cumulative,
--     cost_gbp_pence_cumulative,
--     beat_pricing_impressions_cumulative,
--     meet_pricing_impressions_cumulative,
--     avg_display_rank_cumulative,
--     avg_price_cumulative,
--     absolute_price_difference_cumulative,
--     pct_price_difference_cumulative,
--     beat_meet_pricing_ratio_cumulative,
--     min_bid_average_gbp_pence_cumulative,
--     max_bid_average_gbp_pence_cumulative,
--     avg_bid_average_gbp_pence_cumulative,
--     display_rank,
--     main_display_ind,
--     price_difference_gbp_pence_avg,
--     total_price_gbp_pence,
--     next_cheapest_total_price_gbp_pence,
--     next_cheapest_total_price_gbp_pence_pct,
--     -- cost_gbp_pence,
--     -- beat_pricing_impressions,
--     -- meet_pricing_impressions,
--     mock_bid as avg_bid_current,
--     FROM `skyscanner_bidding.v3_bid_template%s`
--     -- LIMIT 10
--     -- WHERE campaign_name = "SA-DESKTOP"	
--     -- AND partner_property_id = "2921357"
--     )
--     -- ,
--     -- STRUCT(TRUE AS approx_feature_contrib)
--     )
--     predoutput 
--   )
--   )
--   )
--   )
-- )
-- )
-- """
-- ,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
-- );



-- -- -- --------- SELECT BEST BID BASED ON HIGHEST PREDICTED VALUE (Profit) ---------
-- --- Based on each combined click + conversion = profit prediction from the model, if there's a tie for 1st place, take the lowest bid needed to get a max predicted profit
-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_bid_selection%s AS (
-- SELECT *, 
-- -- LEAST(avg_bid_current,135) 
-- IF(profit <= 0, 20, LEAST(avg_bid_current,135)) as bid,
-- FROM
--   (SELECT campaign_name, partner_property_id, MIN(avg_bid_current) as avg_bid_current, MIN(profit) as profit
--   FROM `skyscanner_bidding.v3_training_data_explain_predict%s`
--   WHERE prediction_ranking = 1
--   GROUP BY 1,2)
-- )
-- """
-- ,date_shard_suffix,date_shard_suffix
-- );


-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_union_group%s AS (
-- SELECT
--   partner_property_id
--   , campaign_name
--   , least(bid,135) as bid
-- FROM `skyscanner_bidding.v3_bid_selection%s`
-- UNION ALL
-- SELECT 
--   b.partner_property_id, 
--   a.campaign_name, 
--   1 AS bid 
-- FROM (
--   SELECT DISTINCT CONCAT(campaign_name, "-", "GROUP") AS campaign_name 
--   FROM `skyscanner_bidding.v3_bid_selection%s`
-- ) AS a
-- JOIN (
--   SELECT DISTINCT partner_property_id 
--   FROM `skyscanner_bidding.v3_bid_selection%s`
-- ) AS b ON 1=1
-- ORDER BY campaign_name, partner_property_id
-- )
-- """
-- ,date_shard_suffix,date_shard_suffix,date_shard_suffix,date_shard_suffix
-- );

-- EXECUTE IMMEDIATE FORMAT("""
-- CREATE OR REPLACE TABLE skyscanner_bidding.v3_bid_file%s AS (
-- SELECT *
-- FROM (
--   SELECT
--     partner_property_id as `Hotel ID`,
--     campaign_name,
--     bid
--   FROM `skyscanner_bidding.v3_union_group%s`
-- )
-- PIVOT (
--   SUM(bid) FOR campaign_name IN (
--     'AE-APP'
--     , 'AE-APP-GROUP'
--     , 'AE-DESKTOP'
--     , 'AE-DESKTOP-GROUP'
--     , 'AE-MWEB'
--     , 'AE-MWEB-GROUP'
--     , 'BH-APP'
--     , 'BH-APP-GROUP'
--     , 'BH-DESKTOP'
--     , 'BH-DESKTOP-GROUP'
--     , 'BH-MWEB'
--     , 'BH-MWEB-GROUP'
--     , 'CA-APP'
--     , 'CA-APP-GROUP'
--     , 'CA-DESKTOP'
--     , 'CA-DESKTOP-GROUP'
--     , 'CA-MWEB'
--     , 'CA-MWEB-GROUP'
--     , 'UK-APP'
--     , 'UK-APP-GROUP'
--     , 'UK-DESKTOP'
--     , 'UK-DESKTOP-GROUP'
--     , 'UK-MWEB'
--     , 'UK-MWEB-GROUP'
--     , 'JO-APP'
--     , 'JO-APP-GROUP'
--     , 'JO-DESKTOP'
--     , 'JO-DESKTOP-GROUP'
--     , 'JO-MWEB'
--     , 'JO-MWEB-GROUP'
--     , 'KW-APP'
--     , 'KW-APP-GROUP'
--     , 'KW-DESKTOP'
--     , 'KW-DESKTOP-GROUP'
--     , 'KW-MWEB'
--     , 'KW-MWEB-GROUP'
--     , 'OM-APP'
--     , 'OM-APP-GROUP'
--     , 'OM-DESKTOP'
--     , 'OM-DESKTOP-GROUP'
--     , 'OM-MWEB'
--     , 'OM-MWEB-GROUP'
--     , 'QA-APP'
--     , 'QA-APP-GROUP'
--     , 'QA-DESKTOP'
--     , 'QA-DESKTOP-GROUP'
--     , 'QA-MWEB'
--     , 'QA-MWEB-GROUP'
--     , 'SA-APP'
--     , 'SA-APP-GROUP'
--     , 'SA-DESKTOP'
--     , 'SA-DESKTOP-GROUP'
--     , 'SA-MWEB'
--     , 'SA-MWEB-GROUP'
--     , 'US-APP'
--     , 'US-APP-GROUP'
--     , 'US-DESKTOP'
--     , 'US-DESKTOP-GROUP'
--     , 'US-MWEB'
--     , 'US-MWEB-GROUP'
-- ))
-- )
-- """
-- ,date_shard_suffix,date_shard_suffix
-- );






------------------------ DELETE OLDER TABLES ------------------------
---------- Maintain the past 60 tables, delete the rest -------------
SET (table_list, table_count) = (
SELECT AS STRUCT ARRAY_AGG(table_id) as table_list, COUNT(*) as table_count
FROM (
  SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY base_table_id ORDER BY creation_time DESC) AS rn FROM
    (SELECT *, 
    REGEXP_REPLACE(table_id, r'[0-9]{8}$', '') AS base_table_id,
    FROM `wego-cloud.skyscanner_bidding.__TABLES__`
    WHERE type = 1
    AND (
    table_id LIKE 'bid_file%'
    OR table_id LIKE 'bid_selection%'
    OR table_id LIKE 'bid_template%'
    OR table_id LIKE 'rf_model_evaluation%'
    OR table_id LIKE 'rf_model_feature_importance%'
    OR table_id LIKE 'rf_model_global_explain%'
    OR table_id LIKE 'skyscanner_raw_data%'
    OR table_id LIKE 'training_data%'
    OR table_id LIKE 'training_data_downsample%'
    OR table_id LIKE 'training_data_explain_predict%'
    OR table_id LIKE 'union_group%'
    OR table_id LIKE 'v1_%'
    OR table_id LIKE 'v2_%'
    OR table_id LIKE 'v3_%'
    )
    )
  )
  WHERE rn > 60 
  ORDER BY table_id, creation_time
  )
); 


LOOP
  IF j >= table_count THEN
    LEAVE;
  END IF;
    SET table_name = table_list[OFFSET(j)];
    EXECUTE IMMEDIATE FORMAT("""DROP TABLE `wego-cloud.skyscanner_bidding.%s`""", table_name);
    SET j = j + 1;
END LOOP;




-- -- , initial_bid_calc as (
-- --   select 
-- --     campaign_name
-- --     , partner_property_id
-- --     , avg_price_diff
-- --     , display_rank
-- --     , avg_bid_current
-- --     , case 
-- --         when avg_price_diff >= 7 then 20
-- --         when display_rank > 4 and avg_price_diff <= 0 then 80
-- --         when display_rank > 4 and avg_price_diff <= 5 then 55
-- --         when display_rank <= 2 and avg_price_diff <= 0 then 45
-- --         when display_rank <= 4 and avg_price_diff <= 0 then 60
-- --         else 45 
-- --       end as bid
-- --       , impressions
-- --   from property_stats
-- --   group by 1, 2, 3, 4, 5, 6,7
-- -- )

-- -- , adjusted_bid as (
-- --   select 
-- --       a.campaign_name
-- --     , a.partner_property_id
-- --     , a.avg_price_diff
-- --     , display_rank
-- --     , case when avg_bid_current < 25 then 25 else avg_bid_current end as avg_bid_current
-- --     , a.impressions
-- --     , impression_perc_rank
-- --     , case when impression_perc_rank >= 0.5 then 'popular hotel' else 'not popular hotel' end as is_popular 
-- --     , case 
-- --         when b.partner_property_id is not null and a.bid >= 60 then a.bid 
-- --         when b.partner_property_id is not null and a.bid < 60 then 60 
-- --         when b.partner_property_id is null then a.bid 
-- --       end as bid
-- --   from initial_bid_calc as a
-- --   left join (select * from prio_list where perc_occurence >= 0.5) as b 
-- --     on a.partner_property_id = b.partner_property_id 
-- --     and a.campaign_name = b.campaign_name
-- -- )

-- -- , final_bid as (
-- --   select 
-- --       a.partner_property_id
-- --     , a.campaign_name
-- --     , case 
-- --         -- when b.campaign_name is null and country_code in ('AE','OM','KW') then 45
-- --         when lower(campaign_type) <> 'domestic' and  b.campaign_name is null and campaign_country_code in ("AE","OM","KW",'BH','SA','QA','BH','JO','US','CA','UK') then 25
-- --         when lower(campaign_type) = 'domestic'  and  b.campaign_name is null and campaign_country_code = hotel_country_code then 25
-- --         when avg_price_diff <= 0 then 
-- --           case 
-- --             when impression_perc_rank >= 0.5 and display_rank >= 2.8 and avg_price_diff <=0 then avg_bid_current + 60
-- --             when display_rank >= 2.8  then avg_bid_current + 30
-- --             when display_rank >= 2.5 then avg_bid_current + 20
-- --             when display_rank >= 2 then avg_bid_current + 10
-- --             when display_rank > 1.5 then avg_bid_current + 5
-- --             when display_rank <= 1.5 then avg_bid_current - 5
-- --           end
-- --         else coalesce(bid,0) end as bid
-- --   from bid_list a
-- --   left join adjusted_bid b
-- --     on a.partner_property_id = b.partner_property_id
-- --     and a.campaign_name = b.campaign_name
-- --   left join exclude_hotel c
-- --     on a.partner_property_id = c.partner_property_id
-- --   where c.partner_property_id is null
-- -- )
{% endraw %}
