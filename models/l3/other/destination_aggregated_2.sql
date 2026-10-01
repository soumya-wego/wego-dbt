{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : destination_aggregated_2
-- Destination: analysis.destination_aggregated_2  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- Comment: BoW Decouple project changes done.
  -- Updated Date: 2023-10-25
  
  -- Original:
  -- WITH session AS
  -- (SELECT created_at,
  -- session_id,
  -- FIRST_VALUE(user_country_code) OVER(PARTITION BY session_id ORDER BY created_at) AS user_country_code,
  -- FIRST_VALUE(user_city) OVER(PARTITION BY session_id ORDER BY created_at) AS user_city,
  -- FIRST_VALUE(market) OVER(PARTITION BY session_id ORDER BY created_at) AS market,
  -- FIRST_VALUE(channel) OVER(PARTITION BY session_id ORDER BY created_at) AS channel,
  -- wg_source,
  -- wg_medium,
  -- wg_campaign,
  -- ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY created_at) AS rn
  -- FROM wego_analytics.sessions
  -- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- ),
  -- ss AS
  -- (SELECT * FROM session WHERE rn = 1),
  -- hotel_clicks as
  -- (
  -- select
  -- session_id,
  -- click_id,
  -- hotel_id,
  -- provider_code,
  -- date(created_at) as date,
  -- 'hotels' as product_vertical,
  -- 'meta' as model,
  -- site_code,
  -- device_type,
  -- locale,
  -- CAST(NULL AS STRING) AS origin_country_code,
  -- country_code AS destination_country_code,
  -- CAST(NULL AS STRING) as origin_city_code,
  -- city_code as destination_city_code,
  -- CAST(NULL AS STRING) as trip_category,
  -- CAST(NULL AS STRING) as trip_type,
  -- CAST(NULL AS STRING) as cabin_class,
  -- case
  -- when lead_time is null then '1 Day or Less'
  -- when lead_time <=1 then '1 Day or Less'
  -- when lead_time between 2 and 7 then '2 to 7 Days'
  -- when lead_time between 8 and 14 then '8 to 14 Days'
  -- when lead_time between 15 and 21 then '15 to 21 Days'
  -- when lead_time between 22 and 29 then '22 to 29 Days'
  -- when lead_time between 30 and 59 then '30 to 59 Days'
  -- when lead_time>=60 then '60+ Days' end as lead_time,
  -- case
  -- when trip_duration is null then '1-3 Days'
  -- when trip_duration<=3 then '1-3 Days'
  -- when trip_duration between 4 and 7 then '4-7 Days'
  -- when trip_duration between 8 and 11 then '8-11 Days'
  -- when trip_duration>=12 then '12+ Days' end as trip_duration,
  -- case
  -- when ifnull(guests_count,0) is null then '1'
  -- when ifnull(guests_count,0)<=1 then '1'
  -- when ifnull(guests_count,0)=2 then '2'
  -- when ifnull(guests_count,0)>2 then '3+' end as passengers,
  -- if(date_diff(check_out, check_in, DAY)+1<5 and cast(format_date('%w',check_out) as int64)<5 and cast(format_date('%w',check_out) as int64)>=date_diff(check_out, check_in, DAY), 'worktrip',
  -- if(date_diff(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
  -- if(guests_count=1, 'solotrip',if(rooms_count=1 and guests_count=2, 'coupletrip',if(rooms_count>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
  -- count(*) as clicks,
  -- sum(if(tracking_status is not NULL, 1, 0)) as clicks_tracked,
  -- sum(conversions_tracked) as conversions_tracked,
  -- sum(conversions_adjusted) as conversions_adjusted,
  -- SUM(IF(conversions_tracked > 0, total_price_usd, 0)) AS tracked_booking_gmv,
  -- SUM(if(provider_code='hotels.wego.com',booking_revenue_in_usd,revenue_in_usd)) as revenue_in_usd,
  -- SUM(if(provider_code='hotels.wego.com',booking_finance_revenue_usd,finance_revenue_usd)) as finance_revenue_usd,
  -- SUM(if(provider_code='hotels.wego.com',booking_finance_revenue_usd,booking_revenue_usd)) as booking_revenue_usd,
  -- SUM(total_price_usd) as total_price_usd,
  -- sum(price_in_usd) as price_in_usd,
  -- SUM(lead_time) as total_lead_time,
  -- SUM(trip_duration) as total_trip_duration,
  -- SUM(IFNULL(guests_count,0)) as total_passengers,
  -- from wego_analytics.hotels_clicks c
  -- left join
  -- (
  -- SELECT
  -- click_id as booking_click_id,
  -- revenue_in_usd as booking_revenue_in_usd,
  -- finance_revenue_usd as booking_finance_revenue_usd,
  -- conversions_tracked as booking_conversions_tracked,
  -- conversions_adjusted as booking_conversions_adjusted
  -- FROM wego_analytics.hotels_bookings
  -- WHERE DATE(created_at) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- and conversions_tracked > 0
  -- ) b on c.click_id = b.booking_click_id
  -- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  -- ),
  -- hotel_cu as
  -- (
  -- select
  -- session_id,
  -- bookable_id as click_id,
  -- NULL AS hotel_id,
  -- provider_code,
  -- date(created_at) as date,
  -- 'hotels' as product_vertical,
  -- 'cu' as model,
  -- site_code,
  -- device_type,
  -- locale,
  -- CAST(NULL AS STRING) AS origin_country_code,
  -- country_code AS destination_country_code,
  -- CAST(NULL AS STRING) as origin_city_code,
  -- city_code as destination_city_code,
  -- CAST(NULL AS STRING) AS trip_category,
  -- CAST(NULL AS STRING) AS trip_type,
  -- CAST(NULL AS STRING) AS cabin_class,
  -- case
  -- when date_diff(check_in,date(created_at),DAY) is null then '1 Day or Less'
  -- when date_diff(check_in,date(created_at),DAY) <=1 then '1 Day or Less'
  -- when date_diff(check_in,date(created_at),DAY) between 2 and 7 then '2 to 7 Days'
  -- when date_diff(check_in,date(created_at),DAY) between 8 and 14 then '8 to 14 Days'
  -- when date_diff(check_in,date(created_at),DAY) between 15 and 21 then '15 to 21 Days'
  -- when date_diff(check_in,date(created_at),DAY) between 22 and 29 then '22 to 29 Days'
  -- when date_diff(check_in,date(created_at),DAY) between 30 and 59 then '30 to 59 Days'
  -- when date_diff(check_in,date(created_at),DAY)>=60 then '60+ Days' end as lead_time,
  -- case
  -- when date_diff(check_out,check_in,DAY) is null then '1-3 Days'
  -- when date_diff(check_out,check_in,DAY)<=3 then '1-3 Days'
  -- when date_diff(check_out,check_in,DAY) between 4 and 7 then '4-7 Days'
  -- when date_diff(check_out,check_in,DAY) between 8 and 11 then '8-11 Days'
  -- when date_diff(check_out,check_in,DAY)>=12 then '12+ Days' end as trip_duration,
  -- case
  -- when ifnull(guests_count,0) is null then '1'
  -- when ifnull(guests_count,0)<=1 then '1'
  -- when ifnull(guests_count,0)=2 then '2'
  -- when ifnull(guests_count,0)>2 then '3+' end as passengers,
  -- if(date_diff(check_out, check_in, DAY)+1<5 and cast(format_date('%w',check_out) as int64)<5 and cast(format_date('%w',check_out) as int64)>=date_diff(check_out, check_in, DAY), 'worktrip',
  -- if(date_diff(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
  -- if(guests_count=1, 'solotrip',if(rooms_count=1 and guests_count=2, 'coupletrip',if(rooms_count>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
  -- count(*) as clicks,
  -- sum(0) as clicks_tracked,
  -- sum(0) as conversions_tracked,
  -- sum(0) as conversions_adjusted,
  -- SUM(0) AS tracked_booking_gmv,
  -- SUM(case when revenue_in_usd = 0 and provider_code = "booking.com" then 0.2 else revenue_in_usd end) as revenue_in_usd,
  -- SUM(case when finance_revenue_usd = 0 and provider_code = "booking.com" then 0.2 else finance_revenue_usd end) as finance_revenue_usd,
  -- SUM(0) as booking_revenue_usd,
  -- SUM(0) as total_price_usd,
  -- sum(0) as price_in_usd,
  -- SUM(IFNULL(date_diff(check_in,date(created_at),DAY),0)) as total_lead_time,
  -- SUM(IFNULL(date_diff(check_out,check_in,DAY),0)) as total_trip_duration,
  -- SUM(IFNULL(guests_count,0)) as total_passengers,
  -- from wego_analytics.hotels_bookables
  -- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
  -- ),
  -- hotel_clicks_cu as
  -- (
  -- select * from hotel_clicks
  -- union all
  -- select * from hotel_cu
  -- ),
  -- hotel_clicks_cu_sessions as
  -- (
  -- select
  -- date,
  -- product_vertical,
  -- model,
  -- market,
  -- user_country_code,
  -- user_city,
  -- site_code,
  -- device_type,
  -- locale,
  -- hotel_id,
  -- provider_code,
  -- COALESCE(origin_country_code,user_country_code) as origin_country_code,
  -- destination_country_code,
  -- COALESCE(origin_city_code,user_city) as origin_city_code,
  -- destination_city_code,
  -- case when user_country_code=destination_country_code then 'domestic' else 'international' end as trip_category,
  -- trip_type,
  -- cabin_class,
  -- lead_time,
  -- trip_duration,
  -- passengers,
  -- trip_intent,
  -- trip_pax_type,
  -- channel,
  -- wg_source,
  -- wg_medium,
  -- wg_campaign,
  -- sum(hotel_clicks_cu.clicks) as clicks,
  -- sum(hotel_clicks_cu.clicks_tracked) as clicks_tracked,
  -- sum(conversions_tracked) as conversions_tracked,
  -- sum(conversions_adjusted) as conversions_adjusted,
  -- SUM(tracked_booking_gmv) AS tracked_booking_gmv,
  -- SUM(revenue_in_usd) as revenue_in_usd,
  -- SUM(finance_revenue_usd) as finance_revenue_usd,
  -- SUM(booking_revenue_usd) as booking_revenue_usd,
  -- SUM(total_price_usd) as total_price_usd,
  -- sum(price_in_usd) as price_in_usd,
  -- SUM(total_lead_time) as total_lead_time,
  -- SUM(total_trip_duration) as total_trip_duration,
  -- sum(total_passengers) as total_passengers
  -- from hotel_clicks_cu left join ss on hotel_clicks_cu.session_id=ss.session_id
  -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
  -- ),
  -- --
  -- final_draft as
  -- (
  -- select
  -- * EXCEPT(origin_city_code),
  -- if(product_vertical='hotels',occ.code,origin_city_code) as origin_city_code,
  -- CONCAT(product_vertical,'_', model) AS product_vertical_type,
  -- CONCAT(product_vertical, '_', model, '_', trip_category) AS product_x_trip_category,
  -- CONCAT(product_vertical, '_', trip_category) AS vertical_x_trip_category,
  -- om_market as origin_market,
  -- dm_market as destination_market,
  -- uc.base_name as user_country,
  -- oc.base_name AS origin_country,
  -- dc.base_name AS destination_country,
  -- occ.city_name as origin_city,
  -- dcc.base_name as destination_city,
  -- if(model='cu','cu',cast(hotels.hotel_star as string)) as star,
  -- if(model='cu','cu',hotels.name) as hotel_name,
  -- if(model='cu','cu',property.property_type_e) as property_type,
  -- if(model='cu','cu',IFNULL(brands.brand_name,"Independent Hotels")) as hotel_brand,
  -- if(model='cu','cu',IFNULL(chains.chain_name,IFNULL(brands.brand_name,"Independent Hotels"))) as hotel_chain
  -- from
  -- (
  -- (
  -- select * from hotel_clicks_cu_sessions
  -- )
  -- ) as draft
  -- LEFT JOIN (select REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') as name,id,property_type_id,brand_id,star as hotel_star from `wego-cloud.hotel_services.hotels`) AS hotels on draft.hotel_id=hotels.id #hotel name and star
  -- LEFT JOIN (select REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') as property_type_e,id from `wego-cloud.hotel_services.property_types`) AS property on hotels.property_type_id=property.id # hotel property type
  -- LEFT JOIN (select REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') as brand_name,id,chain_id from `wego-cloud.hotel_services.brands`) AS brands on hotels.brand_id=brands.id # hotel brands
  -- LEFT JOIN (select REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') as chain_name,id from `wego-cloud.hotel_services.chains`) AS chains on brands.chain_id=chains.id # hotel chains
  -- LEFT JOIN (SELECT country_code, market as om_market FROM `wego-cloud.analytics.countries_misc`) AS om ON draft.origin_country_code = om.country_code #origin_market
  -- LEFT JOIN (SELECT country_code, market as dm_market FROM `wego-cloud.analytics.countries_misc`) AS dm ON draft.destination_country_code = dm.country_code #destination_market
  -- LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS uc ON draft.user_country_code = uc.code #user_country
  -- LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS oc ON draft.origin_country_code = oc.code #origin_country
  -- LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS dc ON draft.destination_country_code = dc.code #destination_country
  -- #Destination City
  -- LEFT JOIN
  -- (
  -- select * from
  -- (
  -- SELECT base_name,code,RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank
  -- FROM `place_services.locations`where active in (true)
  -- )
  -- where rank in (1)
  -- ) as dcc on draft.destination_city_code=dcc.code
  -- #origin city
  -- LEFT JOIN
  -- (
  -- select * from
  -- (
  -- SELECT base_name as city_name,code,RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank ,RANK() OVER(PARTITION BY base_name,country_id ORDER BY hotel_count DESC) AS rank1,country_id,id
  -- FROM `place_services.locations`where active in (true)
  -- ) as city
  -- left join (SELECT base_name as country_name, code as country_code ,id FROM `wego-cloud.place_services.countries`) AS country on city.country_id=country.id
  -- where city.rank in (1) and city.rank1 in (1) --and city_name in ('Bohol')
  -- ) as occ on (draft.origin_city_code=occ.code and draft.origin_country_code=occ.country_code) or (draft.origin_city_code=occ.city_name and draft.origin_country_code=occ.country_code)
  -- ),
  -- holygrail as
  -- (
  -- select
  -- date,
  -- user_country_code,
  -- user_country,
  -- market,
  -- site_code,
  -- device_type,
  -- channel,
  -- wg_source,
  -- wg_medium,
  -- wg_campaign,
  -- locale,
  -- provider_code,
  -- origin_market,
  -- destination_market,
  -- origin_country_code,
  -- destination_country_code,
  -- origin_country,
  -- destination_country,
  -- origin_city_code,
  -- destination_city_code,
  -- origin_city,
  -- destination_city,
  -- star,
  -- hotel_name,
  -- property_type,
  -- hotel_brand,
  -- hotel_chain,
  -- trip_category,
  -- trip_type,
  -- cabin_class,
  -- trip_intent,
  -- trip_pax_type,
  -- product_vertical,
  -- model,
  -- lead_time,
  -- trip_duration,
  -- passengers,
  -- product_vertical_type,
  -- product_x_trip_category,
  -- vertical_x_trip_category,
  -- IFNULL(clicks,0) as clicks,
  -- clicks_tracked,
  -- conversions_tracked AS bookings_tracked,
  -- conversions_adjusted AS bookings_adjusted,
  -- 0 AS tracked_booking_segments,
  -- tracked_booking_gmv,
  -- revenue_in_usd,
  -- booking_revenue_usd,
  -- finance_revenue_usd,
  -- price_in_usd,
  -- total_price_usd as total_price_in_usd,
  -- total_lead_time as total_lead_time,
  -- total_trip_duration as total_trip_duration,
  -- total_passengers as total_passengers
  -- from final_draft
  -- )
  -- select * from holygrail
  -- updated version to remove duplicates and add travel date (3rd Nov 2022):
WITH ss AS (
  SELECT
    created_at,
    session_id,
    user_country_code,
    user_city,
    market,
    channel,
    wg_source,
    wg_medium,
    wg_campaign,
  FROM wego_analytics.sessions
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01'
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) 
),
hotel_clicks AS (
  SELECT
    session_id,
    click_id,
    hotel_id,
    provider_code,
    DATE(created_at) AS date,
    'hotels' AS product_vertical,
    'meta' AS MODEL,
    site_code,
    device_type,
    locale,
    CAST(NULL AS STRING) AS origin_country_code,
    country_code AS destination_country_code,
    CAST(NULL AS STRING) AS origin_city_code,
    city_code AS destination_city_code,
    CAST(NULL AS STRING) AS trip_category,
    CAST(NULL AS STRING) AS trip_type,
    CAST(NULL AS STRING) AS cabin_class,
    CASE 
      WHEN lead_time IS NULL THEN '1 Day or Less'
      WHEN lead_time <=1 THEN '1 Day or Less'
      WHEN lead_time BETWEEN 2 AND 7 THEN '2 to 7 Days'
      WHEN lead_time BETWEEN 8 AND 14 THEN '8 to 14 Days'
      WHEN lead_time BETWEEN 15 AND 21 THEN '15 to 21 Days'
      WHEN lead_time BETWEEN 22 AND 29 THEN '22 to 29 Days'
      WHEN lead_time BETWEEN 30 AND 59 THEN '30 to 59 Days'
      WHEN lead_time>=60 THEN '60+ Days'
    END AS lead_time,
    CASE
      WHEN trip_duration IS NULL THEN '1-3 Days'
      WHEN trip_duration<=3 THEN '1-3 Days'
      WHEN trip_duration BETWEEN 4 AND 7 THEN '4-7 Days'
      WHEN trip_duration BETWEEN 8 AND 11 THEN '8-11 Days'
      WHEN trip_duration>=12 THEN '12+ Days'
    END AS trip_duration,
    CASE
      WHEN IFNULL(guests_count,0) IS NULL THEN '1'
      WHEN IFNULL(guests_count,0)<=1 THEN '1'
      WHEN IFNULL(guests_count,0)=2 THEN '2'
      WHEN IFNULL(guests_count,0)>2 THEN '3+'
    END AS passengers,
    IF(
        DATE_DIFF(check_out, check_in, DAY)+1<5
        AND CAST(FORMAT_DATE('%w',check_out) AS int64)<5
        AND CAST(FORMAT_DATE('%w',check_out) AS int64)>=DATE_DIFF(check_out, check_in, DAY), 'worktrip',
        IF(DATE_DIFF(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')
    ) AS trip_intent,
    IF(
        guests_count=1, 'solotrip', 
        IF(rooms_count=1 AND guests_count=2, 'coupletrip',
          IF(rooms_count>1 AND guests_count>2, 'grouptrip', 'others')
        )
    ) AS trip_pax_type,
    check_in AS travel_date,
    COUNT(DISTINCT click_id) AS clicks,
    SUM(IF(tracking_status IS NOT NULL, 1, 0)) AS clicks_tracked,
    SUM(conversions_tracked) AS conversions_tracked,
    SUM(conversions_adjusted) AS conversions_adjusted,
    SUM(IF(conversions_tracked > 0, total_price_usd, 0)) AS tracked_booking_gmv,
    -- #######  Attention -- BoW decoupling in action -- removed BoW booking exceptions
    -- SUM(IF(provider_code='hotels.wego.com',booking_revenue_in_usd,revenue_in_usd)) AS revenue_in_usd,
    -- SUM(IF(provider_code='hotels.wego.com',booking_finance_revenue_usd,finance_revenue_usd)) AS finance_revenue_usd,
    -- SUM(IF(provider_code='hotels.wego.com',booking_finance_revenue_usd,booking_revenue_usd)) AS booking_revenue_usd,
    
    -- #######  change in script as follows (without BoW exceptions)
    SUM(revenue_in_usd) AS revenue_in_usd,
    SUM(finance_revenue_usd) AS finance_revenue_usd,
    SUM(booking_revenue_usd) AS booking_revenue_usd,
    

    SUM(total_price_usd) AS total_price_usd,
    SUM(price_in_usd) AS price_in_usd,
    SUM(lead_time) AS total_lead_time,
    SUM(trip_duration) AS total_trip_duration,
    SUM(IFNULL(guests_count,0)) AS total_passengers,
    SUM(IF(conversions_tracked > 0, IFNULL(guests_count,0), 0)) AS booked_passengers,
    SUM(trip_duration * rooms_count) AS room_nights,
    SUM(IF(conversions_tracked > 0, trip_duration * rooms_count, 0)) AS booked_room_nights
  FROM wego_analytics.hotels_clicks c
  #### following join contains BoW specific revenue numbers 
  #### And as part of BoW decoupling project this exception needs to be removed.
  #### The purpose of this comment is to increase the readibility and enhance overall
  #### transparency of the project.

  -- LEFT JOIN(
  --   SELECT
  --     click_id AS booking_click_id,
  --     revenue_in_usd AS booking_revenue_in_usd,
  --     finance_revenue_usd AS booking_finance_revenue_usd,
  --     conversions_tracked AS booking_conversions_tracked,
  --     conversions_adjusted AS booking_conversions_adjusted
  --   FROM wego_analytics.hotels_bookings
  --   WHERE DATE(created_at) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  --     AND conversions_tracked > 0 
  -- ) b ON c.click_id = b.booking_click_id
  
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
),
hotel_cu AS (
  SELECT
    session_id,
    bookable_id AS click_id,
    NULL AS hotel_id,
    provider_code,
    DATE(created_at) AS date,
    'hotels' AS product_vertical,
    'cu' AS MODEL,
    site_code,
    device_type,
    locale,
    CAST(NULL AS STRING) AS origin_country_code,
    country_code AS destination_country_code,
    CAST(NULL AS STRING) AS origin_city_code,
    city_code AS destination_city_code,
    CAST(NULL AS STRING) AS trip_category,
    CAST(NULL AS STRING) AS trip_type,
    CAST(NULL AS STRING) AS cabin_class,
    CASE
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) IS NULL THEN '1 Day or Less'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) <=1 THEN '1 Day or Less'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) BETWEEN 2 AND 7 THEN '2 to 7 Days'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) BETWEEN 8 AND 14 THEN '8 to 14 Days'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) BETWEEN 15 AND 21 THEN '15 to 21 Days'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) BETWEEN 22 AND 29 THEN '22 to 29 Days'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY) BETWEEN 30 AND 59 THEN '30 to 59 Days'
      WHEN DATE_DIFF(check_in,DATE(created_at),DAY)>=60 THEN '60+ Days'
    END AS lead_time,
    CASE
      WHEN DATE_DIFF(check_out,check_in,DAY) IS NULL THEN '1-3 Days'
      WHEN DATE_DIFF(check_out,check_in,DAY)<=3 THEN '1-3 Days'
      WHEN DATE_DIFF(check_out,check_in,DAY) BETWEEN 4 AND 7 THEN '4-7 Days'
      WHEN DATE_DIFF(check_out,check_in,DAY) BETWEEN 8 AND 11 THEN '8-11 Days'
      WHEN DATE_DIFF(check_out,check_in,DAY)>=12 THEN '12+ Days'
    END AS trip_duration,
    CASE
      WHEN IFNULL(guests_count,0) IS NULL THEN '1'
      WHEN IFNULL(guests_count,0)<=1 THEN '1'
      WHEN IFNULL(guests_count,0)=2 THEN '2'
      WHEN IFNULL(guests_count,0)>2 THEN '3+'
    END AS passengers,
    IF(DATE_DIFF(check_out, check_in, DAY)+1<5
        AND CAST(FORMAT_DATE('%w',check_out) AS int64)<5
        AND CAST(FORMAT_DATE('%w',check_out) AS int64)>=DATE_DIFF(check_out, check_in, DAY), 'worktrip',
      IF(DATE_DIFF(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')
    ) AS trip_intent,
    IF(guests_count=1, 'solotrip',
      IF(rooms_count=1 AND guests_count=2, 'coupletrip',
        IF(rooms_count>1 AND guests_count>2, 'grouptrip', 'others')
        )
    ) AS trip_pax_type,
    check_in AS travel_date,
    COUNT(DISTINCT bookable_id) AS clicks,
    SUM(0) AS clicks_tracked,
    SUM(0) AS conversions_tracked,
    SUM(0) AS conversions_adjusted,
    SUM(0) AS tracked_booking_gmv,
    SUM(CASE WHEN revenue_in_usd = 0 AND provider_code = "booking.com" THEN 0.2 ELSE revenue_in_usd END) AS revenue_in_usd,
    SUM(CASE WHEN finance_revenue_usd = 0 AND provider_code = "booking.com" THEN 0.2 ELSE finance_revenue_usd END) AS finance_revenue_usd,
    SUM(0) AS booking_revenue_usd,
    SUM(0) AS total_price_usd,
    SUM(0) AS price_in_usd,
    SUM(IFNULL(DATE_DIFF(check_in,DATE(created_at),DAY),0)) AS total_lead_time,
    SUM(IFNULL(DATE_DIFF(check_out,check_in,DAY),0)) AS total_trip_duration,
    SUM(IFNULL(guests_count,0)) AS total_passengers,
    SUM(0) AS booked_passengers,
    SUM(DATE_DIFF(check_out, check_in, DAY) * rooms_count) AS room_nights,
    SUM(0) AS booked_room_nights
  FROM wego_analytics.hotels_bookables
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
),
hotel_clicks_cu AS (
  SELECT * FROM hotel_clicks
  UNION ALL
  SELECT * FROM hotel_cu 
),
  hotel_clicks_cu_sessions AS (
  SELECT
    date,
    product_vertical,
    MODEL,
    market,
    user_country_code,
    user_city,
    site_code,
    device_type,
    locale,
    hotel_id,
    provider_code,
    COALESCE(origin_country_code,user_country_code) AS origin_country_code,
    destination_country_code,
    COALESCE(origin_city_code,user_city) AS origin_city_code,
    destination_city_code,
    CASE WHEN user_country_code=destination_country_code THEN 'domestic' ELSE 'international' END AS trip_category,
    trip_type,
    cabin_class,
    lead_time,
    trip_duration,
    passengers,
    trip_intent,
    trip_pax_type,
    channel,
    wg_source,
    wg_medium,
    wg_campaign,
    travel_date,
    SUM(hotel_clicks_cu.clicks) AS clicks,
    SUM(hotel_clicks_cu.clicks_tracked) AS clicks_tracked,
    SUM(conversions_tracked) AS conversions_tracked,
    SUM(conversions_adjusted) AS conversions_adjusted,
    SUM(tracked_booking_gmv) AS tracked_booking_gmv,
    SUM(revenue_in_usd) AS revenue_in_usd,
    SUM(finance_revenue_usd) AS finance_revenue_usd,
    SUM(booking_revenue_usd) AS booking_revenue_usd,
    SUM(total_price_usd) AS total_price_usd,
    SUM(price_in_usd) AS price_in_usd,
    SUM(total_lead_time) AS total_lead_time,
    SUM(total_trip_duration) AS total_trip_duration,
    SUM(total_passengers) AS total_passengers,
    SUM(booked_passengers) AS booked_passengers,
    SUM(room_nights) AS room_nights,
    SUM(booked_room_nights) AS booked_room_nights
  FROM hotel_clicks_cu
  LEFT JOIN ss
  ON hotel_clicks_cu.session_id=ss.session_id
    AND hotel_clicks_cu.date=DATE(ss.created_at)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28
),
final_draft AS (
  SELECT 
    * EXCEPT(origin_city_code),
    IF(product_vertical='hotels',occ.code,origin_city_code) AS origin_city_code,
    CONCAT(product_vertical,'_', MODEL) AS product_vertical_type,
    CONCAT(product_vertical, '_', MODEL, '_', trip_category) AS product_x_trip_category,
    CONCAT(product_vertical, '_', trip_category) AS vertical_x_trip_category,
    om_market AS origin_market,
    dm_market AS destination_market,
    uc.base_name AS user_country,
    oc.base_name AS origin_country,
    dc.base_name AS destination_country,
    occ.city_name AS origin_city,
    dcc.base_name AS destination_city,
    IF(MODEL='cu','cu',CAST(hotels.hotel_star AS string)) AS star,
    IF(MODEL='cu','cu',hotels.name) AS hotel_name,
    IF(MODEL='cu','cu',property.property_type_e) AS property_type,
    IF(MODEL='cu','cu',IFNULL(brands.brand_name,"Independent Hotels")) AS hotel_brand,
    IF(MODEL='cu','cu',IFNULL(chains.chain_name,IFNULL(brands.brand_name,"Independent Hotels"))) AS hotel_chain
  FROM hotel_clicks_cu_sessions AS draft
  LEFT JOIN (
    SELECT
      REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') AS name,
      id,
      property_type_id,
      brand_id,
      star AS hotel_star
    FROM `wego-cloud.hotel_services.hotels`
  ) AS hotels
  ON draft.hotel_id=hotels.id #hotel name and star
  LEFT JOIN (
    SELECT
      REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') AS property_type_e,
      id
    FROM `wego-cloud.hotel_services.property_types`
  ) AS property
  ON hotels.property_type_id=property.id # hotel property type
  LEFT JOIN (
    SELECT
      REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') AS brand_name,
      id,
      chain_id
    FROM `wego-cloud.hotel_services.brands`
  ) AS brands
  ON hotels.brand_id=brands.id # hotel brands
  LEFT JOIN (
    SELECT
      REPLACE(JSON_EXTRACT(name, '$.en'), '"', '') AS chain_name,
      id
    FROM `wego-cloud.hotel_services.chains`
  ) AS chains
  ON brands.chain_id=chains.id # hotel chains
  LEFT JOIN (
    SELECT
      country_code,
      market AS om_market
    FROM `wego-cloud.analytics.countries_misc`
  ) AS om
  ON draft.origin_country_code = om.country_code #origin_market
  LEFT JOIN (
    SELECT
      country_code,
      market AS dm_market
    FROM `wego-cloud.analytics.countries_misc`
  ) AS dm
  ON draft.destination_country_code = dm.country_code #destination_market
  LEFT JOIN (
    SELECT
      base_name,
      code
    FROM `wego-cloud.place_services.countries`
  ) AS uc
  ON draft.user_country_code = uc.code #user_country
  LEFT JOIN (
    SELECT
      base_name,
      code
    FROM `wego-cloud.place_services.countries`
  ) AS oc
  ON draft.origin_country_code = oc.code #origin_country
  LEFT JOIN (
    SELECT
      base_name,
      code
    FROM `wego-cloud.place_services.countries`
  ) AS dc
  ON draft.destination_country_code = dc.code #destination_country
  #Destination City
  LEFT JOIN (
    SELECT *
    FROM (
      SELECT
        base_name,
        code,
        RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank
      FROM `place_services.locations`
      WHERE active IN (TRUE) 
    )
    WHERE rank = 1 
  ) AS dcc
  ON draft.destination_city_code=dcc.code
  #origin city
  LEFT JOIN (
    SELECT *
    FROM (
      SELECT
        base_name AS city_name,
        code,
        RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank,
        RANK() OVER(PARTITION BY base_name, country_id ORDER BY hotel_count DESC) AS rank1,
        country_id,
        id
      FROM `place_services.locations`
      WHERE active IN (TRUE) 
    ) AS city
    LEFT JOIN (
      SELECT
        base_name AS country_name,
        code AS country_code,
        id
      FROM `wego-cloud.place_services.countries`
    ) AS country
    ON city.country_id=country.id
    WHERE city.rank = 1
      AND city.rank1 = 1  --and city_name in ('Bohol')
  ) AS occ
  ON (draft.origin_city_code=occ.code AND draft.origin_country_code=occ.country_code)
    OR (draft.origin_city_code=occ.city_name AND draft.origin_country_code=occ.country_code) 
)
SELECT
  date,
  user_country_code,
  user_country,
  market,
  site_code,
  device_type,
  channel,
  wg_source,
  wg_medium,
  wg_campaign,
  locale,
  provider_code,
  origin_market,
  destination_market,
  origin_country_code,
  destination_country_code,
  origin_country,
  destination_country,
  origin_city_code,
  destination_city_code,
  origin_city,
  destination_city,
  star,
  hotel_name,
  property_type,
  hotel_brand,
  hotel_chain,
  trip_category,
  trip_type,
  cabin_class,
  trip_intent,
  trip_pax_type,
  product_vertical,
  MODEL,
  lead_time,
  trip_duration,
  passengers,
  product_vertical_type,
  product_x_trip_category,
  vertical_x_trip_category,
  travel_date,
  IFNULL(clicks,0) AS clicks,
  clicks_tracked,
  conversions_tracked AS bookings_tracked,
  conversions_adjusted AS bookings_adjusted,
  0 AS tracked_booking_segments,
  tracked_booking_gmv,
  revenue_in_usd,
  booking_revenue_usd,
  finance_revenue_usd,
  price_in_usd,
  total_price_usd AS total_price_in_usd,
  total_lead_time AS total_lead_time,
  total_trip_duration AS total_trip_duration,
  total_passengers AS total_passengers,
  booked_passengers AS booked_passengers,
  room_nights AS room_nights,
  booked_room_nights AS booked_room_nights
FROM final_draft
{% endraw %}
