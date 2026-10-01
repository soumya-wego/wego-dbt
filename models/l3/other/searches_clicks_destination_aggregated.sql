{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : searches_clicks_destination_aggregated
-- Destination: analysis.searches_clicks_destination_aggregated  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.searches_clicks_destination_aggregated_testing 
-- partition by date as
-- Comment: BoW decouple project
-- Updated Date: 2023-10-25

-- -- Original:


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

-- flight_searches as
-- (
-- select
-- session_id,
-- date(created_at) as date,
-- 'flights' as product_vertical,
-- 'meta' as model,
-- site_code,
-- device_type,
-- locale,
-- first_departure_country_code AS origin_country_code,
-- first_arrival_country_code AS destination_country_code,
-- first_departure_city_code as origin_city_code,
-- first_arrival_city_code as destination_city_code,
-- trip_category,
-- trip_type,
-- cabin as cabin_class,
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
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
-- if(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)') is null, 'oneway',
-- if(date_diff(cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,
-- cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<5 and cast(format_date('%w', cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)<5 and cast(format_date( '%w', cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)>=date_diff(cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY), 'worktrip', 
-- if(date_diff(cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
-- if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
-- if(adults_count=1, 'solotrip',
-- if(adults_count=2, 'coupletrip',
-- if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
-- count(*) as searches
-- from wego_analytics.flights_searches
-- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
-- ),

-- flight_clicks as
-- (
-- select
-- session_id,
-- click_id,
-- date(created_at) as date,
-- 'flights' as product_vertical,
-- 'meta' as model,
-- site_code,
-- device_type,
-- locale,
-- departure_country_code AS origin_country_code,
-- arrival_country_code AS destination_country_code,
-- departure_city_code as origin_city_code,
-- arrival_city_code as destination_city_code,
-- trip_category,
-- trip_type,
-- cabin_class,
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
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
-- if(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)') is null, 'oneway',
-- if(date_diff(cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,
-- cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<5 and cast(format_date('%w', cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)<5 and cast(format_date( '%w', cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)>=date_diff(cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY), 'worktrip', 
-- if(date_diff(cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
-- if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
-- if(adults_count=1, 'solotrip',
-- if(adults_count=2, 'coupletrip',
-- if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
-- count(*) as clicks,
-- sum(if(tracking_status is not NULL, 1, 0)) as clicks_tracked,
-- sum(conversions_tracked) as conversions_tracked,
-- sum(conversions_adjusted) as conversions_adjusted,
-- SUM(IF(conversions_tracked > 0, total_price_usd, 0)) AS tracked_booking_gmv,
-- SUM(if(provider_code='wego.com',booking_revenue_in_usd,revenue_in_usd)) as revenue_in_usd,
-- SUM(if(provider_code='wego.com',booking_finance_revenue_usd,finance_revenue_usd)) as finance_revenue_usd,
-- SUM(if(provider_code='wego.com',booking_finance_revenue_usd,booking_revenue_usd)) as booking_revenue_usd,
-- SUM(total_price_usd) as total_price_usd,
-- sum(price_in_usd) as price_in_usd,
-- SUM(lead_time) as total_lead_time,
-- SUM(trip_duration) as total_trip_duration,
-- SUM(ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)) as total_passengers,
-- from wego_analytics.flights_clicks c

-- left join 
-- (
-- SELECT 
-- click_id as booking_click_id,
-- conversions_tracked as booking_conversions_tracked,
-- conversions_adjusted as booking_conversions_adjusted,
-- revenue_in_usd + ifnull(insurance_revenue,0) as booking_revenue_in_usd,
-- finance_revenue_usd + ifnull(insurance_revenue,0) as booking_finance_revenue_usd,
-- FROM
-- `wego-cloud.wego_analytics.flights_bookings` b
-- left join 
-- (
-- select booking_id,
-- finance_revenue_usd as insurance_revenue,
-- cost_of_sales_usd as insurance_cos,
-- gross_revenue_in_usd as gross_insurance
-- from
-- `wego-cloud.wego_analytics.flights_insurance`
-- WHERE DATE(created_at) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- ) i on b.booking_id = i.booking_id

-- WHERE DATE(created_at) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- and conversions_tracked > 0
-- ) b on c.click_id = b.booking_click_id
-- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
-- ),

-- flight_cu as
-- (
-- select
-- session_id,
-- bookable_id as click_id,
-- date(created_at) as date,
-- 'flights' as product_vertical,
-- 'cu' as model,
-- site_code,
-- device_type,
-- locale,
-- departure_country_code AS origin_country_code,
-- arrival_country_code AS destination_country_code,
-- departure_city_code as origin_city_code,
-- arrival_city_code as destination_city_code,
-- trip_category,
-- trip_type,
-- cabin_class,
-- case
-- when date_diff(first_departure_date,date(created_at),DAY) is null then '1 Day or Less' 
-- when date_diff(first_departure_date,date(created_at),DAY) <=1 then '1 Day or Less'
-- when date_diff(first_departure_date,date(created_at),DAY) between 2 and 7 then '2 to 7 Days'
-- when date_diff(first_departure_date,date(created_at),DAY) between 8 and 14 then '8 to 14 Days'
-- when date_diff(first_departure_date,date(created_at),DAY) between 15 and 21 then '15 to 21 Days'
-- when date_diff(first_departure_date,date(created_at),DAY) between 22 and 29 then '22 to 29 Days'
-- when date_diff(first_departure_date,date(created_at),DAY) between 30 and 59 then '30 to 59 Days'
-- when date_diff(first_departure_date,date(created_at),DAY)>=60 then '60+ Days' end as lead_time,
-- case
-- when date_diff(last_departure_date,first_departure_date,DAY) is null then '1-3 Days'  
-- when date_diff(last_departure_date,first_departure_date,DAY)<=3 then '1-3 Days'
-- when date_diff(last_departure_date,first_departure_date,DAY) between 4 and 7 then '4-7 Days'
-- when date_diff(last_departure_date,first_departure_date,DAY) between 8 and 11 then '8-11 Days'
-- when date_diff(last_departure_date,first_departure_date,DAY)>=12 then '12+ Days' end as trip_duration,
-- case
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
-- when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
-- if(last_departure_date is null, 'oneway',
-- if(date_diff(last_departure_date,first_departure_date, DAY)+1<5 and cast(format_date('%w',last_departure_date) as int64)<5 and cast(format_date( '%w', last_departure_date) as int64)>=date_diff(last_departure_date ,first_departure_date, DAY), 'worktrip', 
-- if(date_diff(last_departure_date ,first_departure_date, DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
-- if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
-- if(adults_count=1, 'solotrip',
-- if(adults_count=2, 'coupletrip',
-- if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
-- count(*) as clicks,
-- sum(0) as clicks_tracked,
-- sum(0) as conversions_tracked,
-- sum(0) as conversions_adjusted,
-- SUM(0) AS tracked_booking_gmv,
-- SUM(case when revenue_in_usd = 0 and provider_code = "booking.com" then 0.01 else revenue_in_usd end) as revenue_in_usd,
-- SUM(case when finance_revenue_usd = 0 and provider_code = "booking.com" then 0.01 else finance_revenue_usd end) as finance_revenue_usd,
-- SUM(0) as booking_revenue_usd,
-- SUM(0) as total_price_usd,
-- sum(0) as price_in_usd,
-- SUM(IFNULL(date_diff(last_departure_date,date(created_at),DAY),0)) as total_lead_time,
-- SUM(IFNULL(date_diff(last_departure_date,date(first_departure_date),DAY),0)) as total_trip_duration,
-- SUM(ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)) as total_passengers,
-- from wego_analytics.flights_bookables
-- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
-- ),

-- flight_clicks_cu as 
-- (
-- select * from flight_clicks
-- union all
-- select * from flight_cu
-- ),


-- flight_searches_sessions as 
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
-- origin_country_code,
-- destination_country_code,
-- origin_city_code,
-- destination_city_code,
-- trip_category,
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
-- sum(flight_searches.searches) as searches
-- from flight_searches left join ss on flight_searches.session_id=ss.session_id
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
-- ),

-- flight_clicks_cu_sessions as 
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
-- origin_country_code,
-- destination_country_code,
-- origin_city_code,
-- destination_city_code,
-- trip_category,
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
-- sum(flight_clicks_cu.clicks) as clicks,
-- sum(flight_clicks_cu.clicks_tracked) as clicks_tracked,
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
-- from flight_clicks_cu left join ss on flight_clicks_cu.session_id=ss.session_id
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
-- ),

-- flight_combined as
-- (
-- select
-- COALESCE(s.date,c.date) as date,
-- COALESCE(s.product_vertical,c.product_vertical) as product_vertical,
-- COALESCE(s.model,c.model) as model,
-- COALESCE(s.market,c.market) as market,
-- COALESCE(s.user_country_code,c.user_country_code) as user_country_code,
-- COALESCE(s.user_city,c.user_city) as user_city,
-- COALESCE(s.site_code,c.site_code) as site_code,
-- COALESCE(s.device_type,c.device_type) as device_type,
-- COALESCE(s.locale,c.locale) as locale,
-- COALESCE(s.origin_country_code,c.origin_country_code) as origin_country_code,
-- COALESCE(s.destination_country_code,c.destination_country_code) as destination_country_code,
-- COALESCE(s.origin_city_code,c.origin_city_code) as origin_city_code,
-- COALESCE(s.destination_city_code,c.destination_city_code) as destination_city_code,
-- COALESCE(s.trip_category,c.trip_category) as trip_category,
-- COALESCE(s.trip_type,c.trip_type) as trip_type,
-- COALESCE(s.cabin_class,c.cabin_class) as cabin_class,
-- COALESCE(s.Lead_Time,c.Lead_Time) as Lead_Time,
-- COALESCE(s.Trip_Duration,c.Trip_Duration) as Trip_Duration,
-- COALESCE(s.Passengers,c.Passengers) as Passengers,
-- COALESCE(s.trip_intent,c.trip_intent) as trip_intent,
-- COALESCE(s.trip_pax_type,c.trip_pax_type) as trip_pax_type,
-- COALESCE(s.channel,c.channel) as channel,
-- COALESCE(s.wg_source,c.wg_source) as wg_source,
-- COALESCE(s.wg_medium,c.wg_medium) as wg_medium,
-- COALESCE(s.wg_campaign,c.wg_campaign) as wg_campaign,
-- sum(searches) as searches,
-- sum(clicks) as clicks,
-- sum(clicks_tracked) as clicks_tracked,
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
-- sum(total_passengers) as total_passengers,
-- from flight_searches_sessions s FULL OUTER JOIN flight_clicks_cu_sessions c on s.date=c.date and s.product_vertical=c.product_vertical and s.model=c.model and s.site_code=c.site_code and s.device_type=c.device_type and s.locale=c.locale and s.origin_country_code=c.origin_country_code
-- and s.destination_country_code=c.destination_country_code and s.origin_city_code=c.origin_city_code and s.destination_city_code=c.destination_city_code and s.trip_category=c.trip_category and s.trip_type=c.trip_type and s.cabin_class=c.cabin_class and s.lead_time=c.lead_time and s.trip_duration=c.trip_duration and s.passengers=c.passengers and s.trip_intent=c.trip_intent and s.trip_pax_type=c.trip_pax_type and s.market=c.market and s.user_country_code=c.user_country_code and s.user_city=c.user_city and s.channel=c.channel and s.wg_source=c.wg_source and s.wg_medium=c.wg_medium and s.wg_campaign=c.wg_campaign
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
-- ),

-- --

-- hotel_searches as
-- (
-- select
-- session_id,
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
-- count(*) as searches
-- from wego_analytics.hotels_searches
-- WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
-- ),

-- hotel_clicks as
-- (
-- select
-- session_id,
-- click_id,
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
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
-- ),

-- hotel_cu as
-- (
-- select
-- session_id,
-- bookable_id as click_id,
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
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
-- ),




-- hotel_searches_sessions as 
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
-- sum(hotel_searches.searches) as searches
-- from hotel_searches left join ss on hotel_searches.session_id=ss.session_id
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
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
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
-- ),

-- hotel_combined as
-- (
-- select
-- COALESCE(s.date,c.date) as date,
-- COALESCE(s.product_vertical,c.product_vertical) as product_vertical,
-- COALESCE(s.model,c.model) as model,
-- COALESCE(s.market,c.market) as market,
-- COALESCE(s.user_country_code,c.user_country_code) as user_country_code,
-- COALESCE(s.user_city,c.user_city) as user_city,
-- COALESCE(s.site_code,c.site_code) as site_code,
-- COALESCE(s.device_type,c.device_type) as device_type,
-- COALESCE(s.locale,c.locale) as locale,
-- COALESCE(s.origin_country_code,c.origin_country_code) as origin_country_code,
-- COALESCE(s.destination_country_code,c.destination_country_code) as destination_country_code,
-- COALESCE(s.origin_city_code,c.origin_city_code) as origin_city_code,
-- COALESCE(s.destination_city_code,c.destination_city_code) as destination_city_code,
-- COALESCE(s.trip_category,c.trip_category) as trip_category,
-- COALESCE(s.trip_type,c.trip_type) as trip_type,
-- COALESCE(s.cabin_class,c.cabin_class) as cabin_class,
-- COALESCE(s.Lead_Time,c.Lead_Time) as Lead_Time,
-- COALESCE(s.Trip_Duration,c.Trip_Duration) as Trip_Duration,
-- COALESCE(s.Passengers,c.Passengers) as Passengers,
-- COALESCE(s.trip_intent,c.trip_intent) as trip_intent,
-- COALESCE(s.trip_pax_type,c.trip_pax_type) as trip_pax_type,
-- COALESCE(s.channel,c.channel) as channel,
-- COALESCE(s.wg_source,c.wg_source) as wg_source,
-- COALESCE(s.wg_medium,c.wg_medium) as wg_medium,
-- COALESCE(s.wg_campaign,c.wg_campaign) as wg_campaign,
-- sum(searches) as searches,
-- sum(clicks) as clicks,
-- sum(clicks_tracked) as clicks_tracked,
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
-- sum(total_passengers) as total_passengers,
-- from hotel_searches_sessions s FULL OUTER JOIN hotel_clicks_cu_sessions c on s.date=c.date and s.product_vertical=c.product_vertical and s.model=c.model and s.site_code=c.site_code and s.device_type=c.device_type and s.locale=c.locale and s.origin_country_code=c.origin_country_code
-- and s.destination_country_code=c.destination_country_code and s.origin_city_code=c.origin_city_code and s.destination_city_code=c.destination_city_code and s.trip_category=c.trip_category and s.trip_type=c.trip_type and s.cabin_class=c.cabin_class and s.lead_time=c.lead_time and s.trip_duration=c.trip_duration and s.passengers=c.passengers and s.trip_intent=c.trip_intent and s.trip_pax_type=c.trip_pax_type and s.market=c.market and s.user_country_code=c.user_country_code and s.user_city=c.user_city and s.channel=c.channel and s.wg_source=c.wg_source and s.wg_medium=c.wg_medium and s.wg_campaign=c.wg_campaign
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
-- ),

-- --

-- final_combined_draft as
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
-- from
-- (
-- (
-- select * from flight_combined 
-- union all
-- select * from hotel_combined
-- )) as draft

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
-- IFNULL(searches,0) as searches,
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
-- from final_combined_draft
-- )

-- select * from holygrail


-- updated version to remove duplicates and add additional fields for travel date (3rd Nov 2022)

WITH ss AS 
(
  SELECT distinct
    created_at,
    session_id,
    FIRST_VALUE(user_country_code) OVER(PARTITION BY session_id ORDER BY created_at) AS user_country_code,
    FIRST_VALUE(user_city) OVER(PARTITION BY session_id ORDER BY created_at) AS user_city,
    FIRST_VALUE(market) OVER(PARTITION BY session_id ORDER BY created_at) AS market,
    FIRST_VALUE(channel) OVER(PARTITION BY session_id ORDER BY created_at) AS channel,
    wg_source,
    wg_medium,
    wg_campaign
  FROM wego_analytics.sessions
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
flight_searches as(
  select
    session_id,
    date(created_at) as date,
    'flights' as product_vertical,
    'meta' as model,
    site_code,
    device_type,
    locale,
    first_departure_country_code AS origin_country_code,
    first_arrival_country_code AS destination_country_code,
    first_departure_city_code as origin_city_code,
    first_arrival_city_code as destination_city_code,
    trip_category,
    trip_type,
    cabin as cabin_class,
    case
    when lead_time is null then '1 Day or Less' 
    when lead_time <=1 then '1 Day or Less'
    when lead_time between 2 and 7 then '2 to 7 Days'
    when lead_time between 8 and 14 then '8 to 14 Days'
    when lead_time between 15 and 21 then '15 to 21 Days'
    when lead_time between 22 and 29 then '22 to 29 Days'
    when lead_time between 30 and 59 then '30 to 59 Days'
    when lead_time>=60 then '60+ Days' end as lead_time,
    case
    when trip_duration is null then '1-3 Days'  
    when trip_duration<=3 then '1-3 Days'
    when trip_duration between 4 and 7 then '4-7 Days'
    when trip_duration between 8 and 11 then '8-11 Days'
    when trip_duration>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
    if(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)') is null, 'oneway',
    if(date_diff(safe_cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,
    cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<5 and safe_cast(format_date('%w', cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)<5 and safe_cast(format_date( '%w', cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)>=date_diff(safe_cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,safe_cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY), 'worktrip', 
    if(date_diff(safe_cast(regexp_extract(search_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,safe_cast(regexp_extract(search_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
    if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
    if(adults_count=1, 'solotrip',
    if(adults_count=2, 'coupletrip',
    if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
    first_departure_date as travel_date,
    count(distinct search_id) as searches
  from wego_analytics.flights_searches
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

flight_clicks as (
  select
    session_id,
    click_id,
    date(created_at) as date,
    'flights' as product_vertical,
    'meta' as model,
    site_code,
    device_type,
    locale,
    departure_country_code AS origin_country_code,
    arrival_country_code AS destination_country_code,
    departure_city_code as origin_city_code,
    arrival_city_code as destination_city_code,
    trip_category,
    trip_type,
    cabin_class,
    case
    when lead_time is null then '1 Day or Less' 
    when lead_time <=1 then '1 Day or Less'
    when lead_time between 2 and 7 then '2 to 7 Days'
    when lead_time between 8 and 14 then '8 to 14 Days'
    when lead_time between 15 and 21 then '15 to 21 Days'
    when lead_time between 22 and 29 then '22 to 29 Days'
    when lead_time between 30 and 59 then '30 to 59 Days'
    when lead_time>=60 then '60+ Days' end as lead_time,
    case
    when trip_duration is null then '1-3 Days'  
    when trip_duration<=3 then '1-3 Days'
    when trip_duration between 4 and 7 then '4-7 Days'
    when trip_duration between 8 and 11 then '8-11 Days'
    when trip_duration>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
    if(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)') is null, 'oneway',
    if(date_diff(safe_cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,
    safe_cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<5 and safe_cast(format_date('%w', safe_cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)<5 and safe_cast(format_date( '%w', safe_cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date)) as int64)>=date_diff(safe_cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,safe_cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY), 'worktrip', 
    if(date_diff(safe_cast(regexp_extract(click_cities,r'(?:^[\w\:\-]*=[\w]*:[\w\-]*:)([\d\-]*)')as date) ,safe_cast(regexp_extract(click_cities,r'(?:^[\w]*:[\w\-]*:)([\d\-]*)')as date), DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
    if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
    if(adults_count=1, 'solotrip',
    if(adults_count=2, 'coupletrip',
    if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
    first_departure_date as travel_date,
    count(distinct click_id) as clicks,
    sum(if(tracking_status is not NULL, 1, 0)) as clicks_tracked,
    sum(conversions_tracked) as conversions_tracked,
    sum(conversions_adjusted) as conversions_adjusted,
    SUM(IF(conversions_tracked > 0, total_price_usd, 0)) AS tracked_booking_gmv,
    #### -- following code for summing revenue numbers is for old BoW/Meta model
    -- SUM(if(provider_code='wego.com',booking_revenue_in_usd,revenue_in_usd)) as revenue_in_usd,
    -- SUM(if(provider_code='wego.com',booking_finance_revenue_usd,finance_revenue_usd)) as finance_revenue_usd,
    -- SUM(if(provider_code='wego.com',booking_finance_revenue_usd,booking_revenue_usd)) as booking_revenue_usd,

    -- following code is for updated model where BoW is part of Meta partner for Wego.com
    SUM(revenue_in_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(booking_revenue_usd) as booking_revenue_usd,
    SUM(total_price_usd) as total_price_usd,
    sum(price_in_usd) as price_in_usd,
    SUM(lead_time) as total_lead_time,
    SUM(trip_duration) as total_trip_duration,
    SUM(ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)) as total_passengers,
    SUM(if(conversions_tracked > 0, ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0), 0)) as booked_passengers,
    SUM(0) as room_nights,
    SUM(0) as booked_room_nights
  from wego_analytics.flights_clicks c
  -- left join (
    -- SELECT 
      -- click_id as booking_click_id,
      -- conversions_tracked as booking_conversions_tracked,
      -- conversions_adjusted as booking_conversions_adjusted,
      -- revenue_in_usd + ifnull(insurance_revenue,0) as booking_revenue_in_usd,
      -- finance_revenue_usd + ifnull(insurance_revenue,0) as booking_finance_revenue_usd
    -- FROM `wego-cloud.wego_analytics.flights_bookings` b
    -- left join (
        -- select 
          -- booking_id,
          -- finance_revenue_usd as insurance_revenue,
          -- cost_of_sales_usd as insurance_cos,
          -- gross_revenue_in_usd as gross_insurance
        -- from `wego-cloud.wego_analytics.flights_insurance`
        -- WHERE DATE(created_at) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  -- ) i on b.booking_id = i.booking_id
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),
flight_cu as(
  select
    session_id,
    bookable_id as click_id,
    date(created_at) as date,
    'flights' as product_vertical,
    'cu' as model,
    site_code,
    device_type,
    locale,
    departure_country_code AS origin_country_code,
    arrival_country_code AS destination_country_code,
    departure_city_code as origin_city_code,
    arrival_city_code as destination_city_code,
    trip_category,
    trip_type,
    cabin_class,
    case
    when date_diff(first_departure_date,date(created_at),DAY) is null then '1 Day or Less' 
    when date_diff(first_departure_date,date(created_at),DAY) <=1 then '1 Day or Less'
    when date_diff(first_departure_date,date(created_at),DAY) between 2 and 7 then '2 to 7 Days'
    when date_diff(first_departure_date,date(created_at),DAY) between 8 and 14 then '8 to 14 Days'
    when date_diff(first_departure_date,date(created_at),DAY) between 15 and 21 then '15 to 21 Days'
    when date_diff(first_departure_date,date(created_at),DAY) between 22 and 29 then '22 to 29 Days'
    when date_diff(first_departure_date,date(created_at),DAY) between 30 and 59 then '30 to 59 Days'
    when date_diff(first_departure_date,date(created_at),DAY)>=60 then '60+ Days' end as lead_time,
    case
    when date_diff(last_departure_date,first_departure_date,DAY) is null then '1-3 Days'  
    when date_diff(last_departure_date,first_departure_date,DAY)<=3 then '1-3 Days'
    when date_diff(last_departure_date,first_departure_date,DAY) between 4 and 7 then '4-7 Days'
    when date_diff(last_departure_date,first_departure_date,DAY) between 8 and 11 then '8-11 Days'
    when date_diff(last_departure_date,first_departure_date,DAY)>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0) is null then '1'  
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)<=1 then '1'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)=2 then '2'
    when ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)>2 then '3+' end as passengers,
    if(last_departure_date is null, 'oneway',
    if(date_diff(last_departure_date,first_departure_date, DAY)+1<5 and safe_cast(format_date('%w',last_departure_date) as int64)<5 and safe_cast(format_date( '%w', last_departure_date) as int64)>=date_diff(last_departure_date ,first_departure_date, DAY), 'worktrip', 
    if(date_diff(last_departure_date ,first_departure_date, DAY)+1<8, 'leisuretrip', 'vacation'))) AS trip_intent,
    if((ifnull(children_count,0)+ifnull(infants_count,0))>0, 'familytrip',
    if(adults_count=1, 'solotrip',
    if(adults_count=2, 'coupletrip',
    if(adults_count>2, 'grouptrip', 'others')))) AS trip_pax_type,
    first_departure_date as travel_date,
    count(distinct bookable_id) as clicks,
    sum(0) as clicks_tracked,
    sum(0) as conversions_tracked,
    sum(0) as conversions_adjusted,
    SUM(0) AS tracked_booking_gmv,
    SUM(case when revenue_in_usd = 0 and provider_code = "booking.com" then 0.01 else revenue_in_usd end) as revenue_in_usd,
    SUM(case when finance_revenue_usd = 0 and provider_code = "booking.com" then 0.01 else finance_revenue_usd end) as finance_revenue_usd,
    SUM(0) as booking_revenue_usd,
    SUM(0) as total_price_usd,
    sum(0) as price_in_usd,
    SUM(IFNULL(date_diff(last_departure_date,date(created_at),DAY),0)) as total_lead_time,
    SUM(IFNULL(date_diff(last_departure_date,date(first_departure_date),DAY),0)) as total_trip_duration,
    SUM(ifnull(adults_count,0)+ifnull(children_count,0)+ifnull(infants_count,0)) as total_passengers,
    sum(0) as booked_passengers,
    SUM(0) as room_nights,
    SUM(0) as booked_room_nights
  from wego_analytics.flights_bookables
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),

flight_clicks_cu as (
  select * from flight_clicks
  union all
  select * from flight_cu
),

flight_searches_sessions as (
  select
    date,
    product_vertical,
    model,
    market,
    user_country_code,
    user_city,
    site_code,
    device_type,
    locale,
    origin_country_code,
    destination_country_code,
    origin_city_code,
    destination_city_code,
    trip_category,
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
    sum(flight_searches.searches) as searches
  from flight_searches left join ss on flight_searches.session_id = ss.session_id and flight_searches.date = date(ss.created_at)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

flight_clicks_cu_sessions as (
  select 
    date,
    product_vertical,
    model,
    market,
    user_country_code,
    user_city,
    site_code,
    device_type,
    locale,
    origin_country_code,
    destination_country_code,
    origin_city_code,
    destination_city_code,
    trip_category,
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
    sum(flight_clicks_cu.clicks) as clicks,
    sum(flight_clicks_cu.clicks_tracked) as clicks_tracked,
    sum(conversions_tracked) as conversions_tracked,
    sum(conversions_adjusted) as conversions_adjusted,
    SUM(tracked_booking_gmv) AS tracked_booking_gmv,
    SUM(revenue_in_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(booking_revenue_usd) as booking_revenue_usd,
    SUM(total_price_usd) as total_price_usd,
    sum(price_in_usd) as price_in_usd,
    SUM(total_lead_time) as total_lead_time,
    SUM(total_trip_duration) as total_trip_duration,
    sum(total_passengers) as total_passengers,
    sum(booked_passengers) as booked_passengers,
    SUM(0) as room_nights,
    SUM(0) as booked_room_nights
  from flight_clicks_cu left join ss on flight_clicks_cu.session_id = ss.session_id and flight_clicks_cu.date = date(ss.created_at)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

flight_combined as (
  select
  COALESCE(s.date,c.date) as date,
  COALESCE(s.product_vertical,c.product_vertical) as product_vertical,
  COALESCE(s.model,c.model) as model,
  COALESCE(s.market,c.market) as market,
  COALESCE(s.user_country_code,c.user_country_code) as user_country_code,
  COALESCE(s.user_city,c.user_city) as user_city,
  COALESCE(s.site_code,c.site_code) as site_code,
  COALESCE(s.device_type,c.device_type) as device_type,
  COALESCE(s.locale,c.locale) as locale,
  COALESCE(s.origin_country_code,c.origin_country_code) as origin_country_code,
  COALESCE(s.destination_country_code,c.destination_country_code) as destination_country_code,
  COALESCE(s.origin_city_code,c.origin_city_code) as origin_city_code,
  COALESCE(s.destination_city_code,c.destination_city_code) as destination_city_code,
  COALESCE(s.trip_category,c.trip_category) as trip_category,
  COALESCE(s.trip_type,c.trip_type) as trip_type,
  COALESCE(s.cabin_class,c.cabin_class) as cabin_class,
  COALESCE(s.Lead_Time,c.Lead_Time) as Lead_Time,
  COALESCE(s.Trip_Duration,c.Trip_Duration) as Trip_Duration,
  COALESCE(s.Passengers,c.Passengers) as Passengers,
  COALESCE(s.trip_intent,c.trip_intent) as trip_intent,
  COALESCE(s.trip_pax_type,c.trip_pax_type) as trip_pax_type,
  COALESCE(s.channel,c.channel) as channel,
  COALESCE(s.wg_source,c.wg_source) as wg_source,
  COALESCE(s.wg_medium,c.wg_medium) as wg_medium,
  COALESCE(s.wg_campaign,c.wg_campaign) as wg_campaign,
  COALESCE(s.travel_date,c.travel_date) as travel_date,
  sum(searches) as searches,
  sum(clicks) as clicks,
  sum(clicks_tracked) as clicks_tracked,
  sum(conversions_tracked) as conversions_tracked,
  sum(conversions_adjusted) as conversions_adjusted,
  SUM(tracked_booking_gmv) AS tracked_booking_gmv,
  SUM(revenue_in_usd) as revenue_in_usd,
  SUM(finance_revenue_usd) as finance_revenue_usd,
  SUM(booking_revenue_usd) as booking_revenue_usd,
  SUM(total_price_usd) as total_price_usd,
  sum(price_in_usd) as price_in_usd,
  SUM(total_lead_time) as total_lead_time,
  SUM(total_trip_duration) as total_trip_duration,
  sum(total_passengers) as total_passengers,
  sum(booked_passengers) as booked_passengers,
  SUM(0) as room_nights,
  SUM(0) as booked_room_nights
  from flight_searches_sessions s 
  
  FULL OUTER JOIN flight_clicks_cu_sessions c 
  on s.travel_date=c.travel_date and s.date=c.date 
    and s.product_vertical=c.product_vertical 
    and s.model=c.model 
    and s.site_code=c.site_code 
    and s.device_type=c.device_type 
    and s.locale=c.locale and s.origin_country_code=c.origin_country_code
    and s.destination_country_code=c.destination_country_code 
    and s.origin_city_code=c.origin_city_code 
    and s.destination_city_code=c.destination_city_code 
    and s.trip_category=c.trip_category 
    and s.trip_type=c.trip_type 
    and s.cabin_class=c.cabin_class 
    and s.lead_time=c.lead_time 
    and s.trip_duration=c.trip_duration 
    and s.passengers=c.passengers 
    and s.trip_intent=c.trip_intent 
    and s.trip_pax_type=c.trip_pax_type 
    and s.market=c.market 
    and s.user_country_code=c.user_country_code 
    and s.user_city=c.user_city 
    and s.channel=c.channel 
    and s.wg_source=c.wg_source 
    and s.wg_medium=c.wg_medium 
    and s.wg_campaign=c.wg_campaign
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

hotel_searches as (
  select
    session_id,
    date(created_at) as date,
    'hotels' as product_vertical,
    'meta' as model,
    site_code,
    device_type,
    locale,
    safe_cast(NULL AS STRING) AS origin_country_code,
    country_code AS destination_country_code,
    safe_cast(NULL AS STRING) as origin_city_code,
    city_code as destination_city_code,
    safe_cast(NULL AS STRING) as trip_category,
    safe_cast(NULL AS STRING) as trip_type,
    safe_cast(NULL AS STRING) as cabin_class,
    case
    when lead_time is null then '1 Day or Less' 
    when lead_time <=1 then '1 Day or Less'
    when lead_time between 2 and 7 then '2 to 7 Days'
    when lead_time between 8 and 14 then '8 to 14 Days'
    when lead_time between 15 and 21 then '15 to 21 Days'
    when lead_time between 22 and 29 then '22 to 29 Days'
    when lead_time between 30 and 59 then '30 to 59 Days'
    when lead_time>=60 then '60+ Days' end as lead_time,
    case
    when trip_duration is null then '1-3 Days'  
    when trip_duration<=3 then '1-3 Days'
    when trip_duration between 4 and 7 then '4-7 Days'
    when trip_duration between 8 and 11 then '8-11 Days'
    when trip_duration>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(guests_count,0) is null then '1'  
    when ifnull(guests_count,0)<=1 then '1'
    when ifnull(guests_count,0)=2 then '2'
    when ifnull(guests_count,0)>2 then '3+' end as passengers,
    if(date_diff(check_out, check_in, DAY)+1<5 and safe_cast(format_date('%w',check_out) as int64)<5 and safe_cast(format_date('%w',check_out) as int64)>=date_diff(check_out, check_in, DAY), 'worktrip', 
    if(date_diff(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
    if(guests_count=1, 'solotrip',if(rooms_count=1 and guests_count=2, 'coupletrip',if(rooms_count>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
    check_in as travel_date,
    count(distinct search_id) as searches
  from wego_analytics.hotels_searches
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

hotel_clicks as (

with bookings AS (
  SELECT
    click_id,
    COUNT(*)                   AS booking_rows,
    SUM(conversions_tracked)   AS conversions_tracked,
    SUM(conversions_adjusted)  AS conversions_adjusted,
   -- SUM(finance_revenue_usd)   AS finance_revenue_usd,
   -- SUM(revenue_in_usd)        AS revenue_in_usd,

    -- ungated totals, bookings-sourced
    SUM( wego_total_price_usd)                                      AS total_price_usd,
   -- SUM(IFNULL(guests_count, 0))                                   AS guests_count,
    SUM(IFNULL(trip_duration, 0) * IFNULL(total_rooms, 0))         AS room_nights,

    -- gated at row level: bookings measure + bookings flag
    SUM(IF(conversions_tracked > 0, wego_total_price_usd, 0))      AS tracked_gmv,
    SUM(IF(conversions_tracked > 0, IFNULL(guests_count, 0), 0))   AS tracked_pax,
    SUM(IF(conversions_tracked > 0,
           IFNULL(trip_duration, 0) * IFNULL(total_rooms, 0), 0))  AS tracked_room_nights,

        SUM(IF(conversions_adjusted > 0, wego_total_price_usd, 0))      AS adj_gmv,
    SUM(IF(conversions_adjusted > 0, IFNULL(guests_count, 0), 0))   AS adj_pax,
    SUM(IF(conversions_adjusted > 0,
           IFNULL(trip_duration, 0) * IFNULL(total_rooms, 0), 0))  AS adj_room_nights

  FROM wego_analytics.hotels_bookings
  WHERE click_id IS NOT NULL
    AND IFNULL(booking_status, '') NOT IN ('FAILED', 'PENDING')
  GROUP BY click_id
),



hotel_clicks as (

with meta as   
 (select
    session_id,
    a.click_id,
    date(created_at) as date,
    'hotels' as product_vertical,
    'meta' as model,
    site_code,
    device_type,
    locale,
    safe_cast(NULL AS STRING) AS origin_country_code,
    country_code AS destination_country_code,
    safe_cast(NULL AS STRING) as origin_city_code,
    city_code as destination_city_code,
    safe_cast(NULL AS STRING) as trip_category,
    safe_cast(NULL AS STRING) as trip_type,
    safe_cast(NULL AS STRING) as cabin_class,
    case
    when lead_time is null then '1 Day or Less' 
    when lead_time <=1 then '1 Day or Less'
    when lead_time between 2 and 7 then '2 to 7 Days'
    when lead_time between 8 and 14 then '8 to 14 Days'
    when lead_time between 15 and 21 then '15 to 21 Days'
    when lead_time between 22 and 29 then '22 to 29 Days'
    when lead_time between 30 and 59 then '30 to 59 Days'
    when lead_time>=60 then '60+ Days' end as lead_time,
    case
    when trip_duration is null then '1-3 Days'  
    when trip_duration<=3 then '1-3 Days'
    when trip_duration between 4 and 7 then '4-7 Days'
    when trip_duration between 8 and 11 then '8-11 Days'
    when trip_duration>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(guests_count,0) is null then '1'  
    when ifnull(guests_count,0)<=1 then '1'
    when ifnull(guests_count,0)=2 then '2'
    when ifnull(guests_count,0)>2 then '3+' end as passengers,
    if(date_diff(check_out, check_in, DAY)+1<5 and safe_cast(format_date('%w',check_out) as int64)<5 and safe_cast(format_date('%w',check_out) as int64)>=date_diff(check_out, check_in, DAY), 'worktrip', 
    if(date_diff(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
    if(guests_count=1, 'solotrip',if(rooms_count=1 and guests_count=2, 'coupletrip',if(rooms_count>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
    check_in as travel_date,
    count(distinct a.click_id) as clicks,
    sum(if(tracking_status is not NULL, 1, 0)) as clicks_tracked,
    sum(b.conversions_tracked) as conversions_tracked,
    sum(b.conversions_adjusted) as conversions_adjusted,
    SUM(adj_gmv) AS tracked_booking_gmv,
    #### -- following code for summing revenue numbers is for old BoW/Meta model
    -- SUM(if(provider_code='hotels.wego.com',booking_revenue_in_usd,revenue_in_usd)) as revenue_in_usd,
    -- SUM(if(provider_code='hotels.wego.com',booking_finance_revenue_usd,finance_revenue_usd)) as finance_revenue_usd,
    -- SUM(if(provider_code='hotels.wego.com',booking_finance_revenue_usd,booking_revenue_usd)) as booking_revenue_usd,

    -- following code is for updated model where BoW is part of Meta partner for Wego.com
    SUM(revenue_in_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(booking_revenue_usd) as booking_revenue_usd,

    SUM(a.total_price_usd) as total_price_usd,
    sum(price_in_usd) as price_in_usd,
    SUM(lead_time) as total_lead_time,
    SUM(case when b.conversions_adjusted >0 then a.trip_duration end) as total_trip_duration,
    SUM(IFNULL(guests_count,0)) as total_passengers,
    SUM(adj_pax) as booked_passengers,
    SUM(trip_duration * rooms_count) as room_nights,
    SUM(adj_room_nights) as booked_room_nights
  from wego_analytics.hotels_clicks a

  left join bookings as b on a.click_id = b.click_id
    
    WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21),

dest_city_lookup AS (
  SELECT city_name, code, country_code
  FROM (
    SELECT
      l.base_name  AS city_name,
      l.code,
      c.code       AS country_code,
      RANK() OVER(PARTITION BY l.code ORDER BY l.updated_at DESC)                  AS rank,
      RANK() OVER(PARTITION BY l.base_name, l.country_id ORDER BY l.hotel_count DESC) AS rank1
    FROM `place_services.locations` l
    LEFT JOIN (SELECT id, code FROM `wego-cloud.place_services.countries`) c
      ON l.country_id = c.id
    WHERE l.active IS TRUE
  )
  WHERE rank = 1 AND rank1 = 1
) 


 select * from meta     
 union all  
 select 
 session_id,
    click_id,
    date(created_at) as date,
    'hotels' as product_vertical,
    'Distribution' as model,
    site_code,
    device_type,
    locale,
    user_country_code AS origin_country_code,
    hotel_country_code AS destination_country_code,
    safe_cast(NULL AS STRING) as origin_city_code,
    b.code as destination_city_code,
    trip_category,
    safe_cast(NULL AS STRING) as trip_type,
    safe_cast(NULL AS STRING) as cabin_class,
    case
    when lead_time is null then '1 Day or Less'
    when lead_time <=1 then '1 Day or Less'
    when lead_time between 2 and 7 then '2 to 7 Days'
    when lead_time between 8 and 14 then '8 to 14 Days'
    when lead_time between 15 and 21 then '15 to 21 Days'
    when lead_time between 22 and 29 then '22 to 29 Days'
    when lead_time between 30 and 59 then '30 to 59 Days'
    when lead_time>=60 then '60+ Days' end as lead_time,
    case
    when trip_duration is null then '1-3 Days'
    when trip_duration<=3 then '1-3 Days'
    when trip_duration between 4 and 7 then '4-7 Days'
    when trip_duration between 8 and 11 then '8-11 Days'
    when trip_duration>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(guests_count,0)<=1 then '1'
    when ifnull(guests_count,0)=2 then '2'
    when ifnull(guests_count,0)>2 then '3+' end as passengers,
    if(date_diff(DATE(check_out), DATE(check_in), DAY)+1<5
       and safe_cast(format_date('%w', DATE(check_out)) as int64)<5
       and safe_cast(format_date('%w', DATE(check_out)) as int64)>=date_diff(DATE(check_out), DATE(check_in), DAY), 'worktrip',
    if(date_diff(DATE(check_out), DATE(check_in), DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
    if(guests_count=1, 'solotrip',if(total_rooms=1 and guests_count=2, 'coupletrip',if(total_rooms>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
    DATE(check_in) as travel_date,

    safe_cast(NULL AS INT64)  as clicks,
    safe_cast(NULL AS INT64)  as clicks_tracked,
    sum(conversions_tracked) as conversions_tracked,
    sum(conversions_adjusted) as conversions_adjusted,
    SUM(IF(conversions_adjusted > 0, wego_total_price_usd, 0)) AS tracked_booking_gmv,
   
    SUM(finance_revenue_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(finance_revenue_usd) as booking_revenue_usd,

    safe_cast(NULL AS FLOAT64)  as total_price_usd,
    safe_cast(NULL AS FLOAT64)  as price_in_usd,
    safe_cast(NULL AS INT64) as total_lead_time,
    sum(case when conversions_adjusted >0 then trip_duration end) as total_trip_duration,
    safe_cast(NULL AS INT64) as total_passengers,
    SUM(IF(conversions_adjusted > 0, IFNULL(guests_count, 0), 0)) as booked_passengers,
    safe_cast(NULL AS INT64) as room_nights,
    SUM(IF(conversions_adjusted > 0,
           IFNULL(trip_duration, 0) * IFNULL(total_rooms, 0), 0)) as booked_room_nights





 
 FROM wego_analytics.hotels_bookings as a 
 left join dest_city_lookup as b on a.hotel_city = b.city_name
 and a.hotel_country_code = b.country_code 
 where conversions_adjusted =  1 and (click_id is null or click_id not in  (select distinct(click_id) from meta))
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

)


select * from hotel_clicks 

 
),

hotel_cu as (
  select
    session_id,
    bookable_id as click_id,
    date(created_at) as date,
    'hotels' as product_vertical,
    'cu' as model,
    site_code,
    device_type,
    locale,
    safe_cast(NULL AS STRING) AS origin_country_code,
    country_code AS destination_country_code,
    safe_cast(NULL AS STRING) as origin_city_code,
    city_code as destination_city_code,
    safe_cast(NULL AS STRING) AS trip_category,
    safe_cast(NULL AS STRING) AS trip_type,
    safe_cast(NULL AS STRING) AS cabin_class,
    case
    when date_diff(check_in,date(created_at),DAY) is null then '1 Day or Less' 
    when date_diff(check_in,date(created_at),DAY) <=1 then '1 Day or Less'
    when date_diff(check_in,date(created_at),DAY) between 2 and 7 then '2 to 7 Days'
    when date_diff(check_in,date(created_at),DAY) between 8 and 14 then '8 to 14 Days'
    when date_diff(check_in,date(created_at),DAY) between 15 and 21 then '15 to 21 Days'
    when date_diff(check_in,date(created_at),DAY) between 22 and 29 then '22 to 29 Days'
    when date_diff(check_in,date(created_at),DAY) between 30 and 59 then '30 to 59 Days'
    when date_diff(check_in,date(created_at),DAY)>=60 then '60+ Days' end as lead_time,
    case
    when date_diff(check_out,check_in,DAY) is null then '1-3 Days'  
    when date_diff(check_out,check_in,DAY)<=3 then '1-3 Days'
    when date_diff(check_out,check_in,DAY) between 4 and 7 then '4-7 Days'
    when date_diff(check_out,check_in,DAY) between 8 and 11 then '8-11 Days'
    when date_diff(check_out,check_in,DAY)>=12 then '12+ Days' end as trip_duration,
    case
    when ifnull(guests_count,0) is null then '1'  
    when ifnull(guests_count,0)<=1 then '1'
    when ifnull(guests_count,0)=2 then '2'
    when ifnull(guests_count,0)>2 then '3+' end as passengers,
    if(date_diff(check_out, check_in, DAY)+1<5 and safe_cast(format_date('%w',check_out) as int64)<5 and safe_cast(format_date('%w',check_out) as int64)>=date_diff(check_out, check_in, DAY), 'worktrip', 
    if(date_diff(check_out, check_in, DAY)+1<8, 'leisuretrip', 'vacation')) AS trip_intent,
    if(guests_count=1, 'solotrip',if(rooms_count=1 and guests_count=2, 'coupletrip',if(rooms_count>1 and guests_count>2, 'grouptrip', 'others'))) AS trip_pax_type,
    check_in as travel_date,
    count(distinct bookable_id) as clicks,
    sum(0) as clicks_tracked,
    sum(0) as conversions_tracked,
    sum(0) as conversions_adjusted,
    SUM(0) AS tracked_booking_gmv,
    SUM(case when revenue_in_usd = 0 and provider_code = "booking.com" then 0.2 else revenue_in_usd end) as revenue_in_usd,
    SUM(case when finance_revenue_usd = 0 and provider_code = "booking.com" then 0.2 else finance_revenue_usd end) as finance_revenue_usd,
    SUM(0) as booking_revenue_usd,
    SUM(0) as total_price_usd,
    sum(0) as price_in_usd,
    SUM(IFNULL(date_diff(check_in,date(created_at),DAY),0)) as total_lead_time,
    SUM(IFNULL(date_diff(check_out,check_in,DAY),0)) as total_trip_duration,
    SUM(IFNULL(guests_count,0)) as total_passengers,
    SUM(0) as booked_passengers,
    SUM(date_diff(check_out, check_in, DAY) * rooms_count) as room_nights,
    SUM(0) as booked_room_nights
    from wego_analytics.hotels_bookables
  WHERE DATE(_PARTITIONTIME) BETWEEN '2018-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),

hotel_searches_sessions as (
  select
    date,
    product_vertical,
    model,
    market,
    user_country_code,
    user_city,
    site_code,
    device_type,
    locale,
    COALESCE(origin_country_code,user_country_code) as origin_country_code,
    destination_country_code,
    COALESCE(origin_city_code,user_city) as origin_city_code,
    destination_city_code,
    case when user_country_code=destination_country_code then 'domestic' else 'international' end as trip_category,
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
    sum(hotel_searches.searches) as searches
  from hotel_searches left join ss on hotel_searches.session_id=ss.session_id and hotel_searches.date=date(ss.created_at)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

hotel_clicks_cu as (
  select * from hotel_clicks
  union all
  select * from hotel_cu
),

hotel_clicks_cu_sessions as 
(
  select 
    date,
    product_vertical,
    model,
    market,
    user_country_code,
    user_city,
    site_code,
    device_type,
    locale,
    COALESCE(origin_country_code,user_country_code) as origin_country_code,
    destination_country_code,
    COALESCE(origin_city_code,user_city) as origin_city_code,
    destination_city_code,
    case when user_country_code=destination_country_code then 'domestic' else 'international' end as trip_category,
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
    sum(hotel_clicks_cu.clicks) as clicks,
    sum(hotel_clicks_cu.clicks_tracked) as clicks_tracked,
    sum(conversions_tracked) as conversions_tracked,
    sum(conversions_adjusted) as conversions_adjusted,
    SUM(tracked_booking_gmv) AS tracked_booking_gmv,
    SUM(revenue_in_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(booking_revenue_usd) as booking_revenue_usd,
    SUM(total_price_usd) as total_price_usd,
    sum(price_in_usd) as price_in_usd,
    SUM(total_lead_time) as total_lead_time,
    SUM(total_trip_duration) as total_trip_duration,
    sum(total_passengers) as total_passengers,
    sum(booked_passengers) as booked_passengers,
    SUM(room_nights) as room_nights,
    SUM(booked_room_nights) as booked_room_nights
  from hotel_clicks_cu left join ss on hotel_clicks_cu.session_id=ss.session_id and hotel_clicks_cu.date=date(ss.created_at)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

hotel_combined as (
  select
    COALESCE(s.date,c.date) as date,
    COALESCE(s.product_vertical,c.product_vertical) as product_vertical,
    COALESCE(s.model,c.model) as model,
    COALESCE(s.market,c.market) as market,
    COALESCE(s.user_country_code,c.user_country_code) as user_country_code,
    COALESCE(s.user_city,c.user_city) as user_city,
    COALESCE(s.site_code,c.site_code) as site_code,
    COALESCE(s.device_type,c.device_type) as device_type,
    COALESCE(s.locale,c.locale) as locale,
    COALESCE(s.origin_country_code,c.origin_country_code) as origin_country_code,
    COALESCE(s.destination_country_code,c.destination_country_code) as destination_country_code,
    COALESCE(s.origin_city_code,c.origin_city_code) as origin_city_code,
    COALESCE(s.destination_city_code,c.destination_city_code) as destination_city_code,
    COALESCE(s.trip_category,c.trip_category) as trip_category,
    COALESCE(s.trip_type,c.trip_type) as trip_type,
    COALESCE(s.cabin_class,c.cabin_class) as cabin_class,
    COALESCE(s.Lead_Time,c.Lead_Time) as Lead_Time,
    COALESCE(s.Trip_Duration,c.Trip_Duration) as Trip_Duration,
    COALESCE(s.Passengers,c.Passengers) as Passengers,
    COALESCE(s.trip_intent,c.trip_intent) as trip_intent,
    COALESCE(s.trip_pax_type,c.trip_pax_type) as trip_pax_type,
    COALESCE(s.channel,c.channel) as channel,
    COALESCE(s.wg_source,c.wg_source) as wg_source,
    COALESCE(s.wg_medium,c.wg_medium) as wg_medium,
    COALESCE(s.wg_campaign,c.wg_campaign) as wg_campaign,
    COALESCE(s.travel_date,c.travel_date) as travel_date,
    sum(searches) as searches,
    sum(clicks) as clicks,
    sum(clicks_tracked) as clicks_tracked,
    sum(conversions_tracked) as conversions_tracked,
    sum(conversions_adjusted) as conversions_adjusted,
    SUM(tracked_booking_gmv) AS tracked_booking_gmv,
    SUM(revenue_in_usd) as revenue_in_usd,
    SUM(finance_revenue_usd) as finance_revenue_usd,
    SUM(booking_revenue_usd) as booking_revenue_usd,
    SUM(total_price_usd) as total_price_usd,
    sum(price_in_usd) as price_in_usd,
    SUM(total_lead_time) as total_lead_time,
    SUM(total_trip_duration) as total_trip_duration,
    sum(total_passengers) as total_passengers,
    sum(booked_passengers) as booked_passengers,
    SUM(room_nights) as room_nights,
    SUM(booked_room_nights) as booked_room_nights
  from hotel_searches_sessions s 
  FULL OUTER JOIN hotel_clicks_cu_sessions c 
  on s.travel_date=c.travel_date 
  AND s.date=c.date
  AND s.product_vertical=c.product_vertical
  AND s.model=c.model
  AND s.site_code=c.site_code
  AND s.device_type=c.device_type
  AND s.locale=c.locale
  AND s.origin_country_code=c.origin_country_code
  AND s.destination_country_code=c.destination_country_code
  AND s.origin_city_code=c.origin_city_code
  AND s.destination_city_code=c.destination_city_code
  AND s.trip_category=c.trip_category
  AND s.trip_type=c.trip_type
  AND s.cabin_class=c.cabin_class
  AND s.lead_time=c.lead_time
  AND s.trip_duration=c.trip_duration
  AND s.passengers=c.passengers
  AND s.trip_intent=c.trip_intent
  AND s.trip_pax_type=c.trip_pax_type
  AND s.market=c.market
  AND s.user_country_code=c.user_country_code
  AND s.user_city=c.user_city
  AND s.channel=c.channel
  AND s.wg_source=c.wg_source
  AND s.wg_medium=c.wg_medium
  AND s.wg_campaign=c.wg_campaign
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
),

final_combined_draft as (
  select 
    * EXCEPT(origin_city_code),
    if(product_vertical='hotels',occ.code,origin_city_code) as origin_city_code,
    CONCAT(product_vertical,'_', model) AS product_vertical_type,
    CONCAT(product_vertical, '_', model, '_', trip_category) AS product_x_trip_category,
    CONCAT(product_vertical, '_', trip_category) AS vertical_x_trip_category,
    om_market as origin_market,
    dm_market as destination_market,
    uc.base_name as user_country,
    oc.base_name AS origin_country,
    dc.base_name AS destination_country,
    occ.city_name as origin_city,
    dcc.base_name as destination_city,
  from (
    select * from flight_combined 
    union all
    select * from hotel_combined
  ) as draft

  LEFT JOIN (SELECT country_code, market as om_market FROM `wego-cloud.analytics.countries_misc`) AS om ON draft.origin_country_code = om.country_code #origin_market
  LEFT JOIN (SELECT country_code, market as dm_market FROM `wego-cloud.analytics.countries_misc`) AS dm ON draft.destination_country_code = dm.country_code #destination_market
  LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS uc ON draft.user_country_code = uc.code #user_country
  LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS oc ON draft.origin_country_code = oc.code #origin_country
  LEFT JOIN (SELECT base_name, code FROM `wego-cloud.place_services.countries`) AS dc ON draft.destination_country_code = dc.code #destination_country

  #Destination City
  LEFT JOIN (
    select * from (
      SELECT base_name,code,RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank 
      FROM `place_services.locations`where active in (true)
    ) 
    where rank in (1)
  ) as dcc on draft.destination_city_code=dcc.code

  #origin city
  LEFT JOIN(
    select * from (
      SELECT 
        base_name as city_name,
        code,
        RANK() OVER(PARTITION BY code ORDER BY updated_at DESC) AS rank ,
        RANK() OVER(PARTITION BY base_name,country_id ORDER BY hotel_count DESC) AS rank1,country_id,id
      FROM `place_services.locations`where active in (true)
    ) as city
    left join (
      SELECT 
        base_name as country_name, 
        code as country_code ,
        id 
      FROM `wego-cloud.place_services.countries`
    ) AS country 
    on city.country_id=country.id
    where city.rank in (1) and city.rank1 in (1) --and city_name in ('Bohol')
  ) as occ 
  on (draft.origin_city_code=occ.code and draft.origin_country_code=occ.country_code) 
  or (draft.origin_city_code=occ.city_name and draft.origin_country_code=occ.country_code)
)
select
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
  trip_category,
  trip_type,
  cabin_class,
  trip_intent,
  trip_pax_type,
  product_vertical,
  model,
  lead_time,
  trip_duration,
  passengers,
  product_vertical_type,
  product_x_trip_category,
  vertical_x_trip_category,
  travel_date,
  IFNULL(searches,0) as searches,
  IFNULL(clicks,0) as clicks,
  clicks_tracked,
  conversions_tracked AS bookings_tracked,
  conversions_adjusted AS bookings_adjusted,
  0 AS tracked_booking_segments,
  tracked_booking_gmv,
  revenue_in_usd,
  booking_revenue_usd,
  finance_revenue_usd,
  price_in_usd,
  total_price_usd as total_price_in_usd,
  total_lead_time as total_lead_time,
  total_trip_duration as total_trip_duration,
  total_passengers as total_passengers,
  booked_passengers as booked_passengers,
  room_nights as room_nights,
  booked_room_nights as booked_room_nights
from final_combined_draft
{% endraw %}
