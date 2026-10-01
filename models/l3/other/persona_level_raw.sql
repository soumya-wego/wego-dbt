{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : persona_level_raw
-- Destination: analysis.persona_level_raw  (unchanged)
-- Schedule   : 3 of month 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.persona_level_raw
-- partition by created_at_date as
with clients as     
(

  select 
a.client_id,
max(nationality) as nationality,
max(gender) as gender,
max(age) as age
from (select * from `wego-cloud.wego_analytics.flights_bookings` where conversions_adjusted = 1 ) as a  
left join `wego-cloud.wego_analytics.flights_bookings_passengers`  as b on a.booking_id = b.booking_id 
group by 1


),







sessions as
(SELECT

session_id,date(created_at) as created_at_date,
created_at,device_type,user_country_code,user_city,site_code,locale,cf_bot_score,client_id





FROM `wego-cloud.wego_analytics.sessions` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) between TIMESTAMP("2019-01-01") and TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) and client_id in (select distinct(client_id) from  clients)),

sessions_demo as    (

select a.*,b.nationality, b.gender, b.age from sessions as a     
left join clients as b  
 on a.client_id = b.client_id



),



searches as
(
SELECT session_id,date(created_at) as created_at_date,search_id,
created_at as search_created_at,currency_code,user_logged_in,trip_type,trip_category,legs,cabin,adults_count,children_count,infants_count,
first_departure_airport_code,first_departure_country_code,first_arrival_airport_code,first_arrival_country_code,first_departure_date,last_departure_date,lead_time





 FROM `wego-cloud.wego_analytics.flights_searches` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2019-01-01")



),

clicks as
(
SELECT created_at as click_created_at,session_id,search_id,click_id,stops,airlines,total_price_usd,provider_code,provider_type,conversions_tracked FROM `wego-cloud.wego_analytics.flights_clicks` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2019-01-01")

),



flights_bookings as
(

select session_id,search_id,click_id,created_at as booking_created_at, booking_id from  `wego-cloud.wego_analytics.flights_bookings`  where conversions_adjusted = 1 group by 1,2,3,4,5



)



select a.*,
b.* except(session_id,created_at_date),
c.* except(search_id,session_id),
d.booking_created_at,
d.booking_id
from sessions_demo as a
left join searches as b
on a.session_id = b.session_id
and a.created_at_date = b.created_at_date
left join clicks as c
on b.search_id = c.search_id
left join   flights_bookings as d
on c.click_id = d.click_id
{% endraw %}
