{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : RTP_analysis_aggregated_daily_append
-- Destination: analysis.hotels_rtp_analysis_aggregated_daily  (unchanged)
-- Schedule   : every 24 hours   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels', 'sessions') }}
-- depends_on: {{ source('ib_hotels', 'wego_rates') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
{% raw %}
-- create table 
-- analysis.hotels_RTP_analysis_aggregated_daily
-- partition by created_at_date
-- as



with wego_rates as 

(select 
* except(rate_id,cancellations,rooms_count) ,
case when real_time_price_comparison_wait_time is not null then 1 else 0 end as is_attempt_made,
case when meta_rate_id is not null then 1 else 0 end as is_meta_rates_pulled,
case when price_info_by_markup_type.REALTIME.initial_markup_percentage >= 0 then 1 else 0 end as is_wego_price_leader,
case when price_info_by_markup_type.REALTIME.initial_markup_percentage >= COALESCE(price_info_by_markup_type.HISTORICAL.final_markup_percentage,price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage) then 1 else 0 end as is_historical_price_cheaper_than_1_perc,
case when markup_type = "REALTIME" then 1 else 0 end as is_realtime_markup_applied,
price_info_by_markup_type.REALTIME.initial_markup_percentage as realtime_initial_markup_percentage,
price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_final_markup_percentage,
price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
price_info_by_markup_type.historical.initial_markup_percentage as historical_initial_markup_percentage,
price_info_by_markup_type.historical.final_markup_percentage as historical_final_markup_percentage,
price_info_by_markup_type.historical.final_price as historical_final_price,
price_info_by_markup_type.SUPPLIER_CONFIG.initial_markup_percentage as SUPPLIER_CONFIG_initial_markup_percentage,
price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as SUPPLIER_CONFIG_final_markup_percentage,
price_info_by_markup_type.SUPPLIER_CONFIG.final_price as SUPPLIER_CONFIG_final_price,
TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) as created_at_date,
created_at as created_at_utc,
FROM `wego-cloud.ib_hotels.wego_rates*` where  flow_type IN ("CREATE_SEARCH","GET_ROOMS") and displayed_price_amount is null and final_price is not null
and _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
qualify row_number() over(partition by search_id,hotel_id order by final_price asc, COALESCE(historical_final_price, SUPPLIER_CONFIG_final_price) asc) = 1),


meta_sessions as 
(select 
  search_id,
  max(city_code) as city_code ,
  max(check_in_date) as check_in_date,
  max(check_out_date) as check_out_date,
  max(rooms_count) as rooms_count,
  max(adults_count) as adults_count,
  max(child_count) as child_count,
  max(currency) as currency,
  max(user_city) as user_city,
  max(user_country_code) as user_country_code,
  max(user_logged_in) as user_logged_in,
  max(device_type) as device_type,
  max(app_type) as app_type,
  max(site_code) as site_code
  FROM `wego-cloud.ib_hotels.sessions*` 
  WHERE  
  device = "Wego BoW Hotels integrations"
and _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 3 day))))
and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
group by 1),



filtered_wego_rates as 
(select a.*,
  b.city_code,
  b.check_in_date,
  b.check_out_date,
  b.rooms_count,
  b.adults_count,
  b.child_count,
  b.currency,
  b.user_city,
  b.user_country_code,
  b.user_logged_in,
  b.device_type,
  b.app_type,
  b.site_code
 from wego_rates as a 
inner  
join meta_Sessions as b on a.search_id = b.search_id
),


--remove _staging from the table name
filtered_wego_rates_clicks as
(select 
a.*,
b.clicks,
case when b.clicks > 0 then a.search_id end as clicked_search,
b.tracked_clicks,
b.tracked_bookings
 from filtered_wego_rates as a 
left join 
(SELECT 
search_id,hotel_id,
count(distinct(click_id)) as clicks,
count(distinct(case when conversions_tracked >= 0 then click_id end )) as tracked_clicks,
count(distinct(case when conversions_tracked > 0 then click_id end )) as tracked_bookings

FROM `wego-cloud.wego_analytics.hotels_clicks` WHERE DATE(_PARTITIONTIME) >= date_sub(current_date(), interval 3 day) and DATE(_PARTITIONTIME) <= date(current_date())
and lower(provider_code) like "%wego.com%"
group by 1,2) as b 
on a.search_id = b.search_id
and a.hotel_id =  cast(b.hotel_id as string))

select 
created_at_date ,
date(created_at_utc) as created_at_utc_date,
is_attempt_made,
is_meta_Rates_pulled,
is_wego_price_leader,
realtime_initial_markup_percentage,
historical_final_markup_percentage,
SUPPLIER_CONFIG_final_markup_percentage,
is_historical_price_cheaper_than_1_perc,
is_realtime_markup_applied,
markup_type,
city_code,
check_in_date,
check_out_date,
rooms_count,
adults_count,
child_count,
currency,
user_city,
user_country_code,
user_logged_in,
device_type,
app_type,
site_code,
count(distinct(concat(search_id,"-",hotel_id))) as hotel_searches,
sum(realtime_final_price - historical_final_price) as historical_RTP_Diff,
sum(case when tracked_bookings > 0 then realtime_final_price - historical_final_price end) as booked_historical_RTP_diff,
count(clicked_search) as clicked_hotel_ids,
sum(tracked_clicks) as tracked_clicks,
sum(tracked_bookings) as tracked_bookings
from filtered_wego_rates_clicks
where created_at_date  =  date_sub(current_date(), interval 1 day)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24;
{% endraw %}
