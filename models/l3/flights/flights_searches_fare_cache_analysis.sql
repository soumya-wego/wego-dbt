{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : flights_searches_fare_cache_analysis
-- Destination: analysis.flights_searches_fare_cache_analysis  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('analytics', 'exchange_rates') }}
-- depends_on: {{ source('integrated_bookings_flights', 'branded_fares') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'fare_calculations20250220') }}
-- depends_on: {{ source('services_curiosity', 'provider_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'provider_fares') }}
-- depends_on: {{ source('wego_analytics', 'flights_searches') }}
{% raw %}
-- create table 
-- analysis.flights_searches_fare_cache_analysis
-- partition by created_at_date
-- as

with base as 
(SELECT id,
search_id,
flight_id,
provider_code,
pos_provider_code,
price.amount as amount,
price.currency_code as currency_code,
price.total_amount as total_amount,
price.amount_per_adult as amount_per_adult,
price.amount_per_child as amount_per_child,
price.amount_per_infant as amount_per_infant,
price.original_amount as original_amount,
price.original_amount_usd as original_amount_usd,
price.tax_amount as tax_amount,
price.tax_amount_usd as tax_amount_usd,
price.tax_inclusive as tax_inclusive,
price.base_amount as base_mount,
price.base_amount_usd as base_mount_usd,
price.booking_fee as booking_fee,
price.booking_fee_usd as booking_fee_usd,
price.total_booking_fee as total_booking_fee,
price.total_booking_fee_usd as total_booking_fee_usd,
TIMESTAMP_ADD(TIMESTAMP(created_at), INTERVAL 8 HOUR) as created_at,
date(TIMESTAMP_ADD(TIMESTAMP(created_at), INTERVAL 8 HOUR)) as created_at_date,
search_type,
extra_info.booking_ipcc ,
extra_info.fare_ipcc,
FROM 
  `wego-cloud.services_curiosity.provider_fares*`  where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore')))) 
  and search_type in ("OE_SEARCH","OE_REVALIDATE") and provider_code = "a.wego.com"
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27),




  base_with_usd as

(select a.* ,

round(a.total_amount*b.amount,2) as total_amount_usd,
round(a.amount*b.amount,2) as amount_usd
from base as a 
left join  (select * from `wego-cloud.analytics.exchange_rates*` where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore'))))  ) as b
on a.currency_code = b.base
and cast(a.created_at_date as date) = cast(b.effective as date) ),




  oe_search as 
  (select * from base_with_usd where search_type = "OE_SEARCH"),

--select count(*),count(distinct(concat(id,search_id))) from oe_Search;
--1933201
--1933201


  bulk_revalidation_selection as
  (select * from base_with_usd where search_type = "OE_REVALIDATE"),


--select count(*) from bulk_revalidation_selection;
--4636573


-- cte as
-- (select 
-- provider_fare_id,
-- search_id,
-- flight_id,
-- --fare_id,
-- fare_ipcc,
-- booking_ipcc,
-- case when lower(fare_id) like "%soo%" then "SOO" else "SS" end as type

--  FROM `wego-cloud.services_curiosity.fare_calculations20250220`  where provider_fare_id is not null group by 1,2,3,4,5,6
--  --,7
--  )

cte as
(select 
provider_fare_id,
search_id,
flight_id,
--fare_id,
fare_ipcc,
booking_ipcc

 FROM `wego-cloud.services_curiosity.provider_fare_calculations*` where selected = true and  
_table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore')))) 
 group by 1,2,3,4,5
 --,7
 ),


--   select a.id,a.search_id,a.flight_id
--  from bulk_revalidation_selection as a 
--  left join cte as b on a.search_id = b.search_id
--  and a.id = b.provider_Fare_id
--  and a.fare_ipcc = b.fare_ipcc
--  where b.provider_fare_id is null 
--  group by 1,2,3;


--  select count(*),count(distinct(concat(a.id,a.search_id))),count(distinct(case when b.provider_fare_id is not null then concat(a.id,a.search_id) end))
--  from bulk_revalidation_selection as a 
--  left join cte as b on a.search_id = b.search_id
--  and a.id = b.provider_Fare_id
--  and a.fare_ipcc = b.fare_ipcc;

--4636573
--1478202
--458116

--only 30% mapping 

--  select count(*),count(distinct(concat(a.id,a.search_id))),count(distinct(case when b.provider_fare_id is not null then concat(a.id,a.search_id) end))
--  from bulk_revalidation_selection as a 
--  left join cte as b on a.search_id = b.search_id
--  and a.id = b.provider_Fare_id
--  and a.fare_ipcc = b.fare_ipcc;
--  --4636573
--1478202
--1112502

--75% mapping in case of provider_fare_Calculation

--bulk revalidated selected ones
revalidation_selected as 
(
select a.* ,


case when c.provider_fare_id is null then "non mapped fares" else "mapped fares" end as fare_mapping,b.fare_ipcc as provider_fare_ipcc,b from bulk_revalidation_selection as a 
left join cte as b 
on a.id = b.provider_fare_id
and a.search_id = b.search_id 
and a.fare_ipcc = b.fare_ipcc
left join cte as c 
on a.id = c.provider_fare_id
and a.search_id = c.search_id 
),

-- select fare_mapping,count(*),count(distinct(concat(id,search_id))) from revalidation_Selected where provider_fare_ipcc = fare_ipcc or fare_mapping = "non mapped fares" group by 1;


--revalidation_Selected_clean

revalidation_Selected_clean as 
(
select * from revalidation_selected where fare_ipcc = provider_Fare_ipcc and fare_mapping = "mapped fares" 
union all
select * from revalidation_selected where fare_mapping = "non mapped fares" qualify row_number() over(partition by search_id,id order by amount_usd asc) = 1

),

-- select fare_mapping,count(*),count(distinct(concat(id,search_id))) from revalidation_Selected_clean  group by 1;
--mapped fares 1112502 1112502

--non mapped fares 365694 365694




base_wide as 
(select 
a.*,
b.fare_ipcc as revalidated_selected_ipcc,
b.total_amount_usd as revalidated_selected_total_amount_usd,
b.amount_usd as revalidated_selected_amount_usd,
b.flight_id as revalidated_selected_flight_id,



case when c.id is not null then "revalidated"
else "not revalidated" end as revalidation_status,
b.fare_mapping,

case when c.id is not null and b.id is not null then "available"
when c.id is not null and b.id is null then "not available"
end as supplier_availability_status 
from
oe_search as a 
left join  
(select search_id,id,flight_id,fare_ipcc,amount_usd,total_amount_usd,fare_mapping from revalidation_Selected_clean group by 1,2,3,4,5,6,7) as b 
on a.id = b.id 
and a.search_id = b.search_id
left join   
(select search_id,id from revalidation_Selected_clean group by 1,2) as c 
on a.id = c.id),


-- select 
-- revalidation_status,
-- count(*),count(distinct(concat(a.id,a.search_id))),count(distinct(case when a.fare_ipcc = b.fare_ipcc then concat(a.id,a.search_id) end)),count(distinct(concat(b.search_id,b.provider_fare_id))) from base_wide as a 
-- left join cte as b 
-- on a.search_id = a.search_id 
-- and a.id = b.provider_fare_id
-- group by 1;
--no provider fares found in the provider fare calculation table for non revalidated results




branded_fare_Calculations as 
(select search_id,flight_id,fare_Id,branded_fare_id,fare_ipcc,booking_ipcc,search_site_code,validating_airline_code,currency_code,
original_total as branded_fare_original_total,
round(a.original_total*b.amount,2) as branded_fare_original_total_usd
from (select * from `wego-cloud.services_curiosity.branded_fare_calculations*` where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore'))))  ) as a 
left join (select * from  `wego-cloud.analytics.exchange_rates*` where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore'))))  ) as b 
on a.currency_code = b.base  
and  date(TIMESTAMP_ADD(TIMESTAMP(a.created_at), INTERVAL 8 HOUR)) = date(b.effective)
where a.endpoint = "COMPARE"
group by 1,2,3,4,5,6,7,8,9,10,11
),

fare_selected as
(
SELECT id,selected  FROM `wego-cloud.integrated_bookings_flights.branded_fares*` where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date('Singapore'), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date('Singapore')))) 
and endpoint = "COMPARE" 
group by 1,2
),


selected_branded_Fares_table as 
(select a.* ,b.selected,case when b.id is null then "non mapped branded fares" else "mapped branded fares" end branded_fare_mapping from branded_fare_Calculations as a 
left join fare_Selected  as b on a.branded_Fare_id = b.id 
where 
(b.selected = true or b.selected is null)

qualify row_number() over(partition by search_id,flight_id order by branded_fare_original_total_usd asc) = 1),




final_table as 
(select a.*,
b.branded_fare_mapping,
b.search_id as branded_search_id,
b.flight_id as branded_flight_id,
split(b.flight_id,':')[1] as branded_flight_id_cleaned,
b.branded_fare_id,
b.fare_ipcc as branded_fare_ipcc,
b.booking_ipcc as branded_booking_ipcc,
b.branded_fare_original_total,
b.branded_fare_original_total_usd,
b.fare_id as branded_msr_fare_id


from base_Wide as a
left join selected_branded_Fares_table as b 
on a.search_id = b.search_id
and a.flight_id = split(b.flight_id,':')[1]),

searches_final_table as 
(select 
b.device_type,
b.site_code,
b.trip_type,
b.trip_category,
b.first_departure_city_code,
b.first_departure_country_code,
b.first_arrival_city_code,
b.first_arrival_country_code,
b.first_departure_date,
b.lead_time,
case when b.search_id is null then 0 else 1 end as data_availability_in_Search_table,
a.*
from final_table as a 
left join 
(select * from `wego-cloud.wego_analytics.flights_searches` WHERE date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) >= date_sub(current_date('Singapore'), interval 2 day)) as b 
on a.search_id = b.search_id)



select 
created_at_date,
data_availability_in_search_table,
device_Type,
site_code,
trip_type,
trip_Category,
first_departure_city_code,
first_departure_country_code,
first_arrival_city_code,
first_arrival_country_code,
first_departure_date,
lead_time,
booking_ipcc,
fare_ipcc,
revalidated_selected_ipcc,
revalidation_status,
fare_mapping,
supplier_availability_status,
branded_fare_mapping,
branded_booking_ipcc,

round((revalidated_selected_total_amount_usd-total_amount_usd)*100/total_amount_usd,2) as revalidated_oe_price_perc_change,

case when abs(round((revalidated_selected_total_amount_usd-total_amount_usd)*100/total_amount_usd,2)) <= 0.5 then "OE-Revalidated price matched"
when abs(round((revalidated_selected_total_amount_usd-total_amount_usd)*100/total_amount_usd,2)) > 0.5  then "OE-Revalidated price not matched"
end as oe_revalidated_price_match_tag,


round((branded_fare_original_total_usd-revalidated_selected_amount_usd)*100/revalidated_selected_amount_usd,2) as branded_revalidated_price_perc_change,

case when abs(round((branded_fare_original_total_usd-revalidated_selected_amount_usd)*100/revalidated_selected_amount_usd,2)) <= 0.5 then "Revalidated-Branded price matched"
when abs(round((branded_fare_original_total_usd-revalidated_selected_amount_usd)*100/revalidated_selected_amount_usd,2)) > 0.5  then "Revalidated-Branded price not matched"
end as revalidated_branded_price_match_tag,


round((branded_fare_original_total_usd-total_amount_usd)*100/total_amount_usd,2) as branded_oe_price_perc_change,

case when abs(round((branded_fare_original_total_usd-total_amount_usd)*100/total_amount_usd,2)) <= 0.5 then "OE-Branded price matched"
when abs(round((branded_fare_original_total_usd-total_amount_usd)*100/total_amount_usd,2)) > 0.5  then "OE-Branded price not matched"
end as oe_branded_price_match_tag,

count(distinct(concat(search_id,flight_id))) as total_fares,

count(distinct(search_id)) as total_searches,

sum(total_amount_usd) as oe_total_amount_usd,

sum(revalidated_selected_total_amount_usd) as revalidated_total_amount_usd,



count(distinct(case when abs(round((revalidated_selected_total_amount_usd-total_amount_usd)*100/total_amount_usd ,2)) <= 0.5 then concat(search_id,flight_id) end)) as revalidated_matched_total_fares,


sum(branded_fare_original_total_usd) as branded_total_amount_usd,


count(distinct(case when abs(round((branded_fare_original_total_usd-revalidated_selected_amount_usd)*100/revalidated_selected_amount_usd,2)) <= 0.5 then concat(search_id,flight_id) end)) as branded_revalidated_matched_total_fares,




count(distinct(case when abs(round((branded_fare_original_total_usd-total_amount_usd)*100/total_amount_usd,2) ) <= 0.5 then concat(search_id,flight_id) end)) as branded_oe_matched_total_fares
from 
searches_final_table where created_at_date = date_sub(current_date('Singapore'), interval 1 day)
group by 
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
{% endraw %}
