{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Hotels_display_price_matching_insert_job
-- Destination: analysis.hotels_display_price_matching_master  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels', 'sessions') }}
-- depends_on: {{ source('ib_hotels', 'wegorates') }}
-- depends_on: {{ source('services_genzo', 'hotels_searches') }}
{% raw %}
-- create table 
-- analysis.hotels_display_price_matching_master
-- partition by date
-- as

with cte as 
(SELECT
  search_id,
  hotel_id,
  CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
  supplier_code,
  room_type_id,
  wego_room_type_id,
  strikethrough_price,
  supplier_min_price,
  supplier_base_price,
  tax_amount,
  rule_id,
  markup_id,
  min_markup_percentage,
  max_markup_percentage,
  initial_markup_percentage,
  action,
  final_markup_percentage,
  final_markup_amount,
  final_price,
  pre_price_matched_final_markup_percentage,
  pre_price_matched_final_markup_amount,
  pre_price_matched_final_amount,
  pre_price_matched_markup_type,
  displayed_price_amount,
  displayed_price_loss_threshold_amount,
  currency_code,
  flow_type,
  markup_type,
  marketing_fee,
  additional_charges_amount,
  pre_price_matched_final_amount-pre_price_matched_final_markup_amount as original_price,
  coalesce(additional_charges_amount,0) + coalesce(final_price,0) as total_final_price,
  
  case when floor(coalesce(additional_charges_amount,0) + coalesce(final_price,0)) = floor(displayed_price_amount) then "Price matched after markup" else "Price not matched after markup" end as price_matching_status,
  
  -- case when 
  -- final_markup_amount >= 0 then 0 else 
  -- coalesce(final_markup_amount,0) + coalesce(marketing_fee,0) end as net_loss_threshold_amount_Applied,
  
   case when 
   supplier_code in ("boognik69qsa","epsbkjh9ro") then 
  coalesce(final_markup_amount,0) + coalesce(marketing_fee,0) 
  else final_markup_amount end as net_profit,
  --net_loss_threshold_amount_Applied,
  
  coalesce(additional_charges_amount,0) + coalesce(final_price,0) - displayed_price_amount as price_variance
FROM
  `wego-cloud.ib_hotels.wegorates*` where
  --supplier_code = displayed_price.supplier_code and
  displayed_price_amount is not null
  and flow_type = "CREATE_SEARCH" 
  and _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
  and date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) = date_sub(current_date(), interval 1 day)

 qualify row_number() over(partition by search_id,hotel_id order by coalesce(additional_charges_amount,0) + coalesce(final_price,0) asc ) = 1),

 cte_1 as
(
SELECT
  date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) as date,
  TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
  created_at as created_at_og,
  displayed_price_by_hotel_id[SAFE_OFFSET(0)].source_id as supplier_code,
  displayed_price_by_hotel_id[SAFE_OFFSET(0)].supplier_rate_created_at as supplier_rate_created_at,
  TIMESTAMP_DIFF(created_at, displayed_price_by_hotel_id[SAFE_OFFSET(0)].supplier_rate_created_at, MINUTE)  as time_difference_from_displayed_price,
  search_id,
  cast(hotel_id as string) as hotel_id,
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
  device,
  language_code,
  wg_campaign,
  ts_code,
  displayed_price_by_hotel_id[SAFE_OFFSET(0)].search_id as supplier_search_id
FROM
  `wego-cloud.ib_hotels.sessions*`,
  UNNEST(hotel_ids) AS hotel_id
  where  _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
  and date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) = date_sub(current_date(), interval 1 day) and displayed_price_by_hotel_id[SAFE_OFFSET(0)].source_id is not null and 
  CAST(displayed_price_by_hotel_id[SAFE_OFFSET(0)].hotel_id AS string) = cast(hotel_id as string)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26),


  final as 

  (select a.* EXCEPT(search_id,hotel_id,search_hotel_id),
  b.search_id,
  b.hotel_id,
  CONCAT(b.search_id, " - ", b.hotel_id) AS search_hotel_id,
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
  b.site_code,
  b.device,
  b.language_code,
  b.wg_campaign,
  b.date,
  b.created_at,
  b.created_at_og,
  b.supplier_rate_created_at,
  b.time_difference_from_displayed_price,
  b.supplier_code as supplier_displayed,
  case when a.search_id is null then "unavailable" else "available" end as availabilty_flag,
  b.ts_code,
  b.supplier_search_id
  from cte_1 as b 
  left join cte as a 
  on a.hotel_id = b.hotel_id
  and a.search_id = b.search_id
  and b.date = date_sub(current_date(), interval 1 day))


  
  select final_table.*,c.is_google,c.city_code,c.country_code from final as final_table left join 
 (SELECT Search.id as search_id,max(case when  lower(page.url) LIKE '%isgoogle=true%' then 
    1 
    when 
    lower(page.url) LIKE '%skyscanner%' then 2
    else 0 end) as is_google,max(search.city_code) as city_code,max(search.country_code) as country_code FROM `wego-cloud.services_genzo.hotels_searches*` 
    where _TABLE_SUFFIX >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 3 day)))) group by 1 ) as c 
 on final_table.search_id = c.search_id;
{% endraw %}
