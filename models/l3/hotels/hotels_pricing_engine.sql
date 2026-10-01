{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_pricing_engine_daily_append
-- Destination: analysis.hotels_pricing_engine  (unchanged)
-- Schedule   : every day 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels', 'wegorates') }}
-- depends_on: {{ source('services_akasha', 'rates') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
--old backfill for older runs:

-- -- backfill:
-- create table analysis.hotels_pricing_engine
-- partition by date
-- as

-- Select 
-- *,
-- min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
-- min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
-- rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
-- FROM
-- (
-- (select
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status
-- from
-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--   lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   "none" as flow_type,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.supplier_base_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.supplier_base_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.supplier_base_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.supplier_base_price end as supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.initial_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.initial_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.initial_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.initial_markup_percentage end as initial_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.action
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.action
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.action
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.action end as action,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as final_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as final_markup_amount,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier,
--   FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_rate,
--   final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--   click_booked,
--   clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   null as fare_clicked,
--   cast(null as string) as fare_status,
--   cast(null as string) as prebook_id,
--   cast(null as string) as markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   cast(null as string) as pre_price_matched_markup_type,
--   null as pre_price_matched_price,
--   null as pre_price_matched_markup_amount,
--   null as pre_price_matched_markup_percentage,
--   null as price_matched_display_price,
--   null as price_matched_loss_threshold_amount,
--   cast(null as string) as price_matched_supplier_code,
--   cast(null as timestamp) as price_matched_supplier_rate_created_at,
--   cast(null as string) as price_matching_status,
--   null as price_matched_price_accuracy,
--   -- listing out all the pricing for all outcome types
--   null as historical_final_price,
--   null as realtime_final_price,
--   null as display_price_final_price,
--   null as supplier_config_final_price,
--   null as historical_markup_amount,
--   null as realtime_markup_amount,
--   null as display_price_markup_amount,
--   null as supplier_config_markup_amount,
--   null as historical_markup_percentage,
--   null as realtime_markup_percentage,
--   null as display_price_markup_percentage,
--   null as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   null as realtime_vs_historical_price_diff,
--   null as realtime_vs_historical_price_diff_perc,
--   null AS historical_cheapest_provider_price_diff_usd,
--   null AS historical_cheapest_provider_price_diff_percentage,
--   null AS realtime_cheapest_provider_price_diff_usd,
--   null AS realtime_cheapest_provider_price_diff_percentage,
--   null as realtime_vs_historical_price_leader_rev_saved,
--   null as realtime_vs_historical_price_competitiveness_change_usd,
--   null as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN (
-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX BETWEEN "20220101" and "20230313"
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com")

-- select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id) a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- LEFT JOIN (
--   select --joining to hotels clicks to get the click info for any clicked search hotel ids, taking the last click, in case we have multiple clicks
--   * except(rank)
--   from
--   (
--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS rank
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2022-01-01' and '2023-03-13') ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2022-01-01' and '2023-03-13')
--     where rank = 1
-- ) c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX BETWEEN "20220101" and "20230313"
--   ))

-- UNION ALL 

-- (select
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status
-- from
-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--   lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   flow_type,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.supplier_base_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.supplier_base_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.supplier_base_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.supplier_base_price end as supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.initial_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.initial_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.initial_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.initial_markup_percentage end as initial_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.action
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.action
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.action
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.action end as action,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as final_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as final_markup_amount,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier,
--   FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_rate,
--   final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--   click_booked,
--   clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   if(click_id is not null, 1, 0) as fare_clicked,
--   case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
--   prebook_id,
--   applied_markup_type as markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   pre_price_matched_markup_type,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as pre_price_matched_price,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as pre_price_matched_markup_amount,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as pre_price_matched_markup_percentage,
--   coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
--   coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
--   displayed_price.supplier_code as price_matched_supplier_code,
--   displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
--   if(coalesce(displayed_price_amount, displayed_price.amount) = case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end, "price matched", "price did not meet tolerance") as price_matching_status,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
--   -- listing out all the pricing for all outcome types
--   price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
--   price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
--   price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
--   price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
--   price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
--   price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
--   round((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) / price_info_by_markup_type.HISTORICAL.final_price * 100, 2) as realtime_vs_historical_price_diff_perc,
--   round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
--   round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
--   if(
--     round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
--     (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
--      null) as realtime_vs_historical_price_leader_rev_saved,
--   (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
--   round(((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100) - ((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN (
-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX BETWEEN "20230314" and (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com")

-- select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id) a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- LEFT JOIN (
--   select --joining to hotels clicks to get the click info for any clicked search hotel ids, taking the last click, in case we have multiple clicks
--   * except(rank)
--   from
--   (
--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS rank
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2023-03-13' and  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN '2023-03-13' and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
--     where rank = 1
-- ) c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX BETWEEN "20230314" and (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--   ))
-- )




-- New backfill for one month or so:

-- backfill:
-- create table analysis.hotels_pricing_engine
-- partition by date
-- as

-- select distinct 
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status,
-- min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
-- min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
-- rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
-- from
-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--   lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   flow_type,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.supplier_base_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.supplier_base_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.supplier_base_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.supplier_base_price end as supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.initial_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.initial_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.initial_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.initial_markup_percentage end as initial_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.action
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.action
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.action
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.action end as action,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as final_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as final_markup_amount,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier,
--   FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_rate,
--   final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--   click_booked,
--   clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   if(click_id is not null, 1, 0) as fare_clicked,
--   case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
--   prebook_id,
--   applied_markup_type as markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   pre_price_matched_markup_type,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as pre_price_matched_price,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as pre_price_matched_markup_amount,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as pre_price_matched_markup_percentage,
--   coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
--   coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
--   displayed_price.supplier_code as price_matched_supplier_code,
--   displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
--   if(coalesce(displayed_price_amount, displayed_price.amount) = case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end, "price matched", "price did not meet tolerance") as price_matching_status,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
--   -- listing out all the pricing for all outcome types
--   price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
--   price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
--   price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
--   price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
--   price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
--   price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
--   round((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) / price_info_by_markup_type.HISTORICAL.final_price * 100, 2) as realtime_vs_historical_price_diff_perc,
--   round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
--   round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
--   if(
--     round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
--     (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
--      null) as realtime_vs_historical_price_leader_rev_saved,
--   (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
--   round(((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100) - ((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN (
-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX between "20230701" and (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com")

-- select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id) a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- LEFT JOIN (
--   select --joining to hotels clicks to get the click info for any clicked search hotel ids, taking the last click, in case we have multiple clicks
--   * except(rank)
--   from
--   (
--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS rank
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) between "2023-07-01" and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     DATE(_PARTITIONTIME) between "2023-07-01" and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
--     where rank = 1
-- ) c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--   )

-- old daily append:


-- select distinct 
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status,
-- min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
-- min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
-- rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
-- from
-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--   lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   flow_type,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.supplier_base_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.supplier_base_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.supplier_base_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.supplier_base_price end as supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.initial_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.initial_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.initial_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.initial_markup_percentage end as initial_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.action
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.action
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.action
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.action end as action,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as final_markup_percentage,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as final_markup_amount,
--   case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier,
--   FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_rate,
--   final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2) / 100) * (100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--   click_booked,
--   clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   if(click_id is not null, 1, 0) as fare_clicked,
--   case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
--   prebook_id,
--   applied_markup_type as markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   pre_price_matched_markup_type,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end as pre_price_matched_price,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_amount
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_amount
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount end as pre_price_matched_markup_amount,
--   case 
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_markup_percentage
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_markup_percentage
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage end as pre_price_matched_markup_percentage,
--   coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
--   coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
--   displayed_price.supplier_code as price_matched_supplier_code,
--   displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
--   if(coalesce(displayed_price_amount, displayed_price.amount) = case 
--   when applied_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when applied_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when applied_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when applied_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end, "price matched", "price did not meet tolerance") as price_matching_status,
--   case
--   when pre_price_matched_markup_type = "DISPLAYED_PRICE_MATCHING" then price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price
--   when pre_price_matched_markup_type = "REALTIME" then price_info_by_markup_type.REALTIME.final_price
--   when pre_price_matched_markup_type = "HISTORICAL" then price_info_by_markup_type.HISTORICAL.final_price
--   when pre_price_matched_markup_type = "SUPPLIER_CONFIG" then price_info_by_markup_type.SUPPLIER_CONFIG.final_price end - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
--   -- listing out all the pricing for all outcome types
--   price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
--   price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
--   price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
--   price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
--   price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
--   price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
--   round((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) / price_info_by_markup_type.HISTORICAL.final_price * 100, 2) as realtime_vs_historical_price_diff_perc,
--   round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
--   round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
--   if(
--     round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
--     (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
--      null) as realtime_vs_historical_price_leader_rev_saved,
--   (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
--   round(((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100) - ((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN (
-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com")

-- select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id) a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- LEFT JOIN (
--   select --joining to hotels clicks to get the click info for any clicked search hotel ids, taking the last click, in case we have multiple clicks
--   * except(rank)
--   from
--   (
--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS rank
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
--     where rank = 1
-- ) c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--   )

-- new version:
----

-----
-- select distinct 
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status,
-- min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
-- min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
-- rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
-- from
-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--   lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   flow_type,
--   supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   initial_markup_percentage,
--   action,
--   final_markup_percentage,
--   final_markup_amount,
--   final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier,
--   FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_rate,
--   final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--   click_booked,
--   clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   if(click_id is not null, 1, 0) as fare_clicked,
--   case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
--   prebook_id,
--   markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   --might be needed 
--   --pre_price_matched_markup_type only populated for displayed_price_matching
--   pre_price_matched_markup_type,
--   pre_price_matched_final_amount as pre_price_matched_price,
--   pre_price_matched_final_markup_amount as pre_price_matched_markup_amount,
--   pre_price_matched_final_markup_percentage as pre_price_matched_markup_percentage,
--   coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
--   coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
--   displayed_price.supplier_code as price_matched_supplier_code,
--   displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
--    if(floor(coalesce(displayed_price_amount, displayed_price.amount)) = floor(coalesce(final_price,0) + coalesce(additional_charges_amount,0)), "price matched", "price did not meet tolerance") as price_matching_status,
--   pre_price_matched_final_amount - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
--   price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
--   price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
--   price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
--   price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
--   price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
--   price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
--   round((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) / price_info_by_markup_type.HISTORICAL.final_price * 100, 2) as realtime_vs_historical_price_diff_perc,
--   round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
--   round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
--   if(
--     round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
--     (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
--      null) as realtime_vs_historical_price_leader_rev_saved,
--   (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
--   round(((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100) - ((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN (
-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com")

-- select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id) a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- LEFT JOIN (
--   select --joining to hotels clicks to get the click info for any clicked search hotel ids, taking the last click, in case we have multiple clicks
--   * except(rank)
--   from
--   (
--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS rank
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
--     where rank = 1
-- ) c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--   )

--latest version



-- with rates as ( --get the provider rank on price for each search hotel id 
--     SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS cheapest_provider,
--       provider.name AS cheapest_provider_name,
--       rooms_count,
--       check_in,
--       check_out,
--       ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         price.total_amount_usd ASC) AS rank
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
-- ),

-- win as 
-- (select --get the cheapest provider info for each search hotel id by rating the 1 rank rates 
-- *
-- from 
-- rates
-- where rank = 1),

-- cheapest_rate_pre as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com"),

-- cheapest_rate_amount_per_night_usd as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY amount_per_night_usd ASC) as amount_per_night_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com"),


-- cheapest_rate_total_amount_usd as 
-- (select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
-- search_hotel_id,
-- first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY total_amount_usd ASC) as total_amount_usd
-- from 
-- rates
-- where cheapest_provider != "hotels.wego.com"),

-- cheapest_rate as

-- (
-- select a.*,b.amount_per_night_usd,c.total_amount_usd from cheapest_rate_pre as a 
-- left join (select search_hotel_id,amount_per_night_usd from cheapest_rate_amount_per_night_usd group by 1,2) as b 
-- on a.search_hotel_id = b.search_hotel_id

-- left join  (select search_hotel_id,total_amount_usd from cheapest_rate_total_amount_usd group by 1,2) as c
-- on a.search_hotel_id = c.search_hotel_id


-- ),



-- rates_base as 

-- (select
-- win.* except(amount_per_night_usd, total_amount_usd),
-- cheapest_rate.* except(search_hotel_id)
-- from
-- win 
-- left join 
-- cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id),


-- sessions as
-- (  SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),

-- hotels_clicks_pre as


-- (SELECT
--     session_id,
--     created_at,
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     conversions_tracked as click_booked,
--     round(total_price_usd, 0) AS clicked_total_price_usd,
--     provider_code AS clicked_provider_code,
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` where DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

--     qualify ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) = 1),


-- hotels_clicks as 

-- (

--   SELECT
--     click_id,
--     search_id,
--     hotel_id,
--     device,
--     device_type,
--     site_code,
--     locale,
--     guests_count,
--     rooms_count,
--     check_in,
--     check_out,
--     lead_time,
--     trip_duration,
--     city_code,
--     country_code,
--     user_country_code,
--     ts_code,
--     market,
--     click_booked,
--     clicked_total_price_usd,
--     clicked_provider_code
--   FROM
--     hotels_clicks_pre as cl
--     left join 
-- sessions as ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date) ,



-- cheapest_supplier_table as
-- (select 
-- CONCAT(search_id, " - ", hotel_id) as search_hotel_id,
-- FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier
-- from
-- `wego-cloud.ib_hotels.wegorates*` WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))),


-- cheapest_supplier_rate_table as
-- (select 
-- CONCAT(search_id, " - ", hotel_id) as search_hotel_id,
-- FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_rate
-- from
-- `wego-cloud.ib_hotels.wegorates*` WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))),


-- cheapest_supplier_price_table as
-- (select 
-- CONCAT(search_id, " - ", hotel_id) as search_hotel_id,
-- FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price
-- from
-- `wego-cloud.ib_hotels.wegorates*` WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))),



-- pricing_table_pre_without_window_function as 

-- (SELECT
--   DISTINCT 
--   date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
--   TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
--   r.search_id,
--   r.hotel_id,
--   CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
--   a.rate_id,
--   room_type_id,
--   --click_id,
--   --device,
--   --device_type,
--   --site_code,
--   --locale,
--   --guests_count,
--   -- c.rooms_count,
--   -- c.check_in,
--   -- c.check_out,
--  -- lead_time,
--   --trip_duration,
--   --city_code,
--   --country_code,
--   --if(country_code = user_country_code, "domestic", "international") as trip_category,
--   --user_country_code,
--   --market,
--   --ts_code,
--   strikethrough_price,
--   supplier_min_price,
--   flow_type,
--   supplier_base_price,
--   tax_amount,
--   rule_id,
--   markup_id,
--   min_markup_percentage,
--   max_markup_percentage,
--   initial_markup_percentage,
--   action,
--   final_markup_percentage,
--   final_markup_amount,
--   final_price,
--   supplier_code,
--   -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
--   -- FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier,
--   -- FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_rate,
--   -- final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
--   -- ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
--   -- getting the provider level information so we can tell who was the cheapest provider
--   amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
--   total_amount_usd AS cheapest_provider_total_amount_usd,
--   cheapest_provider,
--   cheapest_provider_name,
--   -- calculating the price competitiveness
--   round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
--   ROUND((round(final_price,0) / total_amount_usd -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
--   -- working out the original price before markup
--   (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage) AS original_price,
--   round( (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
--   ROUND((round( (round((supplier_base_price + tax_amount),2)*100)/(100 + initial_markup_percentage),0) / total_amount_usd -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
--  -- click_booked,
--   --clicked_provider_code,
--   -- new fields added to the script after 2023-03-16
--   --if(click_id is not null, 1, 0) as fare_clicked,
--   --case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
--   prebook_id,
--   markup_type,
--   -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
--   --might be needed 
--   --pre_price_matched_markup_type only populated for displayed_price_matching
--   pre_price_matched_markup_type,
--   pre_price_matched_final_amount as pre_price_matched_price,
--   pre_price_matched_final_markup_amount as pre_price_matched_markup_amount,
--   pre_price_matched_final_markup_percentage as pre_price_matched_markup_percentage,
--   coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
--   coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
--   displayed_price.supplier_code as price_matched_supplier_code,
--   displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
--    if(floor(coalesce(displayed_price_amount, displayed_price.amount)) = floor(coalesce(final_price,0) + coalesce(additional_charges_amount,0)), "price matched", "price did not meet tolerance") as price_matching_status,
--   pre_price_matched_final_amount - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
--   price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
--   price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
--   price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
--   price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
--   price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
--   price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
--   price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
--   price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
--   -- realtime pricing analysis
--   price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
--   round((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) / price_info_by_markup_type.HISTORICAL.final_price * 100, 2) as realtime_vs_historical_price_diff_perc,
--   round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
--   round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
--   round((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
--   if(
--     round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
--     (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
--      null) as realtime_vs_historical_price_leader_rev_saved,
--   (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
--   round(((round(price_info_by_markup_type.REALTIME.final_price,0) / total_amount_usd -1) * 100) - ((round(price_info_by_markup_type.HISTORICAL.final_price,0) / total_amount_usd -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
-- FROM
--   `wego-cloud.ib_hotels.wegorates*` r
-- LEFT JOIN rates_base as a
-- ON
--   CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
-- -- LEFT JOIN hotels_clicks as c
-- -- ON
-- --   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--   ),

--   pricing_table_pre as (

--   select a.* ,
--   b.cheapest_supplier,
--   c.cheapest_supplier_rate,
--   final_price - d.cheapest_supplier_price AS cheapest_supplier_price_diff_usd,
--   ROUND((final_price / d.cheapest_supplier_price -1) * 100, 2) AS cheapest_supplier_price_diff_percentage

--   from pricing_table_pre_without_window_function as a 
--   left join  (select search_hotel_id , cheapest_supplier from cheapest_supplier_table group by 1,2) as b 
--   on a.search_hotel_id = b.search_hotel_id

--   left join (select search_hotel_id,cheapest_supplier_rate from cheapest_supplier_rate_table group by 1,2)  as c 
--   on a.search_hotel_id = c.search_hotel_id

--   left join (select search_hotel_id,cheapest_supplier_price from cheapest_supplier_price_table group by 1,2 ) as d 
--   on a.search_hotel_id = d.search_hotel_id
--   ),


--   pricing_table as 
-- ( 
-- select a.*,
--   click_id,
--   device,
--   device_type,
--   site_code,
--   locale,
--   guests_count,
--   c.rooms_count,
--   c.check_in,
--   c.check_out,
--  lead_time,
--   trip_duration,
--   city_code,
--   country_code,
--   if(country_code = user_country_code, "domestic", "international") as trip_category,
--   user_country_code,
--   market,
--   ts_code,
--   click_booked,
--   clicked_provider_code,
--   if(click_id is not null, 1, 0) as fare_clicked,
--   case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,

-- from pricing_table_pre as a 
-- LEFT JOIN hotels_clicks as c
-- ON
--   CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
-- ),




-- final as 
-- (select distinct 
-- *,
-- if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
-- if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
-- if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
-- if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status,
-- min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
-- min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
-- rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
-- from pricing_table)



-- select 
-- date,
-- created_at,
-- search_id,
-- hotel_id,
-- search_hotel_id,
-- rate_id,
-- room_type_id,
-- click_id,
-- device,
-- device_type,
-- site_code,
-- locale,
-- guests_count,
-- rooms_count,
-- check_in,
-- check_out,
-- lead_time,
-- trip_duration,
-- city_code,
-- country_code,
-- trip_category,
-- user_country_code,
-- market,
-- ts_code,
-- strikethrough_price,
-- supplier_min_price,
-- flow_type,
-- supplier_base_price,
-- tax_amount,
-- rule_id,
-- markup_id,
-- min_markup_percentage,
-- max_markup_percentage,
-- initial_markup_percentage,
-- action,
-- final_markup_percentage,
-- final_markup_amount,
-- final_price,
-- supplier_code,
-- cheapest_supplier,
-- cheapest_supplier_rate,
-- cheapest_supplier_price_diff_usd,
-- cheapest_supplier_price_diff_percentage,
-- cheapest_provider_amount_per_night_usd,
-- cheapest_provider_total_amount_usd,
-- cheapest_provider,
-- cheapest_provider_name,
-- cheapest_provider_price_diff_usd,
-- cheapest_provider_price_diff_percentage,
-- original_price,
-- original_cheapest_provider_price_diff_usd,
-- original_cheapest_provider_price_diff_percentage,
-- click_booked,
-- clicked_provider_code,
-- fare_clicked,
-- fare_status,
-- prebook_id,
-- markup_type,
-- pre_price_matched_markup_type,
-- pre_price_matched_price,
-- pre_price_matched_markup_amount,
-- pre_price_matched_markup_percentage,
-- price_matched_display_price,
-- price_matched_loss_threshold_amount,
-- price_matched_supplier_code,
-- price_matched_supplier_rate_created_at,
-- price_matching_status,
-- price_matched_price_accuracy,
-- historical_final_price,
-- realtime_final_price,
-- display_price_final_price,
-- supplier_config_final_price,
-- historical_markup_amount,
-- realtime_markup_amount,
-- display_price_markup_amount,
-- supplier_config_markup_amount,
-- historical_markup_percentage,
-- realtime_markup_percentage,
-- display_price_markup_percentage,
-- supplier_config_markup_percentage,
-- realtime_vs_historical_price_diff,
-- realtime_vs_historical_price_diff_perc,
-- historical_cheapest_provider_price_diff_usd,
-- historical_cheapest_provider_price_diff_percentage,
-- realtime_cheapest_provider_price_diff_usd,
-- realtime_cheapest_provider_price_diff_percentage,
-- realtime_vs_historical_price_leader_rev_saved,
-- realtime_vs_historical_price_competitiveness_change_usd,
-- realtime_vs_historical_price_competitiveness_perc_change_absolute,
-- rate_is_cheapest_supplier,
-- rate_is_cheapest_provider,
-- rate_is_clicked_provider,
-- provider_count_status,
-- cheapest_final_price_display_price_matching,
-- cheapest_pre_price_matched_price,
-- cheapest_rate_per_search_hotel_rank
--  from final;


--updated version 

with rates as ( --get the provider rank on price for each search hotel id 
    SELECT
      DISTINCT 
      created_at,
      CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
      id AS rate_id,
      
      round((coalesce(price.amount_per_night_usd,0)

 + coalesce(case when price.local_tax_amount_usd <= 0  then 0 else price.local_tax_amount_usd end ,0))) as amount_per_night_usd,


       round((coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0))) as total_amount_usd,
      provider.code AS cheapest_provider,
      provider.name AS cheapest_provider_name,
      rooms_count,
      check_in,
      check_out,
      ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY  coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0),price.base_amount_usd ASC) as rank

    FROM
      `wego-cloud.services_akasha.rates*`
      WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))

),
 
 win as 

 (
select * from rates where rank = 1


 ),

cheapest_rate as 
(select --get the cheapest rate for each search hotel id excluding wego, so we can work out who the next cheapest provider was when wego was the cheapest
search_hotel_id,
first_value(amount_per_night_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY rank ASC) as amount_per_night_usd,
first_value(total_amount_usd) OVER(PARTITION BY CONCAT(search_hotel_id) ORDER BY rank ASC) as total_amount_usd
from 
rates
where cheapest_provider != "hotels.wego.com"),

rates_base as 

(select
win.* except(amount_per_night_usd, total_amount_usd),
cheapest_rate.* except(search_hotel_id)
from
win 
left join 
(select search_hotel_id,amount_per_night_usd,total_amount_usd  from  cheapest_rate group by 1,2,3) as cheapest_rate on win.search_hotel_id = cheapest_rate.search_hotel_id),


---unchanged as previous 

sessions as
(  SELECT
    date(created_at) as date,
    session_id,
    user_country_code,
    market,
    ts_code
    FROM
    `wego-cloud.wego_analytics.sessions`
  WHERE
    DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),

hotels_clicks_pre as


(SELECT
    session_id,
    created_at,
    click_id,
    search_id,
    hotel_id,
    device,
    device_type,
    site_code,
    locale,
    guests_count,
    rooms_count,
    check_in,
    check_out,
    lead_time,
    trip_duration,
    city_code,
    country_code,
    conversions_tracked as click_booked,
    round(total_price_usd, 0) AS clicked_total_price_usd,
    provider_code AS clicked_provider_code,
  FROM
    `wego-cloud.wego_analytics.hotels_clicks` where DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

    qualify ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
      ORDER BY
        created_at DESC) = 1),


hotels_clicks as 

(

  SELECT
    click_id,
    search_id,
    hotel_id,
    device,
    device_type,
    site_code,
    locale,
    guests_count,
    rooms_count,
    check_in,
    check_out,
    lead_time,
    trip_duration,
    city_code,
    country_code,
    user_country_code,
    ts_code,
    market,
    click_booked,
    clicked_total_price_usd,
    clicked_provider_code
  FROM
    hotels_clicks_pre as cl
    left join 
sessions as ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date) ,



----changed 

wegorates as    
(
select * except(final_price),
coalesce(final_price,0)+ coalesce(additional_charges_amount,0) as final_price
 from `wego-cloud.ib_hotels.wegorates*` WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
and flow_type = "CREATE_SEARCH"
qualify row_number() over(partition by search_id,hotel_id,supplier_code order by coalesce(final_price,0)+ coalesce(additional_charges_amount,0),coalesce(final_price,0) asc) = 1

),



cheapest_supplier_table as
(select 
CONCAT(search_id, " - ", hotel_id) as search_hotel_id,
FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY coalesce(final_price,0),coalesce(final_price,0) - coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier,
FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY coalesce(final_price,0),coalesce(final_price,0) - coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_rate,
FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) ORDER BY coalesce(final_price,0),coalesce(final_price,0) - coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_price
from
wegorates),





pricing_table_pre_without_window_function as 

(SELECT
  DISTINCT 
  date(TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR)) as date,
  TIMESTAMP_ADD(coalesce(r.created_at,a.created_at), INTERVAL 8 HOUR) as created_at,
  r.search_id,
  r.hotel_id,
  CONCAT(r.search_id, " - ", r.hotel_id) AS search_hotel_id,
  a.rate_id,
  room_type_id,
  --click_id,
  --device,
  --device_type,
  --site_code,
  --locale,
  --guests_count,
  -- c.rooms_count,
  -- c.check_in,
  -- c.check_out,
 -- lead_time,
  --trip_duration,
  --city_code,
  --country_code,
  --if(country_code = user_country_code, "domestic", "international") as trip_category,
  --user_country_code,
  --market,
  --ts_code,
  strikethrough_price,
  supplier_min_price,
  flow_type,
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
  supplier_code,
  -- getting the supplier level information so we can tell who is the cheapest provider for the search hotel id
  -- FIRST_VALUE(supplier_code) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier,
  -- FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY coalesce(final_price,0) + coalesce(additional_charges_amount,0) ASC) AS cheapest_supplier_rate,
  -- final_price - FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) AS cheapest_supplier_price_diff_usd,
  -- ROUND((final_price / FIRST_VALUE(final_price) OVER(PARTITION BY CONCAT(r.search_id, " - ", r.hotel_id) ORDER BY final_price ASC) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage,
  -- getting the provider level information so we can tell who was the cheapest provider
  amount_per_night_usd AS cheapest_provider_amount_per_night_usd,
  total_amount_usd AS cheapest_provider_total_amount_usd,
  cheapest_provider,
  cheapest_provider_name,
  -- calculating the price competitiveness
  round(final_price, 0) - total_amount_usd AS cheapest_provider_price_diff_usd,
  ROUND((safe_divide(round(final_price,0) , total_amount_usd) -1) * 100, 2) AS cheapest_provider_price_diff_percentage,
  -- working out the original price before markup
  safe_divide((round((supplier_base_price + tax_amount),2)*100),(100 + initial_markup_percentage)) AS original_price,
  round( safe_divide((round((supplier_base_price + tax_amount),2)*100),(100 + initial_markup_percentage)), 0) - total_amount_usd AS original_cheapest_provider_price_diff_usd,
  ROUND((safe_divide(round(safe_divide( (round((supplier_base_price + tax_amount),2)*100),(100 + initial_markup_percentage)),0) , total_amount_usd) -1) * 100, 2) AS original_cheapest_provider_price_diff_percentage,
 -- click_booked,
  --clicked_provider_code,
  -- new fields added to the script after 2023-03-16
  --if(click_id is not null, 1, 0) as fare_clicked,
  --case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,
  prebook_id,
  markup_type,
  -- Google price matching (note, need to remove the coalesce from the display_matched_price fields after old fields are switched off)
  --might be needed 
  --pre_price_matched_markup_type only populated for displayed_price_matching
  pre_price_matched_markup_type,
  pre_price_matched_final_amount as pre_price_matched_price,
  pre_price_matched_final_markup_amount as pre_price_matched_markup_amount,
  pre_price_matched_final_markup_percentage as pre_price_matched_markup_percentage,
  coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price,
  coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount,
  displayed_price.supplier_code as price_matched_supplier_code,
  displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at,
   if(floor(coalesce(displayed_price_amount, displayed_price.amount)) = floor(coalesce(final_price,0) + coalesce(additional_charges_amount,0)), "price matched", "price did not meet tolerance") as price_matching_status,
  pre_price_matched_final_amount - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy,
  price_info_by_markup_type.HISTORICAL.final_price as historical_final_price,
  price_info_by_markup_type.REALTIME.final_price as realtime_final_price,
  price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_price as display_price_final_price,
  price_info_by_markup_type.SUPPLIER_CONFIG.final_price as supplier_config_final_price,
  price_info_by_markup_type.HISTORICAL.final_markup_amount as historical_markup_amount,
  price_info_by_markup_type.REALTIME.final_markup_amount as realtime_markup_amount,
  price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_amount as display_price_markup_amount,
  price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_amount as supplier_config_markup_amount,
  price_info_by_markup_type.HISTORICAL.final_markup_percentage as historical_markup_percentage,
  price_info_by_markup_type.REALTIME.final_markup_percentage as realtime_markup_percentage,
  price_info_by_markup_type.DISPLAYED_PRICE_MATCHING.final_markup_percentage as display_price_markup_percentage,
  price_info_by_markup_type.SUPPLIER_CONFIG.final_markup_percentage as supplier_config_markup_percentage,
  -- realtime pricing analysis
  price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price as realtime_vs_historical_price_diff,
  round(safe_divide((price_info_by_markup_type.REALTIME.final_price - price_info_by_markup_type.HISTORICAL.final_price) , price_info_by_markup_type.HISTORICAL.final_price) * 100, 2) as realtime_vs_historical_price_diff_perc,
  round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd AS historical_cheapest_provider_price_diff_usd,
  round((safe_divide(round(price_info_by_markup_type.HISTORICAL.final_price,0) , total_amount_usd) -1) * 100, 2) AS historical_cheapest_provider_price_diff_percentage,
  round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd AS realtime_cheapest_provider_price_diff_usd,
  round((safe_divide(round(price_info_by_markup_type.REALTIME.final_price,0) , total_amount_usd) -1) * 100, 2) AS realtime_cheapest_provider_price_diff_percentage,
  if(
    round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd < 0, 
    (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd),
     null) as realtime_vs_historical_price_leader_rev_saved,
  (round(price_info_by_markup_type.REALTIME.final_price, 0) - total_amount_usd) - (round(price_info_by_markup_type.HISTORICAL.final_price, 0) - total_amount_usd) as realtime_vs_historical_price_competitiveness_change_usd,
  round(((safe_divide(round(price_info_by_markup_type.REALTIME.final_price,0) , total_amount_usd) -1) * 100) - ((safe_divide(round(price_info_by_markup_type.HISTORICAL.final_price,0) , total_amount_usd) -1) * 100), 2)  as realtime_vs_historical_price_competitiveness_perc_change_absolute
FROM
  wegorates as r
LEFT JOIN rates_base as a
ON
  CONCAT(r.search_id, " - ", r.hotel_id) = a.search_hotel_id

  ),


  pricing_table_pre as (

  select a.* ,
  b.cheapest_supplier,
  b.cheapest_supplier_rate,
  final_price - b.cheapest_supplier_price AS cheapest_supplier_price_diff_usd,
  ROUND((safe_divide(final_price , b.cheapest_supplier_price) -1) * 100, 2) AS cheapest_supplier_price_diff_percentage

  from pricing_table_pre_without_window_function as a 
  left join  (select search_hotel_id , cheapest_supplier , cheapest_supplier_Rate,cheapest_supplier_price from cheapest_supplier_table group by 1,2,3,4) as b 
  on a.search_hotel_id = b.search_hotel_id

  ),

    pricing_table as 
( 
select a.*,
  click_id,
  device,
  device_type,
  site_code,
  locale,
  guests_count,
  c.rooms_count,
  c.check_in,
  c.check_out,
 lead_time,
  trip_duration,
  city_code,
  country_code,
  if(country_code = user_country_code, "domestic", "international") as trip_category,
  user_country_code,
  market,
  ts_code,
  click_booked,
  clicked_provider_code,
  if(click_id is not null, 1, 0) as fare_clicked,
  case when click_id is not null then "fare clicked" when click_booked > 0 then "fare booked" else "fare searched" end as fare_status,

from pricing_table_pre as a 
LEFT JOIN hotels_clicks as c
ON
  CONCAT(c.search_id, " - ", c.hotel_id) = a.search_hotel_id
),

final as 
(select distinct 
*,
if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier,
if(cheapest_provider = "hotels.wego.com", 1, 0) as rate_is_cheapest_provider,
if(clicked_provider_code = "hotels.wego.com", 1, 0) as rate_is_clicked_provider,
if(cheapest_provider_total_amount_usd is null, "only provider", "muiltiple providers") as provider_count_status,
min(final_price) over( partition by search_hotel_id) as cheapest_final_price_display_price_matching,
min(pre_price_matched_price) over( partition by search_hotel_id) as cheapest_pre_price_matched_price,
rank() OVER(partition by search_hotel_id order by final_price asc) as cheapest_rate_per_search_hotel_rank
from pricing_table)



select 
date,
created_at,
search_id,
hotel_id,
search_hotel_id,
rate_id,
room_type_id,
click_id,
device,
device_type,
site_code,
locale,
guests_count,
rooms_count,
check_in,
check_out,
lead_time,
trip_duration,
city_code,
country_code,
trip_category,
user_country_code,
market,
ts_code,
strikethrough_price,
supplier_min_price,
flow_type,
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
supplier_code,
cheapest_supplier,
cheapest_supplier_rate,
cheapest_supplier_price_diff_usd,
cheapest_supplier_price_diff_percentage,
cheapest_provider_amount_per_night_usd,
cheapest_provider_total_amount_usd,
cheapest_provider,
cheapest_provider_name,
cheapest_provider_price_diff_usd,
cheapest_provider_price_diff_percentage,
original_price,
original_cheapest_provider_price_diff_usd,
original_cheapest_provider_price_diff_percentage,
click_booked,
clicked_provider_code,
fare_clicked,
fare_status,
prebook_id,
markup_type,
pre_price_matched_markup_type,
pre_price_matched_price,
pre_price_matched_markup_amount,
pre_price_matched_markup_percentage,
price_matched_display_price,
price_matched_loss_threshold_amount,
price_matched_supplier_code,
price_matched_supplier_rate_created_at,
price_matching_status,
price_matched_price_accuracy,
historical_final_price,
realtime_final_price,
display_price_final_price,
supplier_config_final_price,
historical_markup_amount,
realtime_markup_amount,
display_price_markup_amount,
supplier_config_markup_amount,
historical_markup_percentage,
realtime_markup_percentage,
display_price_markup_percentage,
supplier_config_markup_percentage,
realtime_vs_historical_price_diff,
realtime_vs_historical_price_diff_perc,
historical_cheapest_provider_price_diff_usd,
historical_cheapest_provider_price_diff_percentage,
realtime_cheapest_provider_price_diff_usd,
realtime_cheapest_provider_price_diff_percentage,
realtime_vs_historical_price_leader_rev_saved,
realtime_vs_historical_price_competitiveness_change_usd,
realtime_vs_historical_price_competitiveness_perc_change_absolute,
rate_is_cheapest_supplier,
rate_is_cheapest_provider,
rate_is_clicked_provider,
provider_count_status,
cheapest_final_price_display_price_matching,
cheapest_pre_price_matched_price,
cheapest_rate_per_search_hotel_rank
 from final
{% endraw %}
