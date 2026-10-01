{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_provider_pricing_analysis_daily_append
-- Destination: analysis.hotels_provider_pricing_analysis  (unchanged)
-- Schedule   : every day 01:25   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- -- -- backfill:

-- create table 
-- analysis.hotels_provider_pricing_analysis
-- partition by created_at
-- as

-- SELECT
-- * except(percent_diff_cheapest_provider, night_percent_diff_cheapest_provider),
-- case 
-- when percent_diff_cheapest_provider < -100 then -100
-- when percent_diff_cheapest_provider > 100 then 100
-- else percent_diff_cheapest_provider end as percent_diff_cheapest_provider,
-- case 
-- when night_percent_diff_cheapest_provider < -100 then -100
-- when night_percent_diff_cheapest_provider > 100 then 100
-- else night_percent_diff_cheapest_provider end as night_percent_diff_cheapest_provider,
-- if(percent_diff_cheapest_provider is null, "only provider", "multiple providers") as only_provider_status
-- FROM
-- (with rates --to get the cheapest rate per provider for each search hotel level 
--   as (
-- SELECT
-- * except(provider_rank),
-- ROW_NUMBER() OVER(PARTITION BY search_hotel_id
--       ORDER BY
--         total_amount_usd ASC) AS rank, --to get the ranking of the cheapest rate per provider for a search hotel level
-- FROM
-- (    SELECT
--       DISTINCT 
--       created_at,
--       CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
--       hotel_id,
--       search_id,
--       id AS rate_id,
--       round(price.amount_per_night_usd, 0) as amount_per_night_usd,
--       round(price.total_amount_usd, 0) as total_amount_usd,
--       provider.code AS provider_code,
--       provider.name AS provider_name,
--         ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) , provider.code
--       ORDER BY
--         price.total_amount_usd ASC) AS provider_rank --ranking here to show which is the providers cheapest rate for the search hotel level
--     FROM
--       `wego-cloud.services_akasha.rates*`
--       WHERE _TABLE_SUFFIX BETWEEN '20220101' AND (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day)))))
--       where provider_rank = 1 --to filter out multiple rates from the same provider, so we have the cheapest rate by search hotel level
-- ),

-- win as --to get the cheapest rate for each search hotel level 
-- (select
-- search_hotel_id,
-- provider_code as cheapest_provider_code,
-- provider_name as cheapest_provider_name,
-- amount_per_night_usd as cheapest_amount_per_night_usd,
-- total_amount_usd as cheapest_total_amount_usd
-- from 
-- rates
-- where rank = 1),

-- second as --to get the second cheapest rate for cases when the provider is the cheapest, need to compare against the next cheapest so we can see how much cheaper
-- (select
-- search_hotel_id,
-- amount_per_night_usd as second_cheapest_amount_per_night_usd,
-- total_amount_usd as second_cheapest_total_amount_usd
-- from 
-- rates
-- where rank = 2),

--  clicks as --pulling all the trip info from clicks, this also filters rates to only ones that had a click
--  (SELECT
--     search_id,
--     click_id,
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
--     user_country_code,
--     ts_code,
--     app_version,
--     channel,
--     market,
--     if(conversions_tracked > 0, provider_code, null) as booked_provider_code,
--     provider_code AS clicked_provider_code,
--     ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
--       ORDER BY
--         created_at DESC) AS dedupe --add this as there can be multiple clicks on a hotel, we take the last one as the "click" winner
--   FROM
--     `wego-cloud.wego_analytics.hotels_clicks` cl
--     left join 
--     (SELECT
--     date(created_at) as date,
--     session_id,
--     user_country_code,
--     market,
--     ts_code,
--     app_version,
--     channel
--     FROM
--     `wego-cloud.wego_analytics.sessions`
--   WHERE
--     -- DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--     DATE(_PARTITIONTIME) Between '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--     ) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
--   WHERE
--     -- DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--     DATE(_PARTITIONTIME) Between '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--     ),

--  hotel_details AS #get details of hotels + brands + chains by city and country
--   (SELECT * EXCEPT(rn) FROM
--    (SELECT
--     hotel_services.*,
--     locations.* EXCEPT(city_code),
--     row_number() OVER (PARTITION BY hotel_id) as rn # some duplicates happen after joining hotel_id to locations, 1 hotel id end up with >1 locations
--     FROM  
--       (SELECT
--        hotels.*,
--        hotels_chains.id AS chain_id,
--        hotel_brand_code,
--        COALESCE(hotel_brand, 'no-brand-associated') AS hotel_brand,
--        hotel_chain_code,
--        COALESCE(hotel_chain, 'no-chain-associated') AS hotel_chain
--        FROM
--         (SELECT 
--          CAST(id as STRING) as hotel_id,
--          name_en as hotel_name,
--            brand_id,
--            star AS hotel_stars,
--            city_code,
--            FROM `hotel_services.hotels`
--         ) AS hotels
       
--        left join
--         (SELECT 
--            id,
--            code AS hotel_brand_code,
--            permalink AS hotel_brand,
--            chain_id
--            FROM `hotel_services.brands`
--         ) AS hotels_brands ON hotels.brand_id = hotels_brands.id
       
--        LEFT join
--         (SELECT id,
--             code AS hotel_chain_code,
--             permalink AS hotel_chain
--           FROM `hotel_services.chains`
--         ) AS hotels_chains ON hotels_brands.chain_id = hotels_chains.id
--       ) as hotel_services
    
--     LEFT JOIN
--       (SELECT
--        locations.city_name,
--        locations.city_code,
--        countries.country_name,
--        countries.country_code
--        FROM
--         (SELECT
--          base_name as city_name,
--          code as city_code,
--          country_id as country_id
--          FROM `wego-cloud.place_services.locations`
--         ) AS locations
--        LEFT JOIN
--         (SELECT
--          id as country_id,
--          code as country_code,
--          base_name as country_name
--          FROM `wego-cloud.place_services.countries`
--         ) AS countries on locations.country_id = countries.country_id
--       ) as locations on hotel_services.city_code = locations.city_code
--    )
--    WHERE rn = 1 #deduplicate to get only 1 hotel_id per row
--   )

-- select distinct 
-- date(rates.created_at) as created_at,
-- rates.created_at as timestamp,
-- clicks.* except(dedupe, search_id, hotel_id, click_id),
-- hotel_details.hotel_name,
-- hotel_details.brand_id,
-- hotel_details.hotel_stars,
-- hotel_details.chain_id,
-- hotel_details.hotel_brand_code,
-- hotel_details.hotel_brand,
-- hotel_details.hotel_chain_code,
-- hotel_details.hotel_chain,
-- hotel_details.city_name,
-- hotel_details.country_name,
-- hotel_details.country_code,
-- rates.* except(rank, created_at),
-- rates.rank as pricing_rank,
-- cheapest_provider_code,
-- cheapest_provider_name,
-- cheapest_amount_per_night_usd,
-- cheapest_total_amount_usd,
-- total_amount_usd - if(rank = 1, second_cheapest_total_amount_usd, cheapest_total_amount_usd) as price_diff_cheapest_provider,
-- (total_amount_usd / if(rank = 1, second_cheapest_total_amount_usd, cheapest_total_amount_usd) -1) * 100 as percent_diff_cheapest_provider,
-- amount_per_night_usd - if(rank = 1, second_cheapest_amount_per_night_usd, cheapest_amount_per_night_usd) as night_price_diff_cheapest_provider,
-- (amount_per_night_usd / if(rank = 1, second_cheapest_amount_per_night_usd, cheapest_amount_per_night_usd) -1) * 100 as night_percent_diff_cheapest_provider
-- from
-- rates
-- left join 
-- win on win.search_hotel_id = rates.search_hotel_id
-- left join 
-- second on second.search_hotel_id = rates.search_hotel_id
-- left join 
-- clicks on CONCAT(clicks.search_id, " - ", clicks.hotel_id) = rates.search_hotel_id
-- left join 
-- hotel_details on clicks.hotel_id = cast(hotel_details.hotel_id as int64)
-- where clicks.dedupe = 1 --deduping clicks from earlier
-- order by search_hotel_id, provider_code)





-- daily append:

SELECT
* except(percent_diff_cheapest_provider, night_percent_diff_cheapest_provider),
case 
when percent_diff_cheapest_provider < -100 then -100
when percent_diff_cheapest_provider > 100 then 100
else percent_diff_cheapest_provider end as percent_diff_cheapest_provider,
case 
when night_percent_diff_cheapest_provider < -100 then -100
when night_percent_diff_cheapest_provider > 100 then 100
else night_percent_diff_cheapest_provider end as night_percent_diff_cheapest_provider,
if(percent_diff_cheapest_provider is null, "only provider", "multiple providers") as only_provider_status
FROM
(with rates --to get the cheapest rate per provider for each search hotel level 
  as (
SELECT
* except(provider_rank),
ROW_NUMBER() OVER(PARTITION BY search_hotel_id
      ORDER BY
        total_amount_usd ASC) AS rank, --to get the ranking of the cheapest rate per provider for a search hotel level
FROM
(    SELECT
      DISTINCT 
      created_at,
      CONCAT(search_id, " - ", hotel_id) AS search_hotel_id,
      hotel_id,
      search_id,
      id AS rate_id,
      round(price.amount_per_night_usd, 0) as amount_per_night_usd,
      round(price.total_amount_usd, 0) as total_amount_usd,
      provider.code AS provider_code,
      provider.name AS provider_name,
        ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id) , provider.code
      ORDER BY
        price.total_amount_usd ASC) AS provider_rank --ranking here to show which is the providers cheapest rate for the search hotel level
    FROM
      `wego-cloud.services_akasha.rates*`
      WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day)))))
      where provider_rank = 1 --to filter out multiple rates from the same provider, so we have the cheapest rate by search hotel level
),

win as --to get the cheapest rate for each search hotel level 
(select
search_hotel_id,
provider_code as cheapest_provider_code,
provider_name as cheapest_provider_name,
amount_per_night_usd as cheapest_amount_per_night_usd,
total_amount_usd as cheapest_total_amount_usd
from 
rates
where rank = 1),

second as --to get the second cheapest rate for cases when the provider is the cheapest, need to compare against the next cheapest so we can see how much cheaper
(select
search_hotel_id,
amount_per_night_usd as second_cheapest_amount_per_night_usd,
total_amount_usd as second_cheapest_total_amount_usd
from 
rates
where rank = 2),

 clicks as --pulling all the trip info from clicks, this also filters rates to only ones that had a click
 (SELECT
    search_id,
    click_id,
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
    user_country_code,
    ts_code,
    app_version,
    channel,
    market,
    if(conversions_tracked > 0, provider_code, null) as booked_provider_code,
    provider_code AS clicked_provider_code,
    ROW_NUMBER() OVER(PARTITION BY CONCAT(search_id, " - ", hotel_id)
      ORDER BY
        created_at DESC) AS dedupe --add this as there can be multiple clicks on a hotel, we take the last one as the "click" winner
  FROM
    `wego-cloud.wego_analytics.hotels_clicks` cl
    left join 
    (SELECT
    date(created_at) as date,
    session_id,
    user_country_code,
    market,
    ts_code,
    app_version,
    channel
    FROM
    `wego-cloud.wego_analytics.sessions`
  WHERE
    -- DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) ss on cl.session_id = ss.session_id and date(cl.created_at) = ss.date
  WHERE
    -- DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ),

  hotel_details AS #get details of hotels + brands + chains by city and country
  (SELECT * EXCEPT(rn) FROM
   (SELECT
    hotel_services.*,
    locations.* EXCEPT(city_code),
    row_number() OVER (PARTITION BY hotel_id) as rn # some duplicates happen after joining hotel_id to locations, 1 hotel id end up with >1 locations
    FROM  
      (SELECT
       hotels.*,
       hotels_chains.id AS chain_id,
       hotel_brand_code,
       COALESCE(hotel_brand, 'no-brand-associated') AS hotel_brand,
       hotel_chain_code,
       COALESCE(hotel_chain, 'no-chain-associated') AS hotel_chain
       FROM
        (SELECT 
         CAST(id as STRING) as hotel_id,
         name_en as hotel_name,
           brand_id,
           star AS hotel_stars,
           city_code,
           FROM `hotel_services.hotels`
        ) AS hotels
       
       left join
        (SELECT 
           id,
           code AS hotel_brand_code,
           permalink AS hotel_brand,
           chain_id
           FROM `hotel_services.brands`
        ) AS hotels_brands ON hotels.brand_id = hotels_brands.id
       
       LEFT join
        (SELECT id,
            code AS hotel_chain_code,
            permalink AS hotel_chain
          FROM `hotel_services.chains`
        ) AS hotels_chains ON hotels_brands.chain_id = hotels_chains.id
      ) as hotel_services
    
    LEFT JOIN
      (SELECT
       locations.city_name,
       locations.city_code,
       countries.country_name,
       countries.country_code
       FROM
        (SELECT
         base_name as city_name,
         code as city_code,
         country_id as country_id
         FROM `wego-cloud.place_services.locations`
        ) AS locations
       LEFT JOIN
        (SELECT
         id as country_id,
         code as country_code,
         base_name as country_name
         FROM `wego-cloud.place_services.countries`
        ) AS countries on locations.country_id = countries.country_id
      ) as locations on hotel_services.city_code = locations.city_code
   )
   WHERE rn = 1 #deduplicate to get only 1 hotel_id per row
  )

select distinct 
date(rates.created_at) as created_at,
rates.created_at as timestamp,
clicks.* except(dedupe, search_id, hotel_id, click_id),
hotel_details.hotel_name,
hotel_details.brand_id,
hotel_details.hotel_stars,
hotel_details.chain_id,
hotel_details.hotel_brand_code,
hotel_details.hotel_brand,
hotel_details.hotel_chain_code,
hotel_details.hotel_chain,
hotel_details.city_name,
hotel_details.country_name,
hotel_details.country_code,
rates.* except(rank, created_at),
rates.rank as pricing_rank,
cheapest_provider_code,
cheapest_provider_name,
cheapest_amount_per_night_usd,
cheapest_total_amount_usd,
total_amount_usd - if(rank = 1, second_cheapest_total_amount_usd, cheapest_total_amount_usd) as price_diff_cheapest_provider,
(total_amount_usd / if(rank = 1, second_cheapest_total_amount_usd, cheapest_total_amount_usd) -1) * 100 as percent_diff_cheapest_provider,
amount_per_night_usd - if(rank = 1, second_cheapest_amount_per_night_usd, cheapest_amount_per_night_usd) as night_price_diff_cheapest_provider,
(amount_per_night_usd / if(rank = 1, second_cheapest_amount_per_night_usd, cheapest_amount_per_night_usd) -1) * 100 as night_percent_diff_cheapest_provider
from
rates
left join 
win on win.search_hotel_id = rates.search_hotel_id
left join 
second on second.search_hotel_id = rates.search_hotel_id
left join 
clicks on CONCAT(clicks.search_id, " - ", clicks.hotel_id) = rates.search_hotel_id
left join 
hotel_details on clicks.hotel_id = cast(hotel_details.hotel_id as int64)
where clicks.dedupe = 1 --deduping clicks from earlier
order by search_hotel_id, provider_code)
{% endraw %}
