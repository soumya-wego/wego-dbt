{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : skyscanner_bid_hotel_list
-- Destination: analysis.skyscanner_bid_hotel_list  (unchanged)
-- Schedule   : every sun 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('bowh_supplier_availability_aggregated') }}
-- depends_on: {{ source('analysis', 'hotels_detail_list') }}
-- depends_on: {{ source('analysis', 'skyscanner_top_hotel_list_monthly') }}
-- depends_on: {{ source('distribution_partner_reports_hotels', 'skyscanner_auction_insights_report') }}
-- depends_on: {{ source('hotel_services', 'hotels') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('wego_analytics', 'hotels_searches') }}
{% raw %}
-- create or replace table `analysis.skyscanner_bid_hotel_list` as
--
-- CHANGE 2026-07-08: added monthly_top_hotels CTE
-- Reads from live external table skyscanner_top_hotel_list_monthly (Google Sheet)
-- Inner-joins to hotels_detail_list (silently excludes invalid IDs)
-- Deduplicates multi-market hotels: alphabetically first market wins (AU < CA < US)
-- Anti-joins against current_list to insert only genuinely new hotels
-- Sets campaign_type = market (target bidding market from sheet)
-- Uses sentinel date_added = 2099-01-01 so bid_file_upload can filter by date

with current_list as
(
  -- pulls the current existing hotel list from the target table
  -- this is used as the reference set to:
  -- 1. identify hotels already in the list
  -- 2. exclude existing hotels from new ranking candidates
  -- 3. find previously listed hotels that may be reintroduced
  select distinct
    *
  from `analysis.skyscanner_bid_hotel_list`
)

, supplier_availability as
(
  -- aggregates recent supplier availability performance at hotel level
  -- over the last 7 days for b2c/meta traffic on Feed page
  -- this creates the base pool of hotels with acceptable supply health
  select
    hotel_id

    -- number of distinct suppliers currently serving the hotel
    , count(distinct supplier_name) as total_supplier

    -- total successful availability responses
    , sum(total_available) as total_available

    -- total request volume
    , sum(total_request) as total_request

    -- flag to identify whether wegobeds supplier is present for the hotel
    , max(case when supplier_code = 'wgobsdgtyetx' then 1 else 0 end) as has_wegobeds_supplier

    -- hotel level availability rate
    , safe_divide(sum(total_available), sum(total_request)) as avail_rate
  from `wego-cloud.analysis.bowh_supplier_availability_aggregated`
  where date >= current_date - interval '7' day
  and supplier_channel in ('b2c','meta')
  and page = 'Feed'
  group by 1

  -- keep only hotels with at least 30% recent availability rate
  having safe_divide(total_available, total_request) >= 0.3
)

, hotel_list as
(
  -- enriches the eligible supplier availability pool with hotel metadata
  -- adds city and country context
  -- keeps only hotels with strong enough supplier coverage
  -- and removes disabled hotels
  select
    a.*
    , c.code as city_code
    , c.base_name as city_name
    , d.code as country_code
    , name_en
  from supplier_availability a
  left join `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
  where ( has_wegobeds_supplier = 1
    or total_supplier >= 2)
    and not disabled
)

, top_destination as
(
  -- ranks countries by recent hotel search demand over the last 14 days
  -- this ranking is later used to prioritize new hotels from stronger demand markets
  select
    country_code

    -- number of distinct hotel searches by country
    , count(distinct search_id) as total_searches

    -- lower rank means higher search demand
    , rank()over(order by count(distinct search_id) desc) as country_rank
  from `wego_analytics.hotels_searches`
  where date(created_at) >= current_date - interval '14' day
    and country_code <> 'IN'
  group by 1
)

, reintro_hotels as
(
  -- identifies hotels that were already in the current list before
  -- but have not been live recently and should now be reconsidered
  -- reintroduction requires:
  -- 1. the hotel existed in the old/current list
  -- 2. the hotel has current supplier availability health
  -- 3. the hotel had past available impression history
  -- 4. the hotel has been inactive for at least 30 days
  select
    c.* except(date_added)

    -- refresh date_added because this hotel is being reintroduced today
    , current_date as date_added
  from (
    -- summarizes auction insights history over the last 60 days
    -- to determine when the hotel was last live
    -- and whether it had meaningful available impressions
    select
      partner_property_id

      -- most recent day the hotel was live in auction insights
      , max(date) as last_live

      -- proxy for historical available impressions
      , sum(coalesce(hotel_impressions,0)-coalesce(avail_impressions_missed,0)) as total_avail_impressions
    from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report`
    where date >= current_date - interval '60' day
    group by 1
  ) a
  join supplier_availability b
    on partner_property_id = hotel_id
  join current_list c
    on a.partner_property_id = c.partner_property_id
  where date_diff(current_date,last_live,day) >= 30
  and total_avail_impressions > 0

  -- prioritize hotels with better current availability rate first
  -- then stronger historical available impressions
  order by avail_rate desc, total_avail_impressions desc

  -- only keep top 100 reintroduction candidates
  limit 100
)

, ranking as
(
  -- creates the net new hotel candidate list
  -- this includes eligible hotels not already present in current_list
  -- hotels are prioritized by country demand rank first
  -- then by hotel request volume
  select distinct
    hotel_id as partner_property_id
    , a.country_code
    , cast(null as string) as campaign_type
    , current_date as date_added
    , total_request

    -- countries missing from demand ranking are pushed lower with rank 1000
    , coalesce(country_rank,1000) as country_rank
  from hotel_list a
  left join current_list b
    on partner_property_id = hotel_id
  left join top_destination c
    on a.country_code = c.country_code

  -- only keep hotels that are not already in the current list
  where b.partner_property_id is null

  -- prioritize by destination demand, then by request volume
  order by coalesce(country_rank,1000), total_request desc

  -- only keep top 1000 new hotels
  limit 1000
)

, monthly_top_hotels as
(
  -- ADDED 2026-07-08: monthly top hotel list from Google Sheets (live external table)
  -- inner join to hotels_detail_list silently excludes hotel IDs not in inventory
  -- row_number deduplicates multi-market hotels: alphabetically first market wins
  -- anti-join against current_list ensures only genuinely new hotels are inserted
  -- campaign_type = market column from sheet (target bidding market)
  -- sentinel date_added = 2099-01-01 for bid_file_upload to filter on
  select
    partner_property_id
    , country_code
    , campaign_type
    , date_added
  from (
    select
      cast(a.wego_hotel_id as string) as partner_property_id
      , b.country_code
      , a.market as campaign_type
      , date '2099-01-01' as date_added
      , row_number() over (partition by a.wego_hotel_id order by a.market) as rn
    from `wego-cloud.analysis.skyscanner_top_hotel_list_monthly` a
    join `wego-cloud.analysis.hotels_detail_list` b
      on a.wego_hotel_id = cast(b.hotel_id as int64)
    left join current_list c
      on cast(a.wego_hotel_id as string) = c.partner_property_id
    where c.partner_property_id is null
  )
  where rn = 1
)

-- final output combines:
-- 1. newly ranked hotels to add
-- 2. previously listed hotels to reintroduce
-- 3. monthly top hotels from Google Sheet (NEW 2026-07-08)
select
  partner_property_id
  , country_code
  , campaign_type
  , date_added
from ranking
union all
select
  partner_property_id
  , country_code
  , campaign_type
  , date_added
from reintro_hotels
union all
select
  partner_property_id
  , country_code
  , campaign_type
  , date_added
from monthly_top_hotels
{% endraw %}
