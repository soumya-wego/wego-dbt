{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : skyscanner_bid_file_upload
-- Destination: wego_analytics.skyscanner_bid_file_upload  (unchanged)
-- Schedule   : every day 09:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- builds the final bid matrix for skyscanner hotel campaigns
-- logic flow:
-- 1. define campaign suffix universe
-- 2. create active campaign list from ML output + AU fallback campaigns
-- 3. calculate booking performance and ROI
-- 4. calculate historical bid cost in USD
-- 5. identify expensive low ROI hotels for exclusion
-- 6. create hotel x campaign combinations from bid hotel list
-- 7. merge ML bids with manually introduced property lists
-- 8. deduplicate at hotel x campaign level
-- 9. pivot into final campaign columns
--
-- CHANGE 2026-04-08: GCC markets (AE, SA, QA, BH, OM, KW) re-enabled on domestic campaigns only
-- All three bid sources (bid_list, bid_list_new, bid_list_en) now include GCC hotels
-- bid_list bids CTE updated with domestic-only gate: GCC hotels bid 0 on non-domestic campaigns
-- EG and CN remain fully excluded
--
-- CHANGE 2026-04-14: EG re-enabled on domestic campaigns only (same pattern as other GCC markets)
-- Removed EG from country_code exclusion in all three bid sources
-- Added EG to domestic-only gate and campaign hard-zero guard in bids CTE
-- Added EG-APP/DESKTOP/MWEB (+ GROUP) columns to PIVOT
-- CN remains fully excluded
--
-- CHANGE 2026-05-06: Makkah properties (city_code=8283) zeroed out on all campaigns due to Hajj closure
-- Top 5000 international properties by click volume (4931 hotels) inserted into hotel list
-- with date_added=2026-05-06; flows through bid_list naturally via ML bids (Option B)
--
-- CHANGE 2026-07-08: monthly top hotel list (Google Sheet external table) added as 4th bid source
-- Hotels from skyscanner_top_hotel_list_monthly inserted with sentinel date_added = 2099-01-01
-- bid_list excludes date_added = 2099-01-01; bid_list_monthly handles them with 70/0/1 pattern
-- Existing hotels already in bid_hotel_list are untouched (sentinel only on genuinely new hotels)
--
-- CHANGE 2026-07-10: EG campaigns fully disabled (non-GROUP zeroed)
-- All EG-APP/EG-DESKTOP/EG-MWEB bids set to 0 across all 4 bid sources
-- EG GROUP campaigns remain at 1 (structural requirement)
-- EG hotels remain in the hotel pool but bid 0 on all EG campaigns

with suffixes as (
    -- static list of campaign suffixes used to generate AU campaign names
    select 'APP' as suffix
    union all select 'APP-GROUP'
    union all select 'DESKTOP'
    union all select 'DESKTOP-GROUP'
    union all select 'MWEB'
    union all select 'MWEB-GROUP'
)

, campaign_list as
(
  -- creates the campaign universe to be used downstream
  -- includes:
  -- 1. recent campaign names from ML tables
  -- 2. synthetic AU campaigns generated from suffixes
  select distinct campaign_name
  from `wego-cloud.skyscanner_bidding.v2_union_group*` a -- ML Model
  where _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', current_date - 3)
  and campaign_name not like 'JO%'

  union distinct

  select concat('AU-', suffix) as campaign_name
  from suffixes

  union distinct

  -- EG campaigns not in ML model; generate synthetically like AU
  select concat('EG-', suffix) as campaign_name
  from suffixes
)

, hotels_booking as
(
  -- aggregates hotel booking performance over the last 30 days
  -- used to identify hotels with proven booking value
  -- only keeps hotels with roi >= 0.5
  select
    cast(hotel_id as string) as partner_property_id
    , sum(total_cost_of_sales_usd) as total_cost
    , sum(finance_revenue_usd+total_cost_of_sales_usd) as net_exc_dist_cost
    , safe_divide(sum(finance_revenue_usd+total_cost_of_sales_usd),sum(total_cost_of_sales_usd)) as roi
  from `wego_analytics.hotels_bookings`
  where date(created_at) >= current_date - interval '30' day
  and conversions_adjusted > 0
  and attribution_ts_code = '6be92'
  group by 1
  having roi >= 0.5
)

, xrates as
(
  -- gets GBP to USD daily exchange rates for the last 30 days
  -- used to convert skyscanner bid cost into USD
  select
    date(effective) as rate_date
    , amount
  from `analytics.exchange_rates*`
  where _table_suffix >= FORMAT_DATE('%Y%m%d', current_date-interval '30' day)
  and base = 'GBP'
  and quote = 'USD'
)

, total_cost as
(
  -- aggregates historical skyscanner bid cost over the last 60 days
  select
    partner_property_id
    , sum(coalesce(cost_gbp_pence,0)*amount/100) as bid_cost_usd
    , sum(coalesce(cost_gbp_pence,0)*amount/100)/count(distinct date) as daily_bid_cost_usd
    , count(distinct date) as total_live_date
    , date_diff(current_date,max(date),day) as date_since_last_live
  from `wego-cloud.distribution_partner_reports_hotels.skyscanner_auction_insights_report` a
  left join xrates
    on date = rate_date
  where date >= current_date - interval '60' day
  group by 1
)

, profitability as
(
  select
    *
    , percent_rank()over(order by roi) as roi_percentile
    , ntile(100)over(order by bid_cost_usd desc) as cost_percentile
  from total_cost a
  left join hotels_booking b
    using(partner_property_id)
  where coalesce(bid_cost_usd,0) > 0
  and total_live_date >= 14
)

, exclusion as
(
  -- excludes high cost poor performers
  select distinct
    a.*
  from profitability a
  where cost_percentile <= 10
  and coalesce(roi,0) <= 0
  and date_since_last_live <= 30
)

, makkah_hotels as
(
  -- Makkah hotels zeroed on all campaigns due to Hajj closure (2026-05-06)
  select distinct hotel_id as partner_property_id
  from `wego-cloud.analysis.hotels_detail_list`
  where city_code = '8283'
)

, bid_list as
(
  -- creates the base hotel x campaign list from the current bid hotel table
  -- CHANGE 2026-04-08: GCC markets re-enabled; EG and CN remain excluded
  -- CHANGE 2026-04-14: EG re-enabled; CN remains excluded
  -- CHANGE 2026-05-06: Top 5000 batch (date_added=2026-05-06) flows through here via ML bids
  -- CHANGE 2026-07-08: monthly top hotels excluded via sentinel date (handled by bid_list_monthly)
  select distinct
    partner_property_id
    , campaign_name
    , country_code
  from `analysis.skyscanner_bid_hotel_list`
  cross join campaign_list
  where date_added >= '2025-12-01'
  and country_code not in ('CN')
  and date_added <> '2026-03-13'
  and date_added <> '2026-03-19'
  and date_added <> '2099-01-01'
)

, bid_list_new as --- new list from hotel mapping google hotel
(
  -- creates a special property list from hotels added on 2026-03-13
  -- CHANGE 2026-04-08: GCC markets re-enabled
  -- CHANGE 2026-04-14: EG re-enabled
  select distinct
    partner_property_id
    , campaign_name
    , country_code
    , case when campaign_name like '%GROUP%' then 1
        when campaign_type = split(campaign_name, '-')[safe_offset(0)] then 70
        else 0 end as bid
  from `analysis.skyscanner_bid_hotel_list`
  cross join campaign_list
  where date_added = '2026-03-13'
  and country_code not in ('CN')
)

, bid_list_en as -- adding US, CA, UK list from another distribution partner that has booking
(
  -- creates another special property list from hotels added on 2026-03-19
  -- CHANGE 2026-04-08: GCC markets re-enabled
  -- CHANGE 2026-04-14: EG re-enabled
  select distinct
    partner_property_id
    , campaign_name
    , country_code
    , case when campaign_name like '%GROUP%' then 1
        when campaign_type = split(campaign_name, '-')[safe_offset(0)] then 70
        else 0 end as bid
  from `analysis.skyscanner_bid_hotel_list`
  cross join campaign_list
  where date_added = '2026-03-19'
  and country_code not in ('CN')
)

, bid_list_monthly as
(
  -- ADDED 2026-07-08: monthly top hotel list (sentinel date_added = 2099-01-01)
  -- bids on target market campaign (70/0/1 pattern matching campaign_type logic)
  -- reads from bid_hotel_list (not the sheet) for consistency with other bid_list CTEs
  select distinct
    partner_property_id
    , campaign_name
    , country_code
    , case
        when campaign_name like '%GROUP%' then 1
        when campaign_type = split(campaign_name, '-')[safe_offset(0)] then 70
        else 0
      end as bid
  from `analysis.skyscanner_bid_hotel_list`
  cross join campaign_list
  where date_added = '2099-01-01'
  and country_code not in ('CN')
)

, ml_bid as -- baseline
(
  -- pulls the current ML generated bid table for today
  select distinct
    partner_property_id
    , campaign_name
    , bid
  from `wego-cloud.skyscanner_bidding.v2_union_group*` a --V2
  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', current_date)
  and campaign_name not like 'JO%'
)

, bids as
(
  -- combines four bid sources into one unified bid table
  -- CHANGE 2026-04-08 (bid_list branch):
  --   GCC hotels (AE/SA/QA/BH/OM/KW) now included but restricted to domestic campaigns only
  --   domestic = split(campaign_name,'-')[0] matches hotel's country_code
  --   non-domestic campaigns for GCC hotels → 0
  --   GCC campaign hard-zero now only applies to non-GCC hotels to avoid blocking domestic GCC bids
  -- CHANGE 2026-04-14: EG added to domestic-only gate and campaign hard-zero guard
  -- CHANGE 2026-05-06: Makkah hotels (city_code=8283) → bid 0 on all campaigns (Hajj closure)
  -- CHANGE 2026-07-08: bid_list_monthly added as 4th source (monthly top hotels, campaign_type bidding)
  -- CHANGE 2026-07-10: EG campaigns fully disabled (non-GROUP zeroed across all sources)

  select distinct
    a.partner_property_id
    , campaign_name
    , case
      -- GROUP always 1 (structural Skyscanner requirement, overrides all other rules)
      when a.campaign_name like '%GROUP%' then coalesce(b.bid,1)
      -- Makkah closure: zero all non-GROUP bids (Hajj closure)
      -- when m.partner_property_id is not null then 0
      -- EG campaigns fully disabled (CHANGE 2026-07-10)
      when a.campaign_name like 'EG%' then 0
      -- domestic-only gate for re-enabled GCC markets
      when a.country_code in ('AE','SA','QA','BH','OM','KW','EG')
        and split(a.campaign_name,'-')[safe_offset(0)] <> a.country_code then 0
      -- GCC campaign hard-zero for non-GCC hotels only
      when a.country_code not in ('AE','SA','QA','BH','OM','KW','EG')
        and (a.campaign_name like 'AE%'
          or a.campaign_name like 'BH%'
          or a.campaign_name like 'EG%'
          or a.campaign_name like 'KW%'
          or a.campaign_name like 'OM%'
          or a.campaign_name like 'QA%'
          or a.campaign_name like 'SA%') then 0
      when c.partner_property_id is not null and a.campaign_name like 'AU%' then 90
      when c.partner_property_id is not null then 100
      when a.campaign_name like 'AU%' then least(coalesce(b.bid,90),90)
      else least(coalesce(b.bid,100),100) end as bid
    , 'ml' as source
  from bid_list a
  left join ml_bid b
    using(partner_property_id,campaign_name)
  left join hotels_booking c
    on a.partner_property_id = c.partner_property_id
  left join exclusion d
    on a.partner_property_id = d.partner_property_id
  -- left join makkah_hotels m
  --   on a.partner_property_id = m.partner_property_id
  where d.partner_property_id is null

  union all

  select
    a.partner_property_id
    , campaign_name
    , case
      when campaign_name like '%GROUP%' then 1
      -- Makkah closure: zero all non-GROUP bids (Hajj closure)
      -- when m.partner_property_id is not null then 0
      -- EG campaigns fully disabled (CHANGE 2026-07-10)
      when a.campaign_name like 'EG%' then 0
      when a.country_code in ('AE','SA','QA','BH','OM','KW','EG')
        and split(a.campaign_name,'-')[safe_offset(0)] <> a.country_code then 0
      when campaign_name like 'AU%' then least(coalesce(b.bid,a.bid),90)
      else least(coalesce(b.bid,a.bid),100) end as bid
    , 'skyscanner' as source
  from bid_list_new a
  left join ml_bid b
    using(partner_property_id,campaign_name)
  left join exclusion d
    on a.partner_property_id = d.partner_property_id
  -- left join makkah_hotels m
  --   on a.partner_property_id = m.partner_property_id
  where d.partner_property_id is null

  union all

  select
    a.partner_property_id
    , campaign_name
    , case
      when campaign_name like '%GROUP%' then 1
      -- Makkah closure: zero all non-GROUP bids (Hajj closure)
      -- when m.partner_property_id is not null then 0
      -- EG campaigns fully disabled (CHANGE 2026-07-10)
      when a.campaign_name like 'EG%' then 0
      when a.country_code in ('AE','SA','QA','BH','OM','KW','EG')
        and split(a.campaign_name,'-')[safe_offset(0)] <> a.country_code then 0
      when campaign_name like 'AU%' then least(coalesce(b.bid,a.bid),90)
      else least(coalesce(b.bid,a.bid),100) end as bid
    , 'other distribution' as source
  from bid_list_en a
  left join ml_bid b
    using(partner_property_id,campaign_name)
  left join exclusion d
    on a.partner_property_id = d.partner_property_id
  -- left join makkah_hotels m
  --   on a.partner_property_id = m.partner_property_id
  where d.partner_property_id is null

  union all

  -- ADDED 2026-07-08: monthly top hotel list (campaign_type-based bidding)
  select
    a.partner_property_id
    , campaign_name
    , case
      when campaign_name like '%GROUP%' then 1
      -- Makkah closure: zero all non-GROUP bids (Hajj closure)
      -- when m.partner_property_id is not null then 0
      -- EG campaigns fully disabled (CHANGE 2026-07-10)
      when a.campaign_name like 'EG%' then 0
      -- domestic-only gate for GCC-located hotels in the monthly list
      when a.country_code in ('AE','SA','QA','BH','OM','KW','EG')
        and split(a.campaign_name,'-')[safe_offset(0)] <> a.country_code then 0
      when campaign_name like 'AU%' then least(coalesce(b.bid,a.bid),90)
      else least(coalesce(b.bid,a.bid),100) end as bid
    , 'monthly_top' as source
  from bid_list_monthly a
  left join ml_bid b
    using(partner_property_id,campaign_name)
  left join exclusion d
    on a.partner_property_id = d.partner_property_id
  -- left join makkah_hotels m
  --   on a.partner_property_id = m.partner_property_id
  where d.partner_property_id is null
)

, production_final as (
  SELECT *
  FROM (
    select
      partner_property_id as `Hotel ID`
      , campaign_name
      , bid
    from bids
    qualify row_number()over(partition by partner_property_id, campaign_name order by bid desc) = 1
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
      , 'EG-APP'
      , 'EG-APP-GROUP'
      , 'EG-DESKTOP'
      , 'EG-DESKTOP-GROUP'
      , 'EG-MWEB'
      , 'EG-MWEB-GROUP'
      , 'UK-APP'
      , 'UK-APP-GROUP'
      , 'UK-DESKTOP'
      , 'UK-DESKTOP-GROUP'
      , 'UK-MWEB'
      , 'UK-MWEB-GROUP'
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
      , 'AU-APP'
      , 'AU-APP-GROUP'
      , 'AU-DESKTOP'
      , 'AU-DESKTOP-GROUP'
      , 'AU-MWEB'
      , 'AU-MWEB-GROUP'
  ))
)

-- Single-statement 100% EB rollout (from the validated 80/20 split, 2026-09-15 to 09-30).
-- Reads the fixed-name eb_latest shadow table, written by baikal's publish-shadow task
-- (PR #1803) after every EB DAG run (03:00 UTC daily), so this query always sees the
-- freshest shadow bids with no per-day table-name maintenance.
-- Freshness guard: if eb_latest is older than 36 hours (EB DAG missed a run), ALL hotels
-- fall back to production bids rather than serving stale EB bids -- unchanged from the
-- split version, now the only safety net (there is no control arm left to compare against).
-- ============================================================
-- REVERT TOGGLE: to restore the pre-shadow output, delete or comment out everything
-- from 'eb_latest as (' below through the final split-based SELECT, and uncomment the
-- single line immediately below instead.
-- select * from production_final
-- ============================================================
, eb_latest as (
  select * from `wego-cloud.analysis.skyscanner_bid_file_upload_eb_latest`
)
, eb_fresh as (
  select timestamp_diff(current_timestamp(), timestamp_millis(last_modified_time), hour) <= 36 as fresh
  from `wego-cloud.analysis.__TABLES__`
  where table_id = 'skyscanner_bid_file_upload_eb_latest'
)
, split as (
  select
    p.`Hotel ID`
    , e.`Hotel ID` is not null
      and coalesce((select fresh from eb_fresh), false) as treatment
    , p.* except (`Hotel ID`)
    , e.`US-APP` as eb_us_app, e.`US-DESKTOP` as eb_us_desktop, e.`US-MWEB` as eb_us_mweb
    , e.`UK-APP` as eb_uk_app, e.`UK-DESKTOP` as eb_uk_desktop, e.`UK-MWEB` as eb_uk_mweb
    , e.`CA-APP` as eb_ca_app, e.`CA-DESKTOP` as eb_ca_desktop, e.`CA-MWEB` as eb_ca_mweb
    , e.`AU-APP` as eb_au_app, e.`AU-DESKTOP` as eb_au_desktop, e.`AU-MWEB` as eb_au_mweb
  from production_final p
  left join eb_latest e on e.`Hotel ID` = p.`Hotel ID`
)
select
  `Hotel ID`
  , `AE-APP`, `AE-APP-GROUP`, `AE-DESKTOP`, `AE-DESKTOP-GROUP`, `AE-MWEB`, `AE-MWEB-GROUP`
  , `BH-APP`, `BH-APP-GROUP`, `BH-DESKTOP`, `BH-DESKTOP-GROUP`, `BH-MWEB`, `BH-MWEB-GROUP`
  , if(treatment, eb_ca_app, `CA-APP`) as `CA-APP`, `CA-APP-GROUP`
  , if(treatment, eb_ca_desktop, `CA-DESKTOP`) as `CA-DESKTOP`, `CA-DESKTOP-GROUP`
  , if(treatment, eb_ca_mweb, `CA-MWEB`) as `CA-MWEB`, `CA-MWEB-GROUP`
  , `EG-APP`, `EG-APP-GROUP`, `EG-DESKTOP`, `EG-DESKTOP-GROUP`, `EG-MWEB`, `EG-MWEB-GROUP`
  , if(treatment, eb_uk_app, `UK-APP`) as `UK-APP`, `UK-APP-GROUP`
  , if(treatment, eb_uk_desktop, `UK-DESKTOP`) as `UK-DESKTOP`, `UK-DESKTOP-GROUP`
  , if(treatment, eb_uk_mweb, `UK-MWEB`) as `UK-MWEB`, `UK-MWEB-GROUP`
  , `KW-APP`, `KW-APP-GROUP`, `KW-DESKTOP`, `KW-DESKTOP-GROUP`, `KW-MWEB`, `KW-MWEB-GROUP`
  , `OM-APP`, `OM-APP-GROUP`, `OM-DESKTOP`, `OM-DESKTOP-GROUP`, `OM-MWEB`, `OM-MWEB-GROUP`
  , `QA-APP`, `QA-APP-GROUP`, `QA-DESKTOP`, `QA-DESKTOP-GROUP`, `QA-MWEB`, `QA-MWEB-GROUP`
  , `SA-APP`, `SA-APP-GROUP`, `SA-DESKTOP`, `SA-DESKTOP-GROUP`, `SA-MWEB`, `SA-MWEB-GROUP`
  , if(treatment, eb_us_app, `US-APP`) as `US-APP`, `US-APP-GROUP`
  , if(treatment, eb_us_desktop, `US-DESKTOP`) as `US-DESKTOP`, `US-DESKTOP-GROUP`
  , if(treatment, eb_us_mweb, `US-MWEB`) as `US-MWEB`, `US-MWEB-GROUP`
  , if(treatment, eb_au_app, `AU-APP`) as `AU-APP`, `AU-APP-GROUP`
  , if(treatment, eb_au_desktop, `AU-DESKTOP`) as `AU-DESKTOP`, `AU-DESKTOP-GROUP`
  , if(treatment, eb_au_mweb, `AU-MWEB`) as `AU-MWEB`, `AU-MWEB-GROUP`
from split
{% endraw %}
