{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_pricing_engine_distribution
-- Destination: pricing_engine.hotels_pricing_engine_distribution  (unchanged)
-- Schedule   : every day 01:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('hotels_pricing_engine_distribution_calculation') }}
{% raw %}
-- Pass-1 + Pass-2 combined: hotel_id wildcarding then user_country_code wildcarding
-- Staging table: analysis_staging.jf_dist_pricing_usercountry_wildcard_test
-- Created: 2026-07-17
--
-- Goal: reduce cardinality of hotels_pricing_engine_distribution_calculation (16.5M rows/day)
-- so the downstream auto-pricing pipeline doesn't OOM consuming the feed.
--
-- Pass-1 (validated by Julia): collapse hotel_id to NULL when city-group has a dominant
--         markup (count > 5). 14.2M -> 5.88M rows.
-- Pass-2: within NULL-hotel groups only, collapse user_country_code to NULL
--         for the dominant markup across user_countries; keep outlier UCs explicit.
--         Adds another ~160K row reduction (2.7% of pass-1 output).
-- Specific hotel_id outlier rows from pass-1 pass through unchanged (no UC wildcarding)
-- to avoid L2-shadow drift, where a hotel-level NULL-UC row would incorrectly override
-- the city-level fallback for that hotel's wildcarded user_countries.
--
-- Result (staging, current_date snapshot): 14.2M raw -> 5.72M final (59.7% reduction).
-- Sanity checks: 0% markup drift across all partners/resolution levels, zero duplicate
-- lookup keys. Tie-breaks on equal group counts use markup_percentage desc for determinism.
--
-- NOT YET DEPLOYED. Before pushing to prod, confirm with engineering that the serve-time
-- lookup does most-specific-match-first on user_country_code (specific row beats NULL
-- fallback) -- this pattern is proven live for Vio (12 rows) but untested at GH's ~16M-row
-- scale until this pass-2 change ships.

with top_markup as
(
  select * from
  (
    select
      ts_code
      , hotel_city_code
      , hotel_country_code
      , user_country_code
      , device_type
      , round(proposed_markup / 0.005) * 0.005 as markup_percentage
      , count(*) as _ct
    from `wego-cloud.pricing_engine.hotels_pricing_engine_distribution_calculation`
    where date(created_at) = current_date()
    and proposed_markup is not null
    and hotel_id is not null
    group by 1,2,3,4,5,6
  )
  where _ct > 5
  qualify row_number() over(partition by ts_code,hotel_city_code,hotel_country_code,user_country_code,device_type order by _ct desc, markup_percentage desc) = 1
)

, raw as
(
  select
    ts_code
    , hotel_id
    , hotel_city_code
    , hotel_country_code
    , user_country_code
    , device_type
    , round(proposed_markup / 0.005) * 0.005 as markup_percentage
    , 0 as nightly_rate
    , created_at
  from `wego-cloud.pricing_engine.hotels_pricing_engine_distribution_calculation`
  where date(created_at) = current_date()
  and proposed_markup is not null
  and hotel_id is not null
)

-- pass-1: collapse hotel_id to NULL when city-group has a dominant markup
, pass1 as
(
  select distinct
    a.ts_code
    , case when b.markup_percentage is not null then null else a.hotel_id end as hotel_id
    , a.hotel_city_code
    , a.hotel_country_code
    , a.user_country_code
    , a.device_type
    , coalesce(b.markup_percentage, a.markup_percentage) as markup_percentage
    , 0 as nightly_rate
    , a.created_at
  from raw a
  left join top_markup b
  using(
    ts_code
      , hotel_city_code
      , hotel_country_code
      , user_country_code
      , device_type
  )
)

-- split pass-1 output
, pass1_null_hotel as
(
  select * from pass1 where hotel_id is null
)

, pass1_specific_hotel as
(
  select * from pass1 where hotel_id is not null
)

-- pass-2: dominant markup across user_countries within NULL-hotel groups only
, pass2_dominant as
(
  select
    ts_code
    , hotel_city_code
    , hotel_country_code
    , device_type
    , markup_percentage as dominant_markup
  from
  (
    select
      ts_code
      , hotel_city_code
      , hotel_country_code
      , device_type
      , markup_percentage
      , count(distinct user_country_code) as _ct
    from pass1_null_hotel
    group by 1,2,3,4,5
  )
  qualify row_number() over(partition by ts_code, hotel_city_code, hotel_country_code, device_type order by _ct desc, markup_percentage desc) = 1
)

-- pass-2 output
, pass2 as
(
  -- NULL hotel + dominant UC -> NULL both
  select distinct
    a.ts_code
    , cast(null as string) as hotel_id
    , a.hotel_city_code
    , a.hotel_country_code
    , cast(null as string) as user_country_code
    , a.device_type
    , b.dominant_markup as markup_percentage
    , 0 as nightly_rate
    , a.created_at
  from pass1_null_hotel a
  inner join pass2_dominant b
  on a.ts_code = b.ts_code
  and a.hotel_city_code = b.hotel_city_code
  and a.hotel_country_code = b.hotel_country_code
  and a.device_type = b.device_type
  where a.markup_percentage = b.dominant_markup

  union all

  -- NULL hotel + outlier UC -> keep specific user_country
  select distinct
    a.ts_code
    , cast(null as string) as hotel_id
    , a.hotel_city_code
    , a.hotel_country_code
    , a.user_country_code
    , a.device_type
    , a.markup_percentage
    , 0 as nightly_rate
    , a.created_at
  from pass1_null_hotel a
  inner join pass2_dominant b
  on a.ts_code = b.ts_code
  and a.hotel_city_code = b.hotel_city_code
  and a.hotel_country_code = b.hotel_country_code
  and a.device_type = b.device_type
  where a.markup_percentage != b.dominant_markup

  union all

  -- specific hotel_id rows pass through unchanged
  select
    ts_code
    , hotel_id
    , hotel_city_code
    , hotel_country_code
    , user_country_code
    , device_type
    , markup_percentage
    , nightly_rate
    , created_at
  from pass1_specific_hotel
)

select * from pass2
{% endraw %}
