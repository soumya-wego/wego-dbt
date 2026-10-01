{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : client_lifetime_kpi_delsert
-- Destination: analysis.client_lifetime_kpis  (unchanged)
-- Schedule   : every day 01:38   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('clients_sessions_aggregated_daily') }}
-- depends_on: {{ source('external_appsflyer', 'uninstalls') }}
-- depends_on: {{ source('wego_analytics', 'clients') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookables') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookables') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
{% raw %}
declare settle_date date default date_sub(current_date(), interval 8 day);

-- =============================================================================
-- THE WATERMARK -- why clk carries `settled_through`
-- =============================================================================
-- an ADDITIVE merge is not idempotent: `total = total + delta` applied twice
-- counts the day twice, silently. the transfer service retries failed runs, and
-- a manual re-run is one click, so this WILL happen eventually.
--
-- `settled_through` records the last day folded into a client's totals. the
-- additive branch fires only when settled_through < settle_date, so replaying a
-- day is a no-op. it also makes clk self-describing: you can see how current
-- any row is without inferring it.
-- =============================================================================

-- =============================================================================
-- MERGE 1 of 2 -- the settled day's activity
-- =============================================================================

merge `wego-cloud.analysis.client_lifetime_kpis` t
using (

  -- ------------------------------------------- counts, straight off base --
  with base_delta as (
    select
      client_id
    , min(created_at) as day_first_created_at
    , max(created_at) as day_last_created_at
    , count(distinct session_id) as day_total_sessions
    , sum(flights_searches) as flights_searches
    , sum(hotels_searches) as hotels_searches
    , sum(total_searches) as total_searches
    , sum(total_clicks) as total_clicks
    , sum(total_bookings) as total_bookings
    , sum(flights_clicks) as flights_clicks
    , sum(total_flights_bookings) as total_flights_bookings
    , sum(meta_flights_bookings) as meta_flights_bookings
    , sum(BOW_flights_bookings) as BOW_flights_bookings
    , sum(hotels_clicks) as hotels_clicks
    , sum(total_hotels_bookings) as total_hotels_bookings
    , sum(meta_hotels_bookings) as meta_hotels_bookings
    , sum(BOW_hotels_bookings) as BOW_hotels_bookings
    , count(distinct if(total_bookings > 0, session_id, null)) as booking_sessions
    , count(distinct if(coalesce(BOW_flights_bookings,0) + coalesce(BOW_hotels_bookings,0) > 0, session_id, null)) as bow_booking_sessions
    , count(distinct if(coalesce(BOW_flights_bookings,0) > 0, session_id, null)) as bow_flights_booking_sessions
    , count(distinct if(coalesce(BOW_hotels_bookings,0) > 0, session_id, null)) as bow_hotels_booking_sessions
    from `wego-cloud.analysis.clients_sessions_aggregated_daily`
    where created_at_date = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  -- first-touch session attributes, used only when the client is NEW
  , session_anchors as (
    select
      client_id
    , device_type as first_device_type
    , os_type as first_os_type
    , app_version as first_app_version
    , user_country_code as first_user_country_code
    , user_city as first_user_city
    , referrer_url as first_referrer_url
    , site_code as first_site_code
    , locale as first_locale
    , channel as first_channel
    , wg_source as first_wg_source
    , wg_medium as first_wg_medium
    , wg_campaign as first_wg_campaign
    , wg_adgroup as first_wg_adgroup
    , wg_content as first_wg_content
    , ts_code as first_ts_code
    , source as first_source
    , market as first_market
    , app_rt_source as first_app_rt_source
    , app_rt_medium as first_app_rt_medium
    , app_rt_campaign as first_app_rt_campaign
    , app_rt_adgroup as first_app_rt_adgroup
    , app_rt_content as first_app_rt_content
    from `wego-cloud.analysis.clients_sessions_aggregated_daily`
    where created_at_date = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    qualify row_number() over (partition by client_id order by created_at asc, session_id asc) = 1
  )

  -- device / ua anchors are NOT in base -- they come from wego_analytics.clients
  , client_anchors as (
    select
      client_id
    , device_brand as first_device_brand
    , device_model as first_device_model
    , marketing_name as first_marketing_name
    , device_version as first_device_version
    , ua_source as first_ua_source
    , ua_medium as first_ua_medium
    , ua_campaign as first_ua_campaign_raw
    , ua_adgroup as first_ua_adgroup
    , ua_content as first_ua_content
    , appsflyer_id as first_appsflyer_id
    , advertiser_id as first_advertiser_id
    from `wego-cloud.wego_analytics.clients`
    where date(_partitiontime) = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    qualify row_number() over (partition by client_id order by created_at asc) = 1
  )

  -- ------------------------------- revenue, from raw (base carries none) --

  , flights_clicks_leg as (
    select
      client_id
    , "flights" as product
    , date(min(created_at)) as first_click_date
    , date(min(if(if(provider_code != "wego.com", conversions_tracked, 0) > 0, created_at, null))) as first_booking_date
    , cast(null as date) as BOW_first_booking_date
    , sum(if(provider_code != "wego.com", finance_revenue_usd, 0)) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
    , 0 as cau_revenue
    from `wego-cloud.wego_analytics.flights_clicks`
    where date(_partitiontime) = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , flights_bookables_leg as (
    select
      client_id
    , "flights" as product
    , date(min(created_at)) as first_click_date
    , cast(null as date) as first_booking_date
    , cast(null as date) as BOW_first_booking_date
    , 0 as finance_revenue_usd
    , 0 as booking_revenue_usd
    , sum(finance_revenue_usd) as cau_revenue
    from `wego-cloud.wego_analytics.flights_bookables`
    where date(_partitiontime) = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , hotels_clicks_leg as (
    select
      client_id
    , "hotels" as product
    , date(min(created_at)) as first_click_date
    , date(min(if(if(provider_code != "hotels.wego.com", conversions_tracked, 0) > 0, created_at, null))) as first_booking_date
    , cast(null as date) as BOW_first_booking_date
    , sum(if(provider_code != "hotels.wego.com", finance_revenue_usd, 0)) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
    , 0 as cau_revenue
    from `wego-cloud.wego_analytics.hotels_clicks`
    where date(_partitiontime) = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , hotels_bookables_leg as (
    select
      client_id
    , "hotels" as product
    , date(min(created_at)) as first_click_date
    , cast(null as date) as first_booking_date
    , cast(null as date) as BOW_first_booking_date
    , 0 as finance_revenue_usd
    , 0 as booking_revenue_usd
    , sum(finance_revenue_usd) as cau_revenue
    from `wego-cloud.wego_analytics.hotels_bookables`
    where date(_partitiontime) = settle_date
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , flights_insurance_lookup as (
    select
      booking_id
    , finance_revenue_usd as insurance_revenue
    from `wego-cloud.wego_analytics.flights_insurance`
  )

  , bow_flights_leg as (
    select
      b.client_id
    , "BOW flights" as product
    , cast(null as date) as first_click_date
    , cast(null as date) as first_booking_date
    , date(min(b.created_at)) as BOW_first_booking_date
    , sum(b.finance_revenue_usd) + sum(ifnull(i.insurance_revenue, 0)) + sum(ifnull(b.distribution_cost_usd, 0)) as finance_revenue_usd
    , sum(b.finance_revenue_usd) + sum(ifnull(i.insurance_revenue, 0)) + sum(ifnull(b.distribution_cost_usd, 0)) as booking_revenue_usd
    , 0 as cau_revenue
    from `wego-cloud.wego_analytics.flights_bookings` b
    left join flights_insurance_lookup i
      on b.booking_id = i.booking_id
    where date(b._partitiontime) = settle_date
      and b.conversions_adjusted > 0
      and b.client_id is not null
      and b.client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , bow_hotels_leg as (
    select
      client_id
    , "BOW hotels" as product
    , cast(null as date) as first_click_date
    , cast(null as date) as first_booking_date
    , date(min(created_at)) as BOW_first_booking_date
    , sum(finance_revenue_usd) + sum(ifnull(distribution_cost_usd, 0)) as finance_revenue_usd
    , sum(finance_revenue_usd) + sum(ifnull(distribution_cost_usd, 0)) as booking_revenue_usd
    , 0 as cau_revenue
    from `wego-cloud.wego_analytics.hotels_bookings`
    where date(created_at) = settle_date
      and conversions_adjusted > 0
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1
  )

  , revenue_union as (
    select * from flights_clicks_leg
    union all
    select * from flights_bookables_leg
    union all
    select * from hotels_clicks_leg
    union all
    select * from hotels_bookables_leg
    union all
    select * from bow_flights_leg
    union all
    select * from bow_hotels_leg
  )

  , revenue_delta as (
    select
      client_id
    , min(first_click_date) as first_click_date
    , min(first_booking_date) as meta_first_booking_date
    , min(BOW_first_booking_date) as BOW_first_booking_date
    , min(if(product = "flights", first_click_date, null)) as flights_first_click_date
    , min(if(product = "flights", first_booking_date, null)) as meta_flights_first_booking_date
    , min(if(product = "BOW flights", BOW_first_booking_date, null)) as BOW_flights_first_booking_date
    , min(if(product = "hotels", first_click_date, null)) as hotels_first_click_date
    , min(if(product = "hotels", first_booking_date, null)) as meta_hotels_first_booking_date
    , min(if(product = "BOW hotels", BOW_first_booking_date, null)) as BOW_hotels_first_booking_date
    , sum(finance_revenue_usd + cau_revenue) as total_finance_revenue_usd
    , sum(cau_revenue) as total_cau_finance_revenue_usd
    , sum(if(product = "flights", cau_revenue, 0)) as flights_cau_finance_revenue_usd
    , sum(if(product = "hotels", cau_revenue, 0)) as hotels_cau_finance_revenue_usd
    , sum(if(product = "flights", finance_revenue_usd, 0)) + sum(if(product = "BOW flights", finance_revenue_usd, 0)) + sum(if(product = "flights", cau_revenue, 0)) as flights_finance_revenue_usd
    , sum(if(product = "flights", finance_revenue_usd, 0)) as meta_flights_finance_revenue_usd
    , sum(if(product = "BOW flights", finance_revenue_usd, 0)) as BOW_flights_finance_revenue_usd
    , sum(if(product = "hotels", finance_revenue_usd, 0)) + sum(if(product = "BOW hotels", finance_revenue_usd, 0)) + sum(if(product = "hotels", cau_revenue, 0)) as hotels_finance_revenue_usd
    , sum(if(product = "hotels", finance_revenue_usd, 0)) as meta_hotels_finance_revenue_usd
    , sum(if(product = "BOW hotels", finance_revenue_usd, 0)) as BOW_hotels_finance_revenue_usd
    , sum(if(product = "flights", booking_revenue_usd, 0)) + sum(if(product = "BOW flights", booking_revenue_usd, 0)) as flights_booking_revenue_usd
    , sum(if(product = "flights", booking_revenue_usd, 0)) as meta_flights_booking_revenue_usd
    , sum(if(product = "BOW flights", booking_revenue_usd, 0)) as BOW_flights_booking_revenue_usd
    , sum(if(product = "hotels", booking_revenue_usd, 0)) + sum(if(product = "BOW hotels", booking_revenue_usd, 0)) as hotels_booking_revenue_usd
    , sum(if(product = "hotels", booking_revenue_usd, 0)) as meta_hotels_booking_revenue_usd
    , sum(if(product = "BOW hotels", booking_revenue_usd, 0)) as BOW_hotels_booking_revenue_usd
    from revenue_union
    group by 1
  )

  -- product of the earliest booking on the settled day, fills a null only
  , first_product_delta as (
    select
      client_id
    , product
    from revenue_union
    where coalesce(first_booking_date, BOW_first_booking_date) is not null
    qualify row_number() over (
      partition by client_id
      order by coalesce(first_booking_date, BOW_first_booking_date) asc
    ) = 1
  )

  -- ---------------------------------------------------- payment anchors --

  , payment_events as (
    select client_id, created_at, payment_method, payment_gateway, payment_fee_usd
    from `wego-cloud.wego_analytics.flights_bookings`
    where date(_partitiontime) = settle_date
      and conversions_adjusted > 0
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    union all
    -- hotels_bookings has no payment_fee_usd column; explicit zero, matching
    -- prod and file 01. see file 01's note on estimated_gateway_total_fee_usd.
    select client_id, created_at, payment_method, payment_gateway, cast(0 as float64)
    from `wego-cloud.wego_analytics.hotels_bookings`
    where date(created_at) = settle_date
      and conversions_adjusted > 0
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  )

  , first_payment_delta as (
    select
      client_id
    , payment_method as first_payment_method
    , payment_gateway as first_payment_gateway
    , payment_fee_usd as first_payment_fee_usd
    from payment_events
    qualify row_number() over (partition by client_id order by created_at asc) = 1
  )

  -- ----------------------------------------------------------- the delta --

  select
    b.client_id
  , b.day_first_created_at
  , b.day_last_created_at
  , b.day_total_sessions
  , b.flights_searches
  , b.hotels_searches
  , b.total_searches
  , b.total_clicks
  , b.total_bookings
  , b.flights_clicks
  , b.total_flights_bookings
  , b.meta_flights_bookings
  , b.BOW_flights_bookings
  , b.hotels_clicks
  , b.total_hotels_bookings
  , b.meta_hotels_bookings
  , b.BOW_hotels_bookings
  , b.booking_sessions
  , b.bow_booking_sessions
  , b.bow_flights_booking_sessions
  , b.bow_hotels_booking_sessions
  , sa.* except (client_id)
  , ca.* except (client_id, first_ua_campaign_raw)
  , case
      when ca.first_ua_campaign_raw like "None:%" then regexp_extract(ca.first_ua_campaign_raw, r':(.+)')
      when ca.first_ua_campaign_raw like "%:%" then regexp_extract(ca.first_ua_campaign_raw, r'([^:]+):')
      when ca.first_ua_source = "apple search ads" then regexp_extract(ca.first_ua_campaign_raw, r'([^:]+):')
      else ca.first_ua_campaign_raw
    end as first_ua_campaign
  , rd.* except (client_id)
  , fp.product as first_product_delta
  , pay.first_payment_method
  , pay.first_payment_gateway
  , pay.first_payment_fee_usd
  from base_delta b
  left join session_anchors sa on b.client_id = sa.client_id
  left join client_anchors ca on b.client_id = ca.client_id
  left join revenue_delta rd on b.client_id = rd.client_id
  left join first_product_delta fp on b.client_id = fp.client_id
  left join first_payment_delta pay on b.client_id = pay.client_id

) s
on t.client_id = s.client_id

-- =============================== EXISTING CLIENT =============================
-- the watermark guard makes this replay-safe. without it, a retry double-counts.
when matched and (t.updated_date is null or t.updated_date < settle_date) then update set
    t.updated_date = settle_date
  , t.last_created_at = greatest(t.last_created_at, date(s.day_last_created_at))

  -- ADDITIVE: counts
  , t.total_sessions               = ifnull(t.total_sessions, 0) + s.day_total_sessions
  , t.flights_searches             = ifnull(t.flights_searches, 0) + s.flights_searches
  , t.hotels_searches              = ifnull(t.hotels_searches, 0) + s.hotels_searches
  , t.total_searches               = ifnull(t.total_searches, 0) + s.total_searches
  , t.total_clicks                 = ifnull(t.total_clicks, 0) + s.total_clicks
  , t.total_bookings               = ifnull(t.total_bookings, 0) + s.total_bookings
  , t.flights_clicks               = ifnull(t.flights_clicks, 0) + s.flights_clicks
  , t.total_flights_bookings       = ifnull(t.total_flights_bookings, 0) + s.total_flights_bookings
  , t.meta_flights_bookings        = ifnull(t.meta_flights_bookings, 0) + s.meta_flights_bookings
  , t.BOW_flights_bookings         = ifnull(t.BOW_flights_bookings, 0) + s.BOW_flights_bookings
  , t.hotels_clicks                = ifnull(t.hotels_clicks, 0) + s.hotels_clicks
  , t.total_hotels_bookings        = ifnull(t.total_hotels_bookings, 0) + s.total_hotels_bookings
  , t.meta_hotels_bookings         = ifnull(t.meta_hotels_bookings, 0) + s.meta_hotels_bookings
  , t.BOW_hotels_bookings          = ifnull(t.BOW_hotels_bookings, 0) + s.BOW_hotels_bookings

  -- ADDITIVE: the four booking-session counters that seed master's ranks
  , t.booking_sessions             = ifnull(t.booking_sessions, 0) + s.booking_sessions
  , t.bow_booking_sessions         = ifnull(t.bow_booking_sessions, 0) + s.bow_booking_sessions
  , t.bow_flights_booking_sessions = ifnull(t.bow_flights_booking_sessions, 0) + s.bow_flights_booking_sessions
  , t.bow_hotels_booking_sessions  = ifnull(t.bow_hotels_booking_sessions, 0) + s.bow_hotels_booking_sessions

  -- ADDITIVE: revenue
  , t.total_finance_revenue_usd          = ifnull(t.total_finance_revenue_usd, 0) + ifnull(s.total_finance_revenue_usd, 0)
  , t.total_cau_finance_revenue_usd      = ifnull(t.total_cau_finance_revenue_usd, 0) + ifnull(s.total_cau_finance_revenue_usd, 0)
  , t.flights_cau_finance_revenue_usd    = ifnull(t.flights_cau_finance_revenue_usd, 0) + ifnull(s.flights_cau_finance_revenue_usd, 0)
  , t.hotels_cau_finance_revenue_usd     = ifnull(t.hotels_cau_finance_revenue_usd, 0) + ifnull(s.hotels_cau_finance_revenue_usd, 0)
  , t.flights_finance_revenue_usd        = ifnull(t.flights_finance_revenue_usd, 0) + ifnull(s.flights_finance_revenue_usd, 0)
  , t.meta_flights_finance_revenue_usd   = ifnull(t.meta_flights_finance_revenue_usd, 0) + ifnull(s.meta_flights_finance_revenue_usd, 0)
  , t.BOW_flights_finance_revenue_usd    = ifnull(t.BOW_flights_finance_revenue_usd, 0) + ifnull(s.BOW_flights_finance_revenue_usd, 0)
  , t.hotels_finance_revenue_usd         = ifnull(t.hotels_finance_revenue_usd, 0) + ifnull(s.hotels_finance_revenue_usd, 0)
  , t.meta_hotels_finance_revenue_usd    = ifnull(t.meta_hotels_finance_revenue_usd, 0) + ifnull(s.meta_hotels_finance_revenue_usd, 0)
  , t.BOW_hotels_finance_revenue_usd     = ifnull(t.BOW_hotels_finance_revenue_usd, 0) + ifnull(s.BOW_hotels_finance_revenue_usd, 0)
  , t.flights_booking_revenue_usd        = ifnull(t.flights_booking_revenue_usd, 0) + ifnull(s.flights_booking_revenue_usd, 0)
  , t.meta_flights_booking_revenue_usd   = ifnull(t.meta_flights_booking_revenue_usd, 0) + ifnull(s.meta_flights_booking_revenue_usd, 0)
  , t.BOW_flights_booking_revenue_usd    = ifnull(t.BOW_flights_booking_revenue_usd, 0) + ifnull(s.BOW_flights_booking_revenue_usd, 0)
  , t.hotels_booking_revenue_usd         = ifnull(t.hotels_booking_revenue_usd, 0) + ifnull(s.hotels_booking_revenue_usd, 0)
  , t.meta_hotels_booking_revenue_usd    = ifnull(t.meta_hotels_booking_revenue_usd, 0) + ifnull(s.meta_hotels_booking_revenue_usd, 0)
  , t.BOW_hotels_booking_revenue_usd     = ifnull(t.BOW_hotels_booking_revenue_usd, 0) + ifnull(s.BOW_hotels_booking_revenue_usd, 0)

  -- SET-ONCE: earliest wins, fills a null
  , t.first_click_date                 = least(ifnull(t.first_click_date, s.first_click_date), ifnull(s.first_click_date, t.first_click_date))
  , t.meta_first_booking_date          = least(ifnull(t.meta_first_booking_date, s.meta_first_booking_date), ifnull(s.meta_first_booking_date, t.meta_first_booking_date))
  , t.BOW_first_booking_date           = least(ifnull(t.BOW_first_booking_date, s.BOW_first_booking_date), ifnull(s.BOW_first_booking_date, t.BOW_first_booking_date))
  , t.flights_first_click_date         = least(ifnull(t.flights_first_click_date, s.flights_first_click_date), ifnull(s.flights_first_click_date, t.flights_first_click_date))
  , t.meta_flights_first_booking_date  = least(ifnull(t.meta_flights_first_booking_date, s.meta_flights_first_booking_date), ifnull(s.meta_flights_first_booking_date, t.meta_flights_first_booking_date))
  , t.BOW_flights_first_booking_date   = least(ifnull(t.BOW_flights_first_booking_date, s.BOW_flights_first_booking_date), ifnull(s.BOW_flights_first_booking_date, t.BOW_flights_first_booking_date))
  , t.hotels_first_click_date          = least(ifnull(t.hotels_first_click_date, s.hotels_first_click_date), ifnull(s.hotels_first_click_date, t.hotels_first_click_date))
  , t.meta_hotels_first_booking_date   = least(ifnull(t.meta_hotels_first_booking_date, s.meta_hotels_first_booking_date), ifnull(s.meta_hotels_first_booking_date, t.meta_hotels_first_booking_date))
  , t.BOW_hotels_first_booking_date    = least(ifnull(t.BOW_hotels_first_booking_date, s.BOW_hotels_first_booking_date), ifnull(s.BOW_hotels_first_booking_date, t.BOW_hotels_first_booking_date))
  , t.first_product                    = coalesce(t.first_product, s.first_product_delta)
  , t.first_payment_method             = coalesce(t.first_payment_method, s.first_payment_method)
  , t.first_payment_gateway            = coalesce(t.first_payment_gateway, s.first_payment_gateway)
  , t.first_payment_fee_usd            = coalesce(t.first_payment_fee_usd, s.first_payment_fee_usd)

  -- DERIVED: recomputed from POST-merge totals
  , t.booking_type_status = case
      when ifnull(t.total_flights_bookings,0) + s.total_flights_bookings >= 1 and ifnull(t.total_hotels_bookings,0) + s.total_hotels_bookings = 0 then "flights booker"
      when ifnull(t.total_flights_bookings,0) + s.total_flights_bookings >= 1 and ifnull(t.total_hotels_bookings,0) + s.total_hotels_bookings >= 1 then "flights and hotels booker"
      when ifnull(t.total_flights_bookings,0) + s.total_flights_bookings = 0 and ifnull(t.total_hotels_bookings,0) + s.total_hotels_bookings >= 1 then "hotels booker"
      else "non booker"
    end

  -- DERIVED: was INSERT-ONLY before, so it froze at first sight of the client.
  -- recomputed here on post-merge meta/BOW totals, same as booking_type_status.
  , t.booking_type_status_detailed = case
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights meta bow and hotels meta bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights meta bow and hotels meta booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights meta bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights meta booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "non booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights bow and hotels meta bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "hotels meta bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "hotels bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "hotels meta booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights bow and hotels meta booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings = 0 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights bow and hotels bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights meta and hotels bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings = 0 then "flights meta and hotels meta booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings = 0 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings >= 1 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights meta and hotels meta bow booker"
      when ifnull(t.meta_flights_bookings,0)+s.meta_flights_bookings >= 1 and ifnull(t.BOW_flights_bookings,0)+s.BOW_flights_bookings >= 1 and ifnull(t.meta_hotels_bookings,0)+s.meta_hotels_bookings = 0 and ifnull(t.BOW_hotels_bookings,0)+s.BOW_hotels_bookings >= 1 then "flights meta bow and hotels bow booker"
      else "non booker"
    end

  , t.first_booked_status = coalesce(
      t.first_booked_status
    , case
        when s.meta_first_booking_date < s.BOW_first_booking_date then "meta"
        when s.meta_first_booking_date is not null and s.BOW_first_booking_date is null then "meta"
        when s.BOW_first_booking_date < s.meta_first_booking_date then "BOW"
        when s.BOW_first_booking_date is not null and s.meta_first_booking_date is null then "BOW"
        else null
      end
    )

-- ================================= NEW CLIENT ================================
when not matched then insert (
  client_id
  , first_created_at, last_created_at
  , first_device_type, first_os_type, first_app_version, first_user_country_code
  , first_user_city, first_referrer_url, first_site_code, first_locale, first_channel
  , first_wg_source, first_wg_medium, first_wg_campaign, first_wg_adgroup, first_wg_content
  , first_ts_code, first_source, first_market
  , first_app_rt_source, first_app_rt_medium, first_app_rt_campaign, first_app_rt_adgroup, first_app_rt_content
  , total_sessions
  , first_device_brand, first_device_model, first_marketing_name, first_device_version
  , first_ua_source, first_ua_medium, first_ua_adgroup, first_ua_content
  , first_appsflyer_id, first_advertiser_id
  , booking_type_status, booking_type_status_detailed, first_booked_status
  , flights_searches, hotels_searches, total_searches
  , first_click_date, meta_first_booking_date, BOW_first_booking_date
  , flights_first_click_date, meta_flights_first_booking_date, BOW_flights_first_booking_date
  , hotels_first_click_date, meta_hotels_first_booking_date, BOW_hotels_first_booking_date
  , total_clicks, total_bookings, flights_clicks
  , total_flights_bookings, meta_flights_bookings, BOW_flights_bookings
  , hotels_clicks, total_hotels_bookings, meta_hotels_bookings, BOW_hotels_bookings
  , total_finance_revenue_usd, total_cau_finance_revenue_usd
  , flights_cau_finance_revenue_usd, hotels_cau_finance_revenue_usd
  , flights_finance_revenue_usd, meta_flights_finance_revenue_usd, BOW_flights_finance_revenue_usd
  , hotels_finance_revenue_usd, meta_hotels_finance_revenue_usd, BOW_hotels_finance_revenue_usd
  , flights_booking_revenue_usd, meta_flights_booking_revenue_usd, BOW_flights_booking_revenue_usd
  , hotels_booking_revenue_usd, meta_hotels_booking_revenue_usd, BOW_hotels_booking_revenue_usd
  , first_product
  , uninstall_time, churn_time_in_days
  , first_ua_campaign
  , first_payment_method, first_payment_gateway, first_payment_fee_usd
  , booking_sessions, bow_booking_sessions, bow_flights_booking_sessions, bow_hotels_booking_sessions
  , updated_date
)
values (
  s.client_id
  , date(s.day_first_created_at), date(s.day_last_created_at)
  , s.first_device_type, s.first_os_type, s.first_app_version, s.first_user_country_code
  , s.first_user_city, s.first_referrer_url, s.first_site_code, s.first_locale, s.first_channel
  , s.first_wg_source, s.first_wg_medium, s.first_wg_campaign, s.first_wg_adgroup, s.first_wg_content
  , s.first_ts_code, s.first_source, s.first_market
  , s.first_app_rt_source, s.first_app_rt_medium, s.first_app_rt_campaign, s.first_app_rt_adgroup, s.first_app_rt_content
  , s.day_total_sessions
  , s.first_device_brand, s.first_device_model, s.first_marketing_name, s.first_device_version
  , s.first_ua_source, s.first_ua_medium, s.first_ua_adgroup, s.first_ua_content
  , s.first_appsflyer_id, s.first_advertiser_id
  , case
      when s.total_flights_bookings >= 1 and s.total_hotels_bookings = 0 then "flights booker"
      when s.total_flights_bookings >= 1 and s.total_hotels_bookings >= 1 then "flights and hotels booker"
      when s.total_flights_bookings = 0 and s.total_hotels_bookings >= 1 then "hotels booker"
      else "non booker"
    end
  , case
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings >= 1 then "flights meta bow and hotels meta bow booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings = 0 then "flights meta bow and hotels meta booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings = 0 then "flights meta bow booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings = 0 then "flights meta booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings = 0 then "non booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings >= 1 then "flights bow and hotels meta bow booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings >= 1 then "hotels meta bow booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings >= 1 then "hotels bow booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings = 0 then "hotels meta booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings = 0 then "flights bow booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings = 0 then "flights bow and hotels meta booker"
      when s.meta_flights_bookings = 0 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings >= 1 then "flights bow and hotels bow booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings >= 1 then "flights meta and hotels bow booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings = 0 then "flights meta and hotels meta booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings = 0 and s.meta_hotels_bookings >= 1 and s.BOW_hotels_bookings >= 1 then "flights meta and hotels meta bow booker"
      when s.meta_flights_bookings >= 1 and s.BOW_flights_bookings >= 1 and s.meta_hotels_bookings = 0 and s.BOW_hotels_bookings >= 1 then "flights meta bow and hotels bow booker"
      else "non booker"
    end
  , case
      when s.meta_first_booking_date < s.BOW_first_booking_date then "meta"
      when s.meta_first_booking_date is not null and s.BOW_first_booking_date is null then "meta"
      when s.BOW_first_booking_date < s.meta_first_booking_date then "BOW"
      when s.BOW_first_booking_date is not null and s.meta_first_booking_date is null then "BOW"
      else null
    end
  , s.flights_searches, s.hotels_searches, s.total_searches
  , s.first_click_date, s.meta_first_booking_date, s.BOW_first_booking_date
  , s.flights_first_click_date, s.meta_flights_first_booking_date, s.BOW_flights_first_booking_date
  , s.hotels_first_click_date, s.meta_hotels_first_booking_date, s.BOW_hotels_first_booking_date
  , s.total_clicks, s.total_bookings, s.flights_clicks
  , s.total_flights_bookings, s.meta_flights_bookings, s.BOW_flights_bookings
  , s.hotels_clicks, s.total_hotels_bookings, s.meta_hotels_bookings, s.BOW_hotels_bookings
  , ifnull(s.total_finance_revenue_usd, 0), ifnull(s.total_cau_finance_revenue_usd, 0)
  , ifnull(s.flights_cau_finance_revenue_usd, 0), ifnull(s.hotels_cau_finance_revenue_usd, 0)
  , ifnull(s.flights_finance_revenue_usd, 0), ifnull(s.meta_flights_finance_revenue_usd, 0), ifnull(s.BOW_flights_finance_revenue_usd, 0)
  , ifnull(s.hotels_finance_revenue_usd, 0), ifnull(s.meta_hotels_finance_revenue_usd, 0), ifnull(s.BOW_hotels_finance_revenue_usd, 0)
  , ifnull(s.flights_booking_revenue_usd, 0), ifnull(s.meta_flights_booking_revenue_usd, 0), ifnull(s.BOW_flights_booking_revenue_usd, 0)
  , ifnull(s.hotels_booking_revenue_usd, 0), ifnull(s.meta_hotels_booking_revenue_usd, 0), ifnull(s.BOW_hotels_booking_revenue_usd, 0)
  -- stored RAW ("flights"/"hotels"/"BOW flights"/"BOW hotels"), matching file 01.
  -- master derives BOTH first_product and first_model from this string.
  , s.first_product_delta
  -- uninstall_time / churn_time_in_days are NULL here on purpose; MERGE 2 fills
  -- them. an uninstall produces no session, so it cannot ride this delta.
  , cast(null as timestamp), cast(null as int64)
  , s.first_ua_campaign
  , s.first_payment_method, s.first_payment_gateway, s.first_payment_fee_usd
  , s.booking_sessions, s.bow_booking_sessions, s.bow_flights_booking_sessions, s.bow_hotels_booking_sessions
  , settle_date
)
;


-- =============================================================================
-- MERGE 2 of 2 -- uninstalls
--
-- uninstall_time and churn_time_in_days CANNOT ride the session delta above: an
-- uninstall is an appsflyer event that generates no session, so the client is
-- not in "clients active on the settled day" and would never be matched. before
-- this pass existed they were written by NEITHER branch -- 15.4M clients would
-- have frozen at whatever the last full rebuild left, and new clients would
-- have stayed NULL forever.
--
-- keyed on first_appsflyer_id, and restricted to recent uninstall events so it
-- reads a slice rather than the whole wildcard table. uninstall_time only ever
-- changes when a new event lands, so a trailing window is sufficient.
-- =============================================================================

merge `wego-cloud.analysis.client_lifetime_kpis` t
using (
  select
    k.client_id
  , u.uninstall_time
  , date_diff(date(u.uninstall_time), k.first_created_at, day) as churn_time_in_days
  from `wego-cloud.analysis.client_lifetime_kpis` k
  join (
    select
      appsflyer_id
    , max(event_time) as uninstall_time
    from `wego-cloud.external_appsflyer.uninstalls*`
    where event_time is not null
      and date(event_time) between date_sub(settle_date, interval 7 day) and current_date()
    group by 1
  ) u
    on k.first_appsflyer_id = u.appsflyer_id
  where date(u.uninstall_time) > k.first_created_at
) s
on t.client_id = s.client_id
when matched then update set
    t.uninstall_time     = s.uninstall_time
  , t.churn_time_in_days = s.churn_time_in_days
;
{% endraw %}
