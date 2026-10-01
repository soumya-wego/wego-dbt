{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : clients_sessions_aggregated_daily_master_delsert
-- Destination: analysis.clients_sessions_aggregated_daily_master  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('client_lifetime_kpis') }}
-- depends_on: {{ ref('clients_sessions_aggregated_daily') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookables') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookables') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
{% raw %}
-- =============================================================================
-- NEW LOGIC · FILE 03 · clients_sessions_aggregated_daily_master -- DAILY
-- destination : wego-cloud.analysis.clients_sessions_aggregated_daily_master
-- schedule    : every day ~02:30 UTC (replaces config 660662f7's full rebuild)
-- grain       : 1 row per (client_id, session_id)
-- method      : DELETE + INSERT over a 7-day window [D-7 .. D-1]
-- replaces    : a 2.96 TB nightly full rebuild, AND retires _master_pre entirely
--
-- !! RUN AFTER FILE 02. cumulatives are seeded by subtracting this window's own
-- !! contribution from clk's lifetime totals, so clk must already include D-1.
--
-- the seeding identity, for every cumulative column:
--     cumulative(row) = clk.lifetime_total + running_sum(window)
--
-- NO SUBTRACTION. clk now stops at D-8 (file 02's watermark) and this window is
-- [D-7 .. D-1], so they ABUT rather than overlap -- clk.lifetime_total IS the
-- state as of the instant before the window opens. subtracting the window's own
-- contribution, as an earlier draft did, would remove it twice.
--
-- the four transaction ranks use the same identity against the four
-- booking-SESSION counters added to clk in file 02.
--
-- 7 days because 0 bookings were observed attaching more than 7 days after
-- their session; the window is the late-arrival reprocess guarantee.
--
-- columns: 169 existing + 1 (cf_bot_score) = 170
-- has_clicks / has_revenue are NOT stored: master already carries total_clicks
-- and total_finance_revenue_usd, so they are pure derivations. Looker dimensions.
--
-- CORRECTNESS CHANGES vs prod, both deliberate:
--   1. every window is ordered by (created_at, session_id), not created_at
--      alone. prod has no tiebreaker and 6.13% of rows tie, which makes
--      session_count and cummulative_total_sessions non-reproducible. expect
--      ~7% of session_count values to move on first run.
--   2. the six payment columns are produced BY THIS QUERY, not layered on
--      afterwards, so they can no longer be clobbered by a rerun.
--
-- OPEN ITEM -- latest_session_rank (col 94). prod recomputes it for the whole
-- history nightly, so a 2024 row's rank keeps changing as the client returns.
-- an incremental build cannot do that. here it is computed exactly at write
-- time as (clk.total_sessions - session_count + 1), which is correct for every
-- row in the window, but rows outside the window keep the value they had when
-- they were last rebuilt. if any dashboard depends on latest_session_rank = 1
-- meaning "the client's most recent session ever", that consumer needs to move
-- to `created_at = clk.last_created_at` instead. FLAG BEFORE DEPLOY.
-- =============================================================================

declare win_start date default date_sub(current_date(), interval 7 day);
declare win_end   date default date_sub(current_date(), interval 1 day);
-- bounded lookback for the two previous_bow_* lag dates and previous_session.
-- 400 days, not 2018 -- this is the only backward read in the whole file.
-- anchored to win_start, NOT to current_date(): a pinned backfill must look back
-- from the window it is building, not from today.
declare lookback_start date default date_sub(win_start, interval 400 day);

-- =============================================================================
-- ABUTMENT GUARD
-- clk stops at its watermark; this window starts the day after. if file 02 has
-- not run, clk stalls and the days between its watermark and win_start fall
-- into NEITHER clk's totals nor this window -- cumulatives would be silently
-- understated, with no error. fail loudly instead.
-- =============================================================================
assert (
  select max(updated_date) = date_sub(win_start, interval 1 day)
  from `wego-cloud.analysis.client_lifetime_kpis`
) as 'clk watermark does not abut the master window - run file 02 first';

begin transaction;

delete from `wego-cloud.analysis.clients_sessions_aggregated_daily_master`
where created_at_date between win_start and win_end;

insert into `wego-cloud.analysis.clients_sessions_aggregated_daily_master`
(
  session_id, client_id, created_at_date, created_at
  , device_type, os_type, app_version, user_country_code, user_city, referrer_url
  , site_code, locale, channel
  , wg_source, wg_medium, wg_campaign, wg_adgroup, wg_content
  , ts_code, source, market
  , app_rt_source, app_rt_medium, app_rt_campaign, app_rt_adgroup, app_rt_content
  , flights_searches, hotels_searches, total_searches
  , total_clicks, total_bookings, flights_clicks
  , total_flights_bookings, meta_flights_bookings, BOW_flights_bookings
  , hotels_clicks, total_hotels_bookings, meta_hotels_bookings, BOW_hotels_bookings
  , total_finance_revenue_usd, total_cau_finance_revenue_usd
  , flights_cau_finance_revenue_usd, hotels_cau_finance_revenue_usd
  , flights_finance_revenue_usd, meta_flights_finance_revenue_usd, BOW_flights_finance_revenue_usd
  , hotels_finance_revenue_usd, meta_hotels_finance_revenue_usd, BOW_hotels_finance_revenue_usd
  , flights_booking_revenue_usd, meta_flights_booking_revenue_usd, BOW_flights_booking_revenue_usd
  , hotels_booking_revenue_usd, meta_hotels_booking_revenue_usd, BOW_hotels_booking_revenue_usd
  , flights_gmv_tracked, meta_flights_gmv_tracked, BOW_flights_gmv_tracked
  , hotels_gmv_tracked, meta_hotels_gmv_tracked, BOW_hotels_gmv_tracked
  , cummulative_total_sessions, cummulative_flights_searches, cummulative_hotels_searches
  , cummulative_total_searches, cummulative_total_clicks, cummulative_total_bookings
  , cummulative_flights_clicks, cummulative_total_flights_bookings
  , cummulative_meta_flights_bookings, cummulative_BOW_flights_bookings
  , cummulative_hotels_clicks, cummulative_total_hotels_bookings
  , cummulative_meta_hotels_bookings, cummulative_BOW_hotels_bookings
  , cummulative_total_finance_revenue_usd, cummulative_total_cau_finance_revenue_usd
  , cummulative_flights_cau_finance_revenue_usd, cummulative_hotels_cau_finance_revenue_usd
  , cummulative_flights_finance_revenue_usd, cummulative_meta_flights_finance_revenue_usd
  , cummulative_BOW_flights_finance_revenue_usd, cummulative_hotels_finance_revenue_usd
  , cummulative_meta_hotels_finance_revenue_usd, cummulative_BOW_hotels_finance_revenue_usd
  , cummulative_flights_booking_revenue_usd, cummulative_meta_flights_booking_revenue_usd
  , cummulative_BOW_flights_booking_revenue_usd, cummulative_hotels_booking_revenue_usd
  , cummulative_meta_hotels_booking_revenue_usd, cummulative_BOW_hotels_booking_revenue_usd
  , session_count, previous_session_created_at, latest_session_rank
  , first_created_at, last_created_at
  , first_device_type, first_os_type, first_app_version, first_user_country_code
  , first_user_city, first_referrer_url, first_site_code, first_locale, first_channel
  , first_wg_source, first_wg_medium, first_wg_campaign, first_wg_adgroup, first_wg_content
  , first_ts_code, first_source, first_market
  , first_app_rt_source, first_app_rt_medium, first_app_rt_campaign
  , first_app_rt_adgroup, first_app_rt_content
  , first_device_brand, first_device_model, first_marketing_name, first_device_version
  , first_ua_source, first_ua_medium, first_ua_adgroup, first_ua_content
  , first_appsflyer_id, first_advertiser_id
  , booking_type_status, booking_type_status_detailed, first_booked_status
  , first_click_date, meta_first_booking_date, BOW_first_booking_date
  , flights_first_click_date, meta_flights_first_booking_date, BOW_flights_first_booking_date
  , hotels_first_click_date, meta_hotels_first_booking_date, BOW_hotels_first_booking_date
  , first_ua_campaign, first_product, first_model
  , transaction_rank, bow_transaction_rank
  , bow_flights_transaction_rank, bow_hotels_transaction_rank
  , previous_bow_flights_created_at_date, previous_bow_hotels_created_at_date
  , new_existing_bookings, new_existing_Session
  , Customer_Session_Status, customer_booking_type_status_detailed
  , customer_booking_type_status, product, model
  , flights_promo_code, flights_promo_discount_amount_usd
  , hotels_promo_discount_amount_usd, hotels_promo_code
  , bow_flights_acquisition_promo_code, bow_hotels_acquisition_promo_code
  , first_booking_date
  , first_payment_method, first_payment_gateway
  , payment_method, payment_gateway, payment_fee_usd, first_payment_fee_usd
  , cf_bot_score
)

-- (A0) sessions master ALREADY holds before the window.
--
-- prod dedupes globally -- qualify row_number() over (client_id, session_id
-- order by created_at) = 1, earliest wins -- across all history. a 7-day window
-- can only dedupe across itself, so without this a session recorded earlier gets
-- written a SECOND time and master's grain breaks. measured on a 7-day run:
-- 418,398 sessions collide this way.
--
-- the lookback is bounded to 90 days deliberately. full history costs 175 GB a
-- night and catches 100%; 90 days costs 8.8 GB and catches 71.7%. the 118,595
-- that escape are 99.5% zero-activity sessions (590 with any clicks or bookings),
-- so the residual is row-count drift of roughly 0.26%/year, not a metric error.
-- widen this if that drift ever matters.
--
-- !! note: (client_id, session_id) is NOT genuinely unique over long spans --
-- !! 118,595 pairs recur more than 90 days apart across 96,307 clients. prod
-- !! hides this by discarding the later visit, which drops ~1,905 bookings and
-- !! ~$1,055 per 7-day window. that is a prod data question, tracked separately.
with already_in_master as (
  select distinct client_id, session_id
  from `wego-cloud.analysis.clients_sessions_aggregated_daily_master`
  where created_at_date between date_sub(win_start, interval 90 day)
                            and date_sub(win_start, interval 1 day)
)

-- (A) the window, straight from the incremental base table.
-- !! the anti-join MUST be here and not in the final select: the running sums
-- !! below are computed over `facts`, so excluding a session later would leave
-- !! it inside the window total while clk already counts it before the window,
-- !! double-counting it for that client.
, sess as (
  select b.*
  from `wego-cloud.analysis.clients_sessions_aggregated_daily` b
  left join already_in_master a
    on b.client_id = a.client_id
    and b.session_id = a.session_id
  where b.created_at_date between win_start and win_end
    and b.client_id is not null
    and b.client_id not in ('', "00000000-0000-0000-0000-000000000000")
    and a.client_id is null
)

-- (B) revenue and gmv for the window only, same six legs as _pre
, flights_clicks_leg as (
  select
    client_id, session_id, "flights" as product
    , sum(if(provider_code != "wego.com", finance_revenue_usd, 0)) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
    , 0 as cau_revenue
    , sum(case when conversions_tracked > 0 then total_price_usd end) as tracked_gmv
  from `wego-cloud.wego_analytics.flights_clicks`
  where date(_partitiontime) between win_start and win_end
    and client_id is not null
    and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
)

, flights_bookables_leg as (
  select
    client_id, session_id, "flights" as product
    , 0 as finance_revenue_usd, 0 as booking_revenue_usd
    , sum(finance_revenue_usd) as cau_revenue
    , 0 as tracked_gmv
  from `wego-cloud.wego_analytics.flights_bookables`
  where date(_partitiontime) between win_start and win_end
    and client_id is not null
    and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
)

, hotels_clicks_leg as (
  select
    client_id, session_id, "hotels" as product
    , sum(if(provider_code != "hotels.wego.com", finance_revenue_usd, 0)) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
    , 0 as cau_revenue
    , sum(case when conversions_tracked > 0 then total_price_usd end) as tracked_gmv
  from `wego-cloud.wego_analytics.hotels_clicks`
  where date(_partitiontime) between win_start and win_end
    and client_id is not null
    and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
)

, hotels_bookables_leg as (
  select
    client_id, session_id, "hotels" as product
    , 0 as finance_revenue_usd, 0 as booking_revenue_usd
    , sum(finance_revenue_usd) as cau_revenue
    , 0 as tracked_gmv
  from `wego-cloud.wego_analytics.hotels_bookables`
  where date(_partitiontime) between win_start and win_end
    and client_id is not null
    and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
)

, flights_insurance_lookup as (
  select booking_id, finance_revenue_usd as insurance_revenue
  from `wego-cloud.wego_analytics.flights_insurance`
)

, bow_flights_leg as (
  select
    b.client_id, b.session_id, "BOW flights" as product
    , sum(b.finance_revenue_usd) + sum(ifnull(i.insurance_revenue, 0)) + sum(ifnull(b.distribution_cost_usd, 0)) as finance_revenue_usd
    , sum(b.finance_revenue_usd) + sum(ifnull(i.insurance_revenue, 0)) + sum(ifnull(b.distribution_cost_usd, 0)) as booking_revenue_usd
    , 0 as cau_revenue
    , sum(b.total_price_usd) as tracked_gmv
  from `wego-cloud.wego_analytics.flights_bookings` b
  left join flights_insurance_lookup i
    on b.booking_id = i.booking_id
  where date(b._partitiontime) between win_start and win_end
    and b.conversions_adjusted > 0
    and b.client_id is not null
    and b.client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
)

, bow_hotels_leg as (
  select
    client_id, session_id, "BOW hotels" as product
    , sum(finance_revenue_usd) + sum(ifnull(distribution_cost_usd, 0)) as finance_revenue_usd
    , sum(finance_revenue_usd) + sum(ifnull(distribution_cost_usd, 0)) as booking_revenue_usd
    , 0 as cau_revenue
    , sum(wego_total_price_usd) as tracked_gmv
  from `wego-cloud.wego_analytics.hotels_bookings`
  where date(created_at) between win_start and win_end
    and conversions_adjusted > 0
    and client_id is not null
    and client_id not in ('', "00000000-0000-0000-0000-000000000000")
  group by 1, 2
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

, session_revenue as (
  select
    client_id, session_id
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
    , sum(if(product = "flights", tracked_gmv, 0)) + sum(if(product = "BOW flights", tracked_gmv, 0)) as flights_gmv_tracked
    , sum(if(product = "flights", tracked_gmv, 0)) as meta_flights_gmv_tracked
    , sum(if(product = "BOW flights", tracked_gmv, 0)) as BOW_flights_gmv_tracked
    , sum(if(product = "hotels", tracked_gmv, 0)) + sum(if(product = "BOW hotels", tracked_gmv, 0)) as hotels_gmv_tracked
    , sum(if(product = "hotels", tracked_gmv, 0)) as meta_hotels_gmv_tracked
    , sum(if(product = "BOW hotels", tracked_gmv, 0)) as BOW_hotels_gmv_tracked
  from revenue_union
  group by 1, 2
)

-- (C) session facts = base + revenue, still window-only
, facts as (
  select
    s.*
    , ifnull(r.total_finance_revenue_usd, 0) as total_finance_revenue_usd
    , ifnull(r.total_cau_finance_revenue_usd, 0) as total_cau_finance_revenue_usd
    , ifnull(r.flights_cau_finance_revenue_usd, 0) as flights_cau_finance_revenue_usd
    , ifnull(r.hotels_cau_finance_revenue_usd, 0) as hotels_cau_finance_revenue_usd
    , ifnull(r.flights_finance_revenue_usd, 0) as flights_finance_revenue_usd
    , ifnull(r.meta_flights_finance_revenue_usd, 0) as meta_flights_finance_revenue_usd
    , ifnull(r.BOW_flights_finance_revenue_usd, 0) as BOW_flights_finance_revenue_usd
    , ifnull(r.hotels_finance_revenue_usd, 0) as hotels_finance_revenue_usd
    , ifnull(r.meta_hotels_finance_revenue_usd, 0) as meta_hotels_finance_revenue_usd
    , ifnull(r.BOW_hotels_finance_revenue_usd, 0) as BOW_hotels_finance_revenue_usd
    , ifnull(r.flights_booking_revenue_usd, 0) as flights_booking_revenue_usd
    , ifnull(r.meta_flights_booking_revenue_usd, 0) as meta_flights_booking_revenue_usd
    , ifnull(r.BOW_flights_booking_revenue_usd, 0) as BOW_flights_booking_revenue_usd
    , ifnull(r.hotels_booking_revenue_usd, 0) as hotels_booking_revenue_usd
    , ifnull(r.meta_hotels_booking_revenue_usd, 0) as meta_hotels_booking_revenue_usd
    , ifnull(r.BOW_hotels_booking_revenue_usd, 0) as BOW_hotels_booking_revenue_usd
    , ifnull(r.flights_gmv_tracked, 0) as flights_gmv_tracked
    , ifnull(r.meta_flights_gmv_tracked, 0) as meta_flights_gmv_tracked
    , ifnull(r.BOW_flights_gmv_tracked, 0) as BOW_flights_gmv_tracked
    , ifnull(r.hotels_gmv_tracked, 0) as hotels_gmv_tracked
    , ifnull(r.meta_hotels_gmv_tracked, 0) as meta_hotels_gmv_tracked
    , ifnull(r.BOW_hotels_gmv_tracked, 0) as BOW_hotels_gmv_tracked
  from sess s
  left join session_revenue r
    on s.client_id = r.client_id
    and s.session_id = r.session_id
)

-- (E) bounded lookback -- the ONLY backward read in this file.
-- supplies the three carry-in values a window cannot derive by subtraction:
-- the previous session timestamp and the two previous BOW booking dates.
, carry_in as (
  select
    client_id
    , max(created_at) as prior_session_created_at
    , max(if(coalesce(BOW_flights_bookings, 0) > 0, created_at_date, null)) as prior_bow_flights_date
    , max(if(coalesce(BOW_hotels_bookings, 0) > 0, created_at_date, null)) as prior_bow_hotels_date
  from `wego-cloud.analysis.clients_sessions_aggregated_daily_master`
  where created_at_date between lookback_start and date_sub(win_start, interval 1 day)
  group by 1
)

-- (G) payment, per booking session
, flights_pay as (
  select client_id, session_id, payment_method, payment_gateway, payment_fee_usd
  from `wego-cloud.wego_analytics.flights_bookings`
  where date(_partitiontime) between win_start and win_end
    and conversions_adjusted > 0
  qualify row_number() over (partition by client_id, session_id order by created_at asc) = 1
)

-- !! hotels_bookings has no payment_fee_usd column -- explicit zero, matching
-- !! prod. see the note in file 01 on estimated_gateway_total_fee_usd.
, hotels_pay as (
  select
    client_id, session_id, payment_method, payment_gateway
    , cast(0 as float64) as payment_fee_usd
  from `wego-cloud.wego_analytics.hotels_bookings`
  where date(created_at) between win_start and win_end
    and conversions_adjusted > 0
  qualify row_number() over (partition by client_id, session_id order by created_at asc) = 1
)

-- (H) promo, per booking session and per client
, flights_promo as (
  select client_id, session_id, promo_code, promo_discount_amount_usd
  from `wego-cloud.wego_analytics.flights_bookings`
  where date(_partitiontime) between win_start and win_end
    and conversions_adjusted > 0
  qualify row_number() over (partition by client_id, session_id order by created_at asc) = 1
)

, hotels_promo as (
  select client_id, session_id, promo_code, promo_discount_amount_usd
  from `wego-cloud.wego_analytics.hotels_bookings`
  where date(created_at) between win_start and win_end
    and conversions_adjusted > 0
  qualify row_number() over (partition by client_id, session_id order by created_at asc) = 1
)

-- acquisition promo is a lifetime anchor, so it still needs a full read of the
-- bookings tables -- but only two columns, and only for clients in the window.
, flights_promo_acq as (
  select client_id, promo_code
  from `wego-cloud.wego_analytics.flights_bookings`
  where conversions_adjusted > 0
  qualify row_number() over (partition by client_id order by created_at asc) = 1
)

, hotels_promo_acq as (
  select client_id, promo_code
  from `wego-cloud.wego_analytics.hotels_bookings`
  where conversions_adjusted > 0
  qualify row_number() over (partition by client_id order by created_at asc) = 1
)

-- (I) seed each row from clk, then run the window's cumulative sums
, seeded as (
  select
    f.*
    , k.total_sessions as clk_total_sessions
    , k.total_clicks as clk_total_clicks
    , k.total_bookings as clk_total_bookings
    , k.flights_clicks as clk_flights_clicks
    , k.total_flights_bookings as clk_total_flights_bookings
    , k.meta_flights_bookings as clk_meta_flights_bookings
    , k.BOW_flights_bookings as clk_BOW_flights_bookings
    , k.hotels_clicks as clk_hotels_clicks
    , k.total_hotels_bookings as clk_total_hotels_bookings
    , k.meta_hotels_bookings as clk_meta_hotels_bookings
    , k.BOW_hotels_bookings as clk_BOW_hotels_bookings
    , k.flights_searches as clk_flights_searches
    , k.hotels_searches as clk_hotels_searches
    , k.total_searches as clk_total_searches
    , k.total_finance_revenue_usd as clk_total_finance_revenue_usd
    , k.total_cau_finance_revenue_usd as clk_total_cau_finance_revenue_usd
    , k.flights_cau_finance_revenue_usd as clk_flights_cau_finance_revenue_usd
    , k.hotels_cau_finance_revenue_usd as clk_hotels_cau_finance_revenue_usd
    , k.flights_finance_revenue_usd as clk_flights_finance_revenue_usd
    , k.meta_flights_finance_revenue_usd as clk_meta_flights_finance_revenue_usd
    , k.BOW_flights_finance_revenue_usd as clk_BOW_flights_finance_revenue_usd
    , k.hotels_finance_revenue_usd as clk_hotels_finance_revenue_usd
    , k.meta_hotels_finance_revenue_usd as clk_meta_hotels_finance_revenue_usd
    , k.BOW_hotels_finance_revenue_usd as clk_BOW_hotels_finance_revenue_usd
    , k.flights_booking_revenue_usd as clk_flights_booking_revenue_usd
    , k.meta_flights_booking_revenue_usd as clk_meta_flights_booking_revenue_usd
    , k.BOW_flights_booking_revenue_usd as clk_BOW_flights_booking_revenue_usd
    , k.hotels_booking_revenue_usd as clk_hotels_booking_revenue_usd
    , k.meta_hotels_booking_revenue_usd as clk_meta_hotels_booking_revenue_usd
    , k.BOW_hotels_booking_revenue_usd as clk_BOW_hotels_booking_revenue_usd
    , k.booking_sessions as clk_booking_sessions
    , k.bow_booking_sessions as clk_bow_booking_sessions
    , k.bow_flights_booking_sessions as clk_bow_flights_booking_sessions
    , k.bow_hotels_booking_sessions as clk_bow_hotels_booking_sessions
    -- clk anchor columns, passed straight through to master 95-143
    , k.first_created_at, k.last_created_at
    , k.first_device_type, k.first_os_type, k.first_app_version, k.first_user_country_code
    , k.first_user_city, k.first_referrer_url, k.first_site_code, k.first_locale, k.first_channel
    , k.first_wg_source, k.first_wg_medium, k.first_wg_campaign, k.first_wg_adgroup, k.first_wg_content
    , k.first_ts_code, k.first_source, k.first_market
    , k.first_app_rt_source, k.first_app_rt_medium, k.first_app_rt_campaign
    , k.first_app_rt_adgroup, k.first_app_rt_content
    , k.first_device_brand, k.first_device_model, k.first_marketing_name, k.first_device_version
    , k.first_ua_source, k.first_ua_medium, k.first_ua_adgroup, k.first_ua_content
    , k.first_appsflyer_id, k.first_advertiser_id
    , k.booking_type_status, k.booking_type_status_detailed, k.first_booked_status
    , k.first_click_date, k.meta_first_booking_date, k.BOW_first_booking_date
    , k.flights_first_click_date, k.meta_flights_first_booking_date, k.BOW_flights_first_booking_date
    , k.hotels_first_click_date, k.meta_hotels_first_booking_date, k.BOW_hotels_first_booking_date
    , k.first_ua_campaign
    , k.first_product as clk_first_product
    , k.first_payment_method, k.first_payment_gateway, k.first_payment_fee_usd
    -- carry-in
    , ci.prior_session_created_at
    , ci.prior_bow_flights_date
    , ci.prior_bow_hotels_date
  from facts f
  left join `wego-cloud.analysis.client_lifetime_kpis` k
    on f.client_id = k.client_id
  left join carry_in ci
    on f.client_id = ci.client_id
)

-- (J) cumulative = seed + running sum. tiebreaker (created_at, session_id).
, cumed as (
  select
    *
    , ifnull(clk_total_sessions, 0) + count(*) over w as cummulative_total_sessions
    , ifnull(clk_flights_searches, 0) + sum(flights_searches) over w as cummulative_flights_searches
    , ifnull(clk_hotels_searches, 0) + sum(hotels_searches) over w as cummulative_hotels_searches
    , ifnull(clk_total_searches, 0) + sum(total_searches) over w as cummulative_total_searches
    , ifnull(clk_total_clicks, 0) + sum(total_clicks) over w as cummulative_total_clicks
    , ifnull(clk_total_bookings, 0) + sum(total_bookings) over w as cummulative_total_bookings
    , ifnull(clk_flights_clicks, 0) + sum(flights_clicks) over w as cummulative_flights_clicks
    , ifnull(clk_total_flights_bookings, 0) + sum(total_flights_bookings) over w as cummulative_total_flights_bookings
    , ifnull(clk_meta_flights_bookings, 0) + sum(meta_flights_bookings) over w as cummulative_meta_flights_bookings
    , ifnull(clk_BOW_flights_bookings, 0) + sum(BOW_flights_bookings) over w as cummulative_BOW_flights_bookings
    , ifnull(clk_hotels_clicks, 0) + sum(hotels_clicks) over w as cummulative_hotels_clicks
    , ifnull(clk_total_hotels_bookings, 0) + sum(total_hotels_bookings) over w as cummulative_total_hotels_bookings
    , ifnull(clk_meta_hotels_bookings, 0) + sum(meta_hotels_bookings) over w as cummulative_meta_hotels_bookings
    , ifnull(clk_BOW_hotels_bookings, 0) + sum(BOW_hotels_bookings) over w as cummulative_BOW_hotels_bookings
    , ifnull(clk_total_finance_revenue_usd, 0) + sum(total_finance_revenue_usd) over w as cummulative_total_finance_revenue_usd
    , ifnull(clk_total_cau_finance_revenue_usd, 0) + sum(total_cau_finance_revenue_usd) over w as cummulative_total_cau_finance_revenue_usd
    , ifnull(clk_flights_cau_finance_revenue_usd, 0) + sum(flights_cau_finance_revenue_usd) over w as cummulative_flights_cau_finance_revenue_usd
    , ifnull(clk_hotels_cau_finance_revenue_usd, 0) + sum(hotels_cau_finance_revenue_usd) over w as cummulative_hotels_cau_finance_revenue_usd
    , ifnull(clk_flights_finance_revenue_usd, 0) + sum(flights_finance_revenue_usd) over w as cummulative_flights_finance_revenue_usd
    , ifnull(clk_meta_flights_finance_revenue_usd, 0) + sum(meta_flights_finance_revenue_usd) over w as cummulative_meta_flights_finance_revenue_usd
    , ifnull(clk_BOW_flights_finance_revenue_usd, 0) + sum(BOW_flights_finance_revenue_usd) over w as cummulative_BOW_flights_finance_revenue_usd
    , ifnull(clk_hotels_finance_revenue_usd, 0) + sum(hotels_finance_revenue_usd) over w as cummulative_hotels_finance_revenue_usd
    , ifnull(clk_meta_hotels_finance_revenue_usd, 0) + sum(meta_hotels_finance_revenue_usd) over w as cummulative_meta_hotels_finance_revenue_usd
    , ifnull(clk_BOW_hotels_finance_revenue_usd, 0) + sum(BOW_hotels_finance_revenue_usd) over w as cummulative_BOW_hotels_finance_revenue_usd
    , ifnull(clk_flights_booking_revenue_usd, 0) + sum(flights_booking_revenue_usd) over w as cummulative_flights_booking_revenue_usd
    , ifnull(clk_meta_flights_booking_revenue_usd, 0) + sum(meta_flights_booking_revenue_usd) over w as cummulative_meta_flights_booking_revenue_usd
    , ifnull(clk_BOW_flights_booking_revenue_usd, 0) + sum(BOW_flights_booking_revenue_usd) over w as cummulative_BOW_flights_booking_revenue_usd
    , ifnull(clk_hotels_booking_revenue_usd, 0) + sum(hotels_booking_revenue_usd) over w as cummulative_hotels_booking_revenue_usd
    , ifnull(clk_meta_hotels_booking_revenue_usd, 0) + sum(meta_hotels_booking_revenue_usd) over w as cummulative_meta_hotels_booking_revenue_usd
    , ifnull(clk_BOW_hotels_booking_revenue_usd, 0) + sum(BOW_hotels_booking_revenue_usd) over w as cummulative_BOW_hotels_booking_revenue_usd
    , ifnull(clk_total_sessions, 0) + row_number() over w as session_count
    -- previous cumulative bookings, for the "became a customer" transition
    , ifnull(clk_total_bookings, 0) + ifnull(sum(total_bookings) over w_prev, 0) as prev_cummulative_total_bookings
    -- the four booking-session ranks, seeded the same way
    , case when total_bookings > 0
        then ifnull(clk_booking_sessions, 0) + countif(total_bookings > 0) over w
      end as transaction_rank
    , case when coalesce(BOW_flights_bookings, 0) + coalesce(BOW_hotels_bookings, 0) > 0
        then ifnull(clk_bow_booking_sessions, 0)
             + countif(coalesce(BOW_flights_bookings, 0) + coalesce(BOW_hotels_bookings, 0) > 0) over w
      end as bow_transaction_rank
    , case when coalesce(BOW_flights_bookings, 0) > 0
        then ifnull(clk_bow_flights_booking_sessions, 0)
             + countif(coalesce(BOW_flights_bookings, 0) > 0) over w
      end as bow_flights_transaction_rank
    , case when coalesce(BOW_hotels_bookings, 0) > 0
        then ifnull(clk_bow_hotels_booking_sessions, 0)
             + countif(coalesce(BOW_hotels_bookings, 0) > 0) over w
      end as bow_hotels_transaction_rank
    -- lag columns: most recent qualifying row earlier in the window, else carry-in
    , coalesce(
        last_value(created_at ignore nulls) over w_prev
        , prior_session_created_at
      ) as previous_session_created_at_calc
    , case when coalesce(BOW_flights_bookings, 0) > 0 then coalesce(
        last_value(if(coalesce(BOW_flights_bookings, 0) > 0, created_at_date, null) ignore nulls) over w_prev
        , prior_bow_flights_date
      ) end as previous_bow_flights_created_at_date_calc
    , case when coalesce(BOW_hotels_bookings, 0) > 0 then coalesce(
        last_value(if(coalesce(BOW_hotels_bookings, 0) > 0, created_at_date, null) ignore nulls) over w_prev
        , prior_bow_hotels_date
      ) end as previous_bow_hotels_created_at_date_calc
  from seeded
  window
    w as (partition by client_id order by created_at asc, session_id asc)
    , w_prev as (partition by client_id order by created_at asc, session_id asc rows between unbounded preceding and 1 preceding)
)

-- (K) the 172-column output, in master schema order
select
  c.session_id
  , c.client_id
  , c.created_at_date
  , c.created_at
  , c.device_type
  , c.os_type
  , c.app_version
  , c.user_country_code
  , c.user_city
  , c.referrer_url
  , c.site_code
  , c.locale
  , c.channel
  , c.wg_source
  , c.wg_medium
  , c.wg_campaign
  , c.wg_adgroup
  , c.wg_content
  , c.ts_code
  , c.source
  , c.market
  , c.app_rt_source
  , c.app_rt_medium
  , c.app_rt_campaign
  , c.app_rt_adgroup
  , c.app_rt_content
  , c.flights_searches
  , c.hotels_searches
  , c.total_searches
  , c.total_clicks
  , c.total_bookings
  , c.flights_clicks
  , c.total_flights_bookings
  , c.meta_flights_bookings
  , c.BOW_flights_bookings
  , c.hotels_clicks
  , c.total_hotels_bookings
  , c.meta_hotels_bookings
  , c.BOW_hotels_bookings
  , c.total_finance_revenue_usd
  , c.total_cau_finance_revenue_usd
  , c.flights_cau_finance_revenue_usd
  , c.hotels_cau_finance_revenue_usd
  , c.flights_finance_revenue_usd
  , c.meta_flights_finance_revenue_usd
  , c.BOW_flights_finance_revenue_usd
  , c.hotels_finance_revenue_usd
  , c.meta_hotels_finance_revenue_usd
  , c.BOW_hotels_finance_revenue_usd
  , c.flights_booking_revenue_usd
  , c.meta_flights_booking_revenue_usd
  , c.BOW_flights_booking_revenue_usd
  , c.hotels_booking_revenue_usd
  , c.meta_hotels_booking_revenue_usd
  , c.BOW_hotels_booking_revenue_usd
  , c.flights_gmv_tracked
  , c.meta_flights_gmv_tracked
  , c.BOW_flights_gmv_tracked
  , c.hotels_gmv_tracked
  , c.meta_hotels_gmv_tracked
  , c.BOW_hotels_gmv_tracked
  , c.cummulative_total_sessions
  , c.cummulative_flights_searches
  , c.cummulative_hotels_searches
  , c.cummulative_total_searches
  , c.cummulative_total_clicks
  , c.cummulative_total_bookings
  , c.cummulative_flights_clicks
  , c.cummulative_total_flights_bookings
  , c.cummulative_meta_flights_bookings
  , c.cummulative_BOW_flights_bookings
  , c.cummulative_hotels_clicks
  , c.cummulative_total_hotels_bookings
  , c.cummulative_meta_hotels_bookings
  , c.cummulative_BOW_hotels_bookings
  , c.cummulative_total_finance_revenue_usd
  , c.cummulative_total_cau_finance_revenue_usd
  , c.cummulative_flights_cau_finance_revenue_usd
  , c.cummulative_hotels_cau_finance_revenue_usd
  , c.cummulative_flights_finance_revenue_usd
  , c.cummulative_meta_flights_finance_revenue_usd
  , c.cummulative_BOW_flights_finance_revenue_usd
  , c.cummulative_hotels_finance_revenue_usd
  , c.cummulative_meta_hotels_finance_revenue_usd
  , c.cummulative_BOW_hotels_finance_revenue_usd
  , c.cummulative_flights_booking_revenue_usd
  , c.cummulative_meta_flights_booking_revenue_usd
  , c.cummulative_BOW_flights_booking_revenue_usd
  , c.cummulative_hotels_booking_revenue_usd
  , c.cummulative_meta_hotels_booking_revenue_usd
  , c.cummulative_BOW_hotels_booking_revenue_usd
  , c.session_count
  , c.previous_session_created_at_calc as previous_session_created_at
  -- see the OPEN ITEM in the header: exact at write time, frozen thereafter
  , ifnull(c.clk_total_sessions, c.session_count) - c.session_count + 1 as latest_session_rank
  , c.first_created_at
  , c.last_created_at
  , c.first_device_type
  , c.first_os_type
  , c.first_app_version
  , c.first_user_country_code
  , c.first_user_city
  , c.first_referrer_url
  , c.first_site_code
  , c.first_locale
  , c.first_channel
  , c.first_wg_source
  , c.first_wg_medium
  , c.first_wg_campaign
  , c.first_wg_adgroup
  , c.first_wg_content
  , c.first_ts_code
  , c.first_source
  , c.first_market
  , c.first_app_rt_source
  , c.first_app_rt_medium
  , c.first_app_rt_campaign
  , c.first_app_rt_adgroup
  , c.first_app_rt_content
  , c.first_device_brand
  , c.first_device_model
  , c.first_marketing_name
  , c.first_device_version
  , c.first_ua_source
  , c.first_ua_medium
  , c.first_ua_adgroup
  , c.first_ua_content
  , c.first_appsflyer_id
  , c.first_advertiser_id
  , c.booking_type_status
  , c.booking_type_status_detailed
  , c.first_booked_status
  , c.first_click_date
  , c.meta_first_booking_date
  , c.BOW_first_booking_date
  , c.flights_first_click_date
  , c.meta_flights_first_booking_date
  , c.BOW_flights_first_booking_date
  , c.hotels_first_click_date
  , c.meta_hotels_first_booking_date
  , c.BOW_hotels_first_booking_date
  , c.first_ua_campaign
  , case
      when lower(c.clk_first_product) like '%hotel%' then "Hotels"
      when lower(c.clk_first_product) like '%flight%' then "Flights"
    end as first_product
  , case
      when lower(c.clk_first_product) like '%bow%' then "BOW"
      else "Meta"
    end as first_model
  , c.transaction_rank
  , c.bow_transaction_rank
  , c.bow_flights_transaction_rank
  , c.bow_hotels_transaction_rank
  , c.previous_bow_flights_created_at_date_calc as previous_bow_flights_created_at_date
  , c.previous_bow_hotels_created_at_date_calc as previous_bow_hotels_created_at_date
  , case
      when c.transaction_rank = 1 then "New"
      when c.transaction_rank > 1 then "Existing"
    end as new_existing_bookings
  , case
      when c.session_count = 1 then "New"
      else "Existing"
    end as new_existing_Session
  , case
      when c.cummulative_total_bookings = 0 then "non customer"
      when c.cummulative_total_bookings > 0 and ifnull(c.prev_cummulative_total_bookings, 0) = 0 then "became a customer"
      when c.cummulative_total_bookings > 0 then "existing customer"
    end as Customer_Session_Status
  , case
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings >= 1 then "flights meta bow and hotels meta bow booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings = 0 then "flights meta bow and hotels meta booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings = 0 then "flights meta bow booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings = 0 then "flights meta booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings = 0 then "non booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings >= 1 then "flights bow and hotels meta bow booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings >= 1 then "hotels meta bow booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings >= 1 then "hotels bow booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings = 0 then "hotels meta booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings = 0 then "flights bow booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings = 0 then "flights bow and hotels meta booker"
      when c.cummulative_meta_flights_bookings = 0 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings >= 1 then "flights bow and hotels bow booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings >= 1 then "flights meta and hotels bow booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings = 0 then "flights meta and hotels meta booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings = 0 and c.cummulative_meta_hotels_bookings >= 1 and c.cummulative_BOW_hotels_bookings >= 1 then "flights meta and hotels meta bow booker"
      when c.cummulative_meta_flights_bookings >= 1 and c.cummulative_BOW_flights_bookings >= 1 and c.cummulative_meta_hotels_bookings = 0 and c.cummulative_BOW_hotels_bookings >= 1 then "flights meta bow and hotels bow booker"
      else "Non Booker"
    end as customer_booking_type_status_detailed
  , case
      when c.cummulative_total_flights_bookings >= 1 and c.cummulative_total_hotels_bookings >= 1 then "Flights and Hotels booker"
      when c.cummulative_total_flights_bookings >= 1 and c.cummulative_total_hotels_bookings = 0 then "Flights Booker"
      when c.cummulative_total_flights_bookings = 0 and c.cummulative_total_hotels_bookings >= 1 then "Hotels Booker"
      else "Non Booker"
    end as customer_booking_type_status
  , case
      when c.transaction_rank is not null then (
        case
          when c.transaction_rank = 1 then
            case
              when lower(c.clk_first_product) like '%hotel%' then "Hotels"
              when lower(c.clk_first_product) like '%flight%' then "Flights"
            end
          when coalesce(c.total_flights_bookings, 0) >= coalesce(c.total_hotels_bookings, 0) then "Flights"
          when coalesce(c.total_flights_bookings, 0) < coalesce(c.total_hotels_bookings, 0) then "Hotels"
        end
      )
      else null
    end as product
  , case
      when c.transaction_rank is not null then (
        case
          when c.transaction_rank = 1 then
            case
              when lower(c.clk_first_product) like '%bow%' then "BOW"
              else "Meta"
            end
          when coalesce(c.meta_flights_bookings, 0) + coalesce(c.meta_hotels_bookings, 0) >= coalesce(c.BOW_flights_bookings, 0) + coalesce(c.BOW_hotels_bookings, 0) then "Meta"
          else "BOW"
        end
      )
      else null
    end as model
  , fpr.promo_code as flights_promo_code
  , fpr.promo_discount_amount_usd as flights_promo_discount_amount_usd
  , hpr.promo_discount_amount_usd as hotels_promo_discount_amount_usd
  , hpr.promo_code as hotels_promo_code
  , fpa.promo_code as bow_flights_acquisition_promo_code
  , hpa.promo_code as bow_hotels_acquisition_promo_code
  -- !! NOT PROVEN EQUIVALENT. prod derives first_booking_date as
  -- !! min(created_at_date) over _pre rows where total_bookings > 0 -- a SESSION
  -- !! date. clk's meta_/BOW_first_booking_date are derived from the click and
  -- !! booking created_at instead. they will usually agree but can differ when a
  -- !! booking posts on a different day from its session. VERIFY THIS COLUMN
  -- !! SPECIFICALLY in the EXCEPT DISTINCT run before deploying.
  , least(
      ifnull(c.meta_first_booking_date, c.BOW_first_booking_date)
      , ifnull(c.BOW_first_booking_date, c.meta_first_booking_date)
    ) as first_booking_date
  , c.first_payment_method
  , c.first_payment_gateway
  , coalesce(fp.payment_method, hp.payment_method) as payment_method
  , coalesce(fp.payment_gateway, hp.payment_gateway) as payment_gateway
  , coalesce(fp.payment_fee_usd, hp.payment_fee_usd) as payment_fee_usd
  , c.first_payment_fee_usd
  -- raw cf_bot_score, carried straight through from base. master is
  -- (client_id, session_id) grain, so bucketing here would be lossy and
  -- redundant -- bucket in Looker. NULL means Cloudflare did not score the
  -- session (native app traffic, ~14%), NOT that it scored zero.
  , c.cf_bot_score
from cumed c
left join flights_pay fp
  on c.client_id = fp.client_id and c.session_id = fp.session_id
left join hotels_pay hp
  on c.client_id = hp.client_id and c.session_id = hp.session_id
left join flights_promo fpr
  on c.client_id = fpr.client_id and c.session_id = fpr.session_id
left join hotels_promo hpr
  on c.client_id = hpr.client_id and c.session_id = hpr.session_id
left join flights_promo_acq fpa
  on c.client_id = fpa.client_id
left join hotels_promo_acq hpa
  on c.client_id = hpa.client_id
qualify row_number() over (partition by c.client_id, c.session_id order by c.created_at asc, c.session_id asc) = 1
;

commit transaction;
{% endraw %}
