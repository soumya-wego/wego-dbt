{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_pricing_engine_aggregated_daily
-- Destination: analysis.hotels_pricing_engine_aggregated  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('apsalar', 'country_market') }}
-- depends_on: {{ source('hotel_services', 'hotels') }}
-- depends_on: {{ source('ib_hotels', 'sessions') }}
-- depends_on: {{ source('ib_hotels', 'wegorates') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('services_akasha', 'rates') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
-- depends_on: {{ source('wego_analytics', 'hotels_searches') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
-- DEPLOY VERSION — distribution fix for hotels_pricing_engine_aggregated scheduled query
-- Change vs original: added distribution_sessions CTE + UNION ALL in hotels_clicks
-- date_param restored to dynamic (current_date - 1 day)
with date_param as
(
  select
    date(current_date() - interval 1 day) as start_date
    , date(current_date() - interval 1 day) as end_date
    , format_date('%Y%m%d', current_date() - interval 1 day) as start_date2
    , format_date('%Y%m%d', current_date() - interval 1 day) as end_date2
)

, rates as (  -- get the provider rank on price for each search hotel id
  select distinct
    created_at
    , concat(search_id, " - ", hotel_id) as search_hotel_id
    , id as rate_id
    , round((
        coalesce(price.amount_per_night_usd, 0)
        + coalesce(case when price.local_tax_amount_usd <= 0 then 0 else price.local_tax_amount_usd end, 0)
      )) as amount_per_night_usd
    , round((
        coalesce(price.total_amount_usd, 0)
        + coalesce(case when price.total_local_tax_amount_usd <= 0 then 0 else price.total_local_tax_amount_usd end, 0)
      )) as total_amount_usd
    , provider.code as cheapest_provider
    , provider.name as cheapest_provider_name
    , rooms_count
    , row_number() over (
        partition by concat(search_id, " - ", hotel_id)
        order by
          coalesce(price.total_amount_usd, 0)
          + coalesce(case when price.total_local_tax_amount_usd <= 0 then 0 else price.total_local_tax_amount_usd end, 0)
          , price.base_amount_usd asc
      ) as rank
  from `wego-cloud.services_akasha.rates*`
  where _table_suffix BETWEEN (select start_date2 from date_param) and (select end_date2 from date_param)
)

, win as (
  select * from rates where rank = 1
)

, cheapest_rate as (
  select
    search_hotel_id
    , first_value(amount_per_night_usd) over (
        partition by concat(search_hotel_id)
        order by rank asc
      ) as amount_per_night_usd
    , first_value(total_amount_usd) over (
        partition by concat(search_hotel_id)
        order by rank asc
      ) as total_amount_usd
  from rates
  where cheapest_provider != "hotels.wego.com"
)

, rates_base as (
  select
    win.* except(amount_per_night_usd, total_amount_usd)
    , cheapest_rate.* except(search_hotel_id)
  from win
  left join (
    select
      search_hotel_id
      , amount_per_night_usd
      , total_amount_usd
    from cheapest_rate
    group by 1, 2, 3
  ) as cheapest_rate
    on win.search_hotel_id = cheapest_rate.search_hotel_id
)

, market_mapping as
(
  select
    user_country_code
    , market
  from wego-cloud.apsalar.country_market
)

, hotel_mapping as
(
  select distinct
    b.id as hotel_id
    , b.city_code as city_code
    , d.code as country_code
  from `wego-cloud.hotel_services.hotels` b
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
)

, sessions as
(
  select distinct
    date(created_at) as date
    , id as session_id
    , search_id
    , hotel_id
    , s.user_country_code
    , ts_code
    , market
    , created_at
    , site_code
    , device
    , device_type
    , concat(search_id,' - ',cast(hotel_id as string)) as search_hotel_id
  from `wego-cloud.ib_hotels.sessions*` s
  left join unnest(hotel_ids) hotel_id
  left join market_mapping mm
    on s.user_country_code = mm.user_country_code
  where _table_suffix between (select start_date2 from date_param) and (select end_date2 from date_param)
  qualify row_number()over(partition by search_id,hotel_id order by created_at desc) = 1
)

, hotels_clicks_pre as (
  select
    session_id
    , created_at
    , click_id
    , search_id
    , hotel_id
    , device
    , device_type
    , site_code
    , guests_count
    , rooms_count
    , lead_time
    , trip_duration
    , city_code
    , country_code
    , conversions_tracked as click_booked
    , provider_code as clicked_provider_code
  from `wego-cloud.wego_analytics.hotels_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

-- Distribution sessions: capture bookings from distribution channels that bypass meta clicks
, distribution_sessions as (
  select
    hs.session_id as click_id
    , hs.search_id
    , hs.hotel_id
    , coalesce(hs.device, wa.device) as device
    , coalesce(hs.device_type, wa.device_type) as device_type
    , coalesce(hs.site_code, wa.site_code) as site_code
    , hs.guests_count
    , hs.rooms_count
    , hs.lead_time
    , hs.trip_duration
    , hm.city_code
    , hm.country_code
    , wa.user_country_code
    , wa.ts_code
    , wa.market
    , if(hb.booking_id is not null, 1, 0) as click_booked
    , cast(null as string) as clicked_provider_code
  from `wego-cloud.wego_analytics.hotels_searches` hs
  inner join (
    select * from `wego-cloud.wego_analytics.sessions`
    where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
  ) wa
    on hs.session_id = wa.session_id
  left join hotel_mapping hm
    on hs.hotel_id = hm.hotel_id
  left join (
    select
      session_id
      , booking_id
      , created_at
    from `wego-cloud.wego_analytics.hotels_bookings`
    where date(created_at) between (select start_date from date_param) and (select end_date from date_param)
    qualify row_number() over (partition by session_id order by created_at) = 1
  ) hb
    on hs.session_id = hb.session_id
  where date(hs._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and wa.ts_code in ('6a97e','2f2fc','27a46','6be92')
  qualify row_number() over (partition by hs.session_id, hs.search_id, hs.hotel_id order by hs.created_at desc) = 1
)

, hotels_clicks as (
  select
    click_id
    , coalesce(ss.search_id,cl.search_id) as search_id
    , coalesce(ss.hotel_id,cl.hotel_id) as hotel_id
    , coalesce(ss.device,cl.device) as device
    , coalesce(ss.device_type,cl.device_type) as device_type
    , coalesce(ss.site_code,cl.site_code) as site_code
    , guests_count
    , rooms_count
    , lead_time
    , trip_duration
    , coalesce(hm.city_code,hm2.city_code) as city_code
    , coalesce(hm.country_code,hm2.country_code) as country_code
    , ss.user_country_code
    , ss.ts_code
    , ss.market
    , click_booked
    , clicked_provider_code
  from hotels_clicks_pre as cl
  full outer join sessions as ss
    on cl.search_id = ss.search_id
    and cl.hotel_id = ss.hotel_id
  left join hotel_mapping as hm
    on cl.hotel_id = hm.hotel_id
  left join hotel_mapping as hm2
    on ss.hotel_id = hm2.hotel_id
  qualify row_number()over(partition by coalesce(ss.search_id,cl.search_id),coalesce(ss.hotel_id,cl.hotel_id) order by click_booked desc) = 1

  union all

  select
    click_id
    , search_id
    , hotel_id
    , device
    , device_type
    , site_code
    , guests_count
    , rooms_count
    , lead_time
    , trip_duration
    , city_code
    , country_code
    , user_country_code
    , ts_code
    , market
    , click_booked
    , clicked_provider_code
  from distribution_sessions
)

-- select count(*) from hotels_clicks

, wegorates as (
  select
      * except(final_price)
    , coalesce(final_price, 0) + coalesce(additional_charges_amount, 0) as final_price
  from `wego-cloud.ib_hotels.wegorates*`
  where _table_suffix between (select start_date2 from date_param) and (select end_date2 from date_param)
  qualify row_number() over (
    partition by search_id, hotel_id, supplier_code
    order by coalesce(final_price, 0) + coalesce(additional_charges_amount, 0)
         , coalesce(final_price, 0)
  ) = 1
)

-- NEW: mirrors wegorates above, but collapses each supplier's multiple room types down to ONE
-- row by original_price (pre-markup) instead of final_price. Needed because a supplier's cheapest
-- room type by final_price isn't guaranteed to be the same room type that's cheapest by
-- original_price, if markup varies by room type/rule — so the original_price comparison needs its
-- own dedup, not a reuse of the final_price-deduped wegorates rows.
, wegorates_original as (
  select
      search_id
    , hotel_id
    , supplier_code
    , supplier_original_price + coalesce(tax_amount, 0) as original_price
  from `wego-cloud.ib_hotels.wegorates*`
  where _table_suffix between (select start_date2 from date_param) and (select end_date2 from date_param)
  qualify row_number() over (
    partition by search_id, hotel_id, supplier_code
    order by (supplier_original_price + coalesce(tax_amount, 0))
  ) = 1
)

, cheapest_supplier_table as (
  select
      concat(search_id, " - ", hotel_id) as search_hotel_id
    , first_value(supplier_code) over (
        partition by concat(search_id, " - ", hotel_id)
        order by coalesce(final_price, 0) + coalesce(additional_charges_amount, 0)
            , coalesce(final_price, 0)
      ) as cheapest_supplier
    , first_value(final_price) over (
        partition by concat(search_id, " - ", hotel_id)
        order by coalesce(final_price, 0) + coalesce(additional_charges_amount, 0)
            , coalesce(final_price, 0)
      ) as cheapest_supplier_rate
    , first_value(final_price) over (
        partition by concat(search_id, " - ", hotel_id)
        order by coalesce(final_price, 0) + coalesce(additional_charges_amount, 0)
            , coalesce(final_price, 0)
      ) as cheapest_supplier_price
  from wegorates
)

-- NEW: per search+hotel, the minimum original_price across Wego's own suppliers. Split out as its
-- own CTE (rather than computed inline) because BigQuery doesn't allow nesting one window function
-- inside another — this is needed as a plain aggregate before the tie count below can reference it.
, wegorates_original_min as (
  select
      search_id
    , hotel_id
    , min(original_price) as min_original_price
  from wegorates_original
  group by 1, 2
)

-- NEW: mirrors cheapest_supplier_table above, but ranks Wego's own suppliers by their pre-markup
-- original_price instead of final_price — answers "which supplier actually had the best raw rate,
-- independent of whatever markup rule got applied to each one." Sources from wegorates_original
-- (deduped by original_price), not wegorates, so the room-type collapse is consistent with the
-- price basis being compared.
, cheapest_supplier_table_original as (
  select
      concat(r.search_id, " - ", r.hotel_id) as search_hotel_id
    , first_value(r.supplier_code) over (
        partition by concat(r.search_id, " - ", r.hotel_id)
        order by r.original_price
      ) as cheapest_supplier_original
    -- The actual price tied to cheapest_supplier_original — needed so the provider-side win
    -- comparison uses Wego's BEST original_price for the search+hotel, not each individual
    -- supplier row's own price (which would give a different, inconsistent answer per supplier).
    , first_value(r.original_price) over (
        partition by concat(r.search_id, " - ", r.hotel_id)
        order by r.original_price
      ) as cheapest_supplier_original_price
    -- NEW: how many of Wego's own suppliers are tied at the cheapest original_price for this
    -- search+hotel. 1 = a single clear cheapest supplier; >1 = a genuine price tie with no
    -- deterministic winner today (first_value above picks one arbitrarily on ties). Purely
    -- descriptive — does not change cheapest_supplier_original itself or any existing filtering/
    -- qualify logic downstream.
    , count(case when r.original_price = m.min_original_price then 1 end) over (
        partition by concat(r.search_id, " - ", r.hotel_id)
      ) as suppliers_tied_at_cheapest_original_price
  from wegorates_original as r
  left join wegorates_original_min as m
    on r.search_id = m.search_id and r.hotel_id = m.hotel_id
)

, pricing_table_pre_without_window_function as (
  select distinct
      date(timestamp_add(coalesce(r.created_at, a.created_at), interval 8 hour)) as date
    , timestamp_add(coalesce(r.created_at, a.created_at), interval 8 hour) as created_at
    , r.search_id
    , r.hotel_id
    , supplier_channel
    , concat(r.search_id, " - ", r.hotel_id) as search_hotel_id
    , a.rate_id
    , room_type_id
    , strikethrough_price
    , supplier_min_price
    , flow_type
    , supplier_base_price
    , tax_amount
    , rule_id
    , markup_id
    , min_markup_percentage
    , max_markup_percentage
    , initial_markup_percentage
    , action
    , final_markup_percentage
    , final_markup_amount
    , final_price
    , supplier_code
    , amount_per_night_usd as cheapest_provider_amount_per_night_usd
    , total_amount_usd as cheapest_provider_total_amount_usd
    , cheapest_provider
    , cheapest_provider_name
    , round(final_price, 0) - total_amount_usd as cheapest_provider_price_diff_usd
    , round((safe_divide(round(final_price, 0) , total_amount_usd) - 1) * 100, 2) as cheapest_provider_price_diff_percentage
    , prebook_id
    , markup_type
    , pre_price_matched_markup_type
    , pre_price_matched_final_amount as pre_price_matched_price
    , pre_price_matched_final_markup_amount as pre_price_matched_markup_amount
    , pre_price_matched_final_markup_percentage as pre_price_matched_markup_percentage
    , coalesce(displayed_price_amount, displayed_price.amount) as price_matched_display_price
    , coalesce(displayed_price_loss_threshold_amount, displayed_price.loss_threshold_amount) as price_matched_loss_threshold_amount
    , displayed_price.supplier_code as price_matched_supplier_code
    , displayed_price.supplier_rate_created_at as price_matched_supplier_rate_created_at
    , if(
        floor(coalesce(displayed_price_amount, displayed_price.amount)) = floor(coalesce(final_price, 0) + coalesce(additional_charges_amount, 0))
      , "price matched"
      , "price did not meet tolerance"
      ) as price_matching_status
    , pre_price_matched_final_amount - coalesce(displayed_price_amount, displayed_price.amount) as price_matched_price_accuracy
    , price_info_by_markup_type.historical.final_price as historical_final_price
    , price_info_by_markup_type.displayed_price_matching.final_price as display_price_final_price
    , price_info_by_markup_type.supplier_config.final_price as supplier_config_final_price
    , round(
        (safe_divide(round(price_info_by_markup_type.historical.final_price, 0) , total_amount_usd) - 1) * 100
      , 2
      ) as historical_cheapest_provider_price_diff_percentage
    -- NEW: pre-markup net rate the guest would pay with no Wego markup applied. Per the RateLog Field
    -- Reference (Confluence HIB-3716317185) and the validated supplier-price-competitiveness basis:
    -- supplier_original_price is the nett price BEFORE markup; supplier_base_price is already
    -- supplier_original_price + final_markup_amount (markup already baked in), so using
    -- supplier_base_price here (as an earlier version of this patch did, copying the raw table's
    -- legacy formula) would have been circular. Deliberately excludes marketing_fee (a revenue/
    -- commission item, not a price the guest pays) — same basis validated in the hotel-midyear-review
    -- price-competitiveness analysis. Used below for the supplier-side original_price comparison —
    -- NOT for a provider-side (external competitor) comparison, since Hotels is an OTA now, not a
    -- metasearch; Akasha's rates table only ever contains hotels.wego.com, so any "cheapest external
    -- provider" comparison is structurally meaningless here, not just currently broken.
    , supplier_original_price + coalesce(tax_amount, 0) as original_price
  from wegorates as r
  left join rates_base as a
    on concat(r.search_id, " - ", r.hotel_id) = a.search_hotel_id
)

-- step 4: enrich with cheapest supplier comparison
, pricing_table_pre as (
  select
      a.*
    , b.cheapest_supplier
    , b.cheapest_supplier_rate
    , final_price - b.cheapest_supplier_price as cheapest_supplier_price_diff_usd
    , round((safe_divide(final_price , b.cheapest_supplier_price) - 1) * 100, 2) as cheapest_supplier_price_diff_percentage
    -- NEW: pre-markup counterparts to cheapest_supplier/cheapest_supplier_rate, appended at the
    -- end so column position stays backward-compatible.
    , c.cheapest_supplier_original
    , c.cheapest_supplier_original_price
    -- NEW: pre-markup counterpart to cheapest_supplier_price_diff_usd/percentage above — mirrors
    -- the exact same shape, just original_price vs. cheapest_supplier_original_price instead of
    -- final_price vs. cheapest_supplier_price. No rate_is_cheapest_supplier filter here, matching
    -- the unfiltered pattern of the final_price version (it's meant to cover every row, winner or not).
    , a.original_price - c.cheapest_supplier_original_price as original_cheapest_supplier_price_diff_usd
    , round((safe_divide(a.original_price, c.cheapest_supplier_original_price) - 1) * 100, 2) as original_cheapest_supplier_price_diff_percentage
    -- NEW: tie visibility, carried straight through from cheapest_supplier_table_original — see
    -- that CTE for the definition. original_price_is_tied is just a >1 convenience flag on top.
    , c.suppliers_tied_at_cheapest_original_price
    , if(c.suppliers_tied_at_cheapest_original_price > 1, 1, 0) as original_price_is_tied
  from pricing_table_pre_without_window_function as a
  left join (
      select
          search_hotel_id
        , cheapest_supplier
        , cheapest_supplier_rate
        , cheapest_supplier_price
      from cheapest_supplier_table
      group by 1, 2, 3, 4
  ) as b
    on a.search_hotel_id = b.search_hotel_id
  left join (
      select distinct
          search_hotel_id
        , cheapest_supplier_original
        , cheapest_supplier_original_price
        , suppliers_tied_at_cheapest_original_price
      from cheapest_supplier_table_original
  ) as c
    on a.search_hotel_id = c.search_hotel_id
)

-- step 5: add search-click context
, pricing_table as (
  select
      a.*
    , click_id
    , device
    , device_type
    , site_code
    , guests_count
    , c.rooms_count
    , lead_time
    , trip_duration
    , city_code
    , country_code
    , if(country_code = user_country_code, 'domestic', 'international') as trip_category
    , user_country_code
    , market
    , ts_code
    , click_booked
    , clicked_provider_code
    , if(click_id is not null, 1, 0) as fare_clicked
    , case
          when click_id is not null then 'fare clicked'
          when click_booked > 0 then 'fare booked'
          else 'fare searched'
      end as fare_status
  from pricing_table_pre as a
  left join hotels_clicks as c
    on concat(c.search_id, ' - ', c.hotel_id) = a.search_hotel_id
)


-- step 6: final feature engineering
, final as (
  select distinct
      * except(date,site_code)
    , if(supplier_code = cheapest_supplier, 1, 0) as rate_is_cheapest_supplier
    , if(cheapest_provider = 'hotels.wego.com', 1, 0) as rate_is_cheapest_provider
    , if(clicked_provider_code = 'hotels.wego.com', 1, 0) as rate_is_clicked_provider
    , if(cheapest_provider_total_amount_usd is null, 'only provider', 'muiltiple providers') as provider_count_status
    , min(final_price) over (partition by search_hotel_id) as cheapest_final_price_display_price_matching
    , min(pre_price_matched_price) over (partition by search_hotel_id) as cheapest_pre_price_matched_price
    , min(date) over(partition by search_hotel_id) as date
    , min(site_code) over(partition by search_hotel_id) as site_code
    , case when row_number() over (partition by search_hotel_id order by if(supplier_code = cheapest_supplier, 1, 0) desc) = 1 then 1 else 0 end as deduped_search_flag
    , case when row_number() over (partition by search_hotel_id, click_id order by  if(clicked_provider_code = 'hotels.wego.com', 1, 0) desc, if(cheapest_provider = 'hotels.wego.com', 1, 0) desc) = 1 then 1 else 0 end as deduped_click_flag
    -- NEW: same "was this row's supplier the cheapest of Wego's own suppliers" comparison as
    -- rate_is_cheapest_supplier, but using cheapest_supplier_original (ranked by pre-markup
    -- original_price) instead of cheapest_supplier (ranked by final_price).
    , if(supplier_code = cheapest_supplier_original, 1, 0) as rate_is_cheapest_supplier_original
  from pricing_table
  qualify row_number() over(partition by search_hotel_id,room_type_id order by if(supplier_code = cheapest_supplier, 1, 0) desc) = 1
)

-- select * from final

, aggregated as
(
  select
    date
    , timestamp(date) as created_at
    , device
    , device_type
    , site_code
    , city_code
    , country_code

    , trip_category
    , user_country_code
    , market
    , ts_code
    , flow_type
    , action
    , supplier_code
    , cheapest_supplier
    , cheapest_provider
    , cheapest_provider_name
    , clicked_provider_code
    , fare_status
    , markup_type
    , pre_price_matched_markup_type
    , price_matched_supplier_code
    , price_matching_status
    , provider_count_status
    , supplier_channel
    , case when guests_count between 1 and 2 then '1-2 guests'
            when guests_count between 3 and 4 then '3-4 guests'
            when guests_count between 5 and 6 then '5-6 guests'
            when guests_count >= 6 then '>=6 guests'
            else cast(guests_count as string) end as guest_count_group
    , case when rooms_count = 1 then '1 room'
            when rooms_count = 2 then '2 rooms'
            when rooms_count >= 3 then '>=3 rooms'
            else cast(rooms_count as string) end as rooms_count_group
    , case when lead_time = 0 then 'Same day'
            when lead_time = 1 then 'Next day'
            when lead_time between 2 and 3 then 'D + 2-3'
            when lead_time between 4 and 7 then 'D + 4-7'
            when lead_time between 8 and 14 then 'D + 8-14'
            when lead_time between 15 and 30 then 'D + 15-30'
            when lead_time > 30 then 'Day + >30'
            else cast(lead_time as string) end as lead_time_group
    , case when trip_duration = 1 then '1 night'
            when trip_duration = 2 then '2 nights'
            when trip_duration = 3 then '3 nights'
            when trip_duration between 4 and 7 then '4-7 nights'
            when trip_duration between 8 and 14 then '8-14 nights'
            when trip_duration between 15 and 30 then '15-30 nights'
            when trip_duration > 30 then '>30 nights'
            else cast(trip_duration as string) end as trip_duration_group
    , rule_id
    , markup_id
    , deduped_search_flag
    , deduped_click_flag
    , count(distinct search_id) as total_searches
    , count(distinct search_hotel_id) as total_search_hotel_results
    , count(distinct click_id) as total_clicks
    , count(distinct concat(click_id,search_hotel_id)) as total_search_hotel_clicks
    , count(distinct concat(search_hotel_id,room_type_id)) as total_search_rates
    , count(distinct concat(rate_id,supplier_code)) as total_supplier_rates
    , count(distinct rate_id) as total_unique_rates
    , count(distinct case when rate_is_cheapest_provider = 1 then search_hotel_id end) as total_price_win
    , count(distinct case when rate_is_cheapest_supplier = 1 then search_hotel_id end) as total_supplier_win
    , count(distinct case when rate_is_clicked_provider = 1 then search_hotel_id end) as total_click_win
    , count(distinct case when rule_id is not null then search_hotel_id end) as total_hotel_searches_with_rule
    , count(distinct case when lower(action) in ('markup','markdown') then search_hotel_id end) as total_hotel_searches_with_action

    , count(distinct case
        when price_matched_display_price = cheapest_pre_price_matched_price
          and price_matched_display_price = cheapest_final_price_display_price_matching then search_hotel_id end) as total_hotel_search_exact_match
    , count(distinct case when price_matched_display_price > cheapest_pre_price_matched_price
          and price_matched_display_price = cheapest_final_price_display_price_matching then search_hotel_id end) as total_hotel_search_markup_to_match
    , count(distinct case when price_matched_display_price < cheapest_pre_price_matched_price
          and price_matched_display_price = cheapest_final_price_display_price_matching then search_hotel_id end) as total_hotel_search_markdown_to_match
    , count(distinct case when price_matched_display_price < cheapest_final_price_display_price_matching then search_hotel_id end) as total_hotel_search_not_within_tolerance_price_matching
    , count(distinct case when price_matched_display_price > cheapest_final_price_display_price_matching then search_hotel_id end) as total_hotel_search_price_lower_than_google_rate
    , count(distinct case when click_booked = 1 then click_id end) as total_bookings

    , sum(cheapest_final_price_display_price_matching) as total_cheapest_final_price_display_price_matching
    , sum(strikethrough_price) as total_strikethrough_price_usd
    , sum(supplier_min_price) as total_supplier_min_price_usd
    , sum(supplier_base_price) as total_supplier_base_price_usd
    , sum(initial_markup_percentage) as total_initial_markup_percentage
    , sum(min_markup_percentage) as total_min_markup_percentage
    , sum(max_markup_percentage) as total_max_markup_percentage
    , sum(final_markup_percentage) as total_final_markup_percentage
    , sum(tax_amount) as total_tax_usd
    , sum(final_markup_amount) as total_final_markup_usd
    , sum(final_price) as total_final_price
    , sum(cheapest_supplier_rate) as total_cheapest_supplier_rate
    , sum(cheapest_supplier_price_diff_usd) as total_cheapest_supplier_price_diff_usd
    , sum(cheapest_supplier_price_diff_percentage) as total_cheapest_supplier_price_diff_percentage
    , sum(cheapest_provider_amount_per_night_usd) as total_cheapest_provider_amount_per_night_usd
    , sum(cheapest_provider_total_amount_usd) as total_cheapest_provider_total_amount_usd
    , sum(cheapest_provider_price_diff_usd) as total_cheapest_provider_price_diff_usd
    , sum(case when rate_is_cheapest_supplier = 1 then cheapest_provider_price_diff_percentage else 0 end) as total_cheapest_provider_price_diff_percentage
    , min(case when rate_is_cheapest_supplier = 1 then cheapest_provider_price_diff_percentage end) as min_cheapest_provider_price_diff_percentage
    , max(case when rate_is_cheapest_supplier = 1 then cheapest_provider_price_diff_percentage end) as max_cheapest_provider_price_diff_percentage
    , sum(fare_clicked) as total_fare_clicked
    , sum(pre_price_matched_price) as total_pre_price_matched_price
    , sum(pre_price_matched_markup_amount) as total_pre_price_matched_markup_amount
    , sum(pre_price_matched_markup_percentage) as total_pre_price_matched_markup_percentage
    , sum(price_matched_display_price) as total_price_matched_display_price
    , sum(price_matched_loss_threshold_amount) as total_price_matched_loss_threshold_amount
    , sum(price_matched_price_accuracy) as total_price_matched_price_accuracy
    , sum(historical_cheapest_provider_price_diff_percentage) as total_historical_cheapest_provider_price_diff_percentage
    , sum(historical_final_price) as total_historical_final_price
    , sum(display_price_final_price) as total_display_price_final_price
    , sum(supplier_config_final_price) as total_supplier_config_final_price
    , sum(cheapest_pre_price_matched_price) as total_cheapest_pre_price_matched_price

    , count(cheapest_final_price_display_price_matching) as count_cheapest_final_price_display_price_matching
    , count(strikethrough_price) as count_strikethrough_price_usd
    , count(supplier_min_price) as count_supplier_min_price_usd
    , count(supplier_base_price) as count_supplier_base_price_usd
    , count(initial_markup_percentage) as count_initial_markup_percentage
    , count(min_markup_percentage) as count_min_markup_percentage
    , count(max_markup_percentage) as count_max_markup_percentage
    , count(final_markup_percentage) as count_final_markup_percentage
    , count(tax_amount) as count_tax_usd
    , count(final_markup_amount) as count_final_markup_usd
    , count(final_price) as count_final_price
    , count(cheapest_supplier_rate) as count_cheapest_supplier_rate
    , count(cheapest_supplier_price_diff_usd) as count_cheapest_supplier_price_diff_usd
    , count(cheapest_supplier_price_diff_percentage) as count_cheapest_supplier_price_diff_percentage
    , count(cheapest_provider_amount_per_night_usd) as count_cheapest_provider_amount_per_night_usd
    , count(cheapest_provider_total_amount_usd) as count_cheapest_provider_total_amount_usd
    , count(cheapest_provider_price_diff_usd) as count_cheapest_provider_price_diff_usd
    , count(case when rate_is_cheapest_supplier = 1 then cheapest_provider_price_diff_percentage else 0 end) as count_cheapest_provider_price_diff_percentage
    , count(click_booked) as count_bookings
    , count(fare_clicked) as count_fare_clicked
    , count(pre_price_matched_price) as count_pre_price_matched_price
    , count(pre_price_matched_markup_amount) as count_pre_price_matched_markup_amount
    , count(pre_price_matched_markup_percentage) as count_pre_price_matched_markup_percentage
    , count(price_matched_display_price) as count_price_matched_display_price
    , count(price_matched_loss_threshold_amount) as count_price_matched_loss_threshold_amount
    , count(price_matched_price_accuracy) as count_price_matched_price_accuracy
    , count(historical_cheapest_provider_price_diff_percentage) as count_historical_cheapest_provider_price_diff_percentage
    , count(historical_final_price) as count_historical_final_price
    , count(display_price_final_price) as count_display_price_final_price
    , count(supplier_config_final_price) as count_supplier_config_final_price
    , count(cheapest_pre_price_matched_price) as count_cheapest_pre_price_matched_price

    -- NEW: original_price (pre-markup) fields, all appended at the end of the SELECT so the
    -- destination table's existing column order/schema stays backward-compatible for WRITE_APPEND.
    -- Supplier-side only — no provider-side (external competitor) fields, since Hotels is an OTA
    -- now and Akasha's rates table only ever contains hotels.wego.com; that comparison is
    -- structurally not applicable, not just currently broken.
    , count(distinct case when rate_is_cheapest_supplier_original = 1 then search_hotel_id end) as total_supplier_win_original
    , sum(original_price) as total_original_price
    , count(original_price) as count_original_price
    -- NEW: pre-markup counterpart to total_cheapest_supplier_price_diff_usd/percentage — same
    -- unfiltered pattern (no rate_is_cheapest_supplier condition), matching how the final_price
    -- version is built.
    , sum(original_cheapest_supplier_price_diff_usd) as total_original_cheapest_supplier_price_diff_usd
    , count(original_cheapest_supplier_price_diff_usd) as count_original_cheapest_supplier_price_diff_usd
    , sum(original_cheapest_supplier_price_diff_percentage) as total_original_cheapest_supplier_price_diff_percentage
    , count(original_cheapest_supplier_price_diff_percentage) as count_original_cheapest_supplier_price_diff_percentage
    -- NEW: tie visibility (Tier 1) — count of search+hotel combos where the cheapest original_price
    -- was a genuine tie between two or more of Wego's own suppliers. Does not change
    -- total_supplier_win_original or any existing win/loss counting — purely descriptive, so
    -- reporting can choose to caveat or exclude ties without a pipeline change later.
    , count(distinct case when original_price_is_tied = 1 then search_hotel_id end) as total_search_hotel_original_tied
    from final
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
)

select * from aggregated
{% endraw %}
