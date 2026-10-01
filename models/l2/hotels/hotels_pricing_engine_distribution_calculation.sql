{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_pricing_engine_distribution_calculation
-- Destination: pricing_engine.hotels_pricing_engine_distribution_calculation  (unchanged)
-- Schedule   : every day 01:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- CREATE OR REPLACE TABLE `wego-cloud.pricing_engine.hotels_pricing_engine_distribution_calculation`
-- PARTITION BY DATE(created_at) as

with pricing_rules as
(
  select
    trim(replace(ts_code, '"', '')) as ts_code
    , nullif(trim(replace(ts_code_excluded, '"', '')), '') as ts_code_excluded
    , nullif(trim(replace(device_types_included, '"', '')), '') as device_types_included
    , nullif(trim(replace(user_country_codes_included, '"', '')), '') as user_country_codes_included
    , nullif(trim(replace(user_country_codes_excluded, '"', '')), '') as user_country_codes_excluded
    , nullif(trim(replace(destination_country_codes_included, '"', '')), '') as destination_country_codes_included
    , nullif(trim(replace(destination_country_codes_excluded, '"', '')), '') as destination_country_codes_excluded
    , nullif(trim(replace(city_codes_included, '"', '')), '') as city_codes_included
    , nullif(trim(replace(city_codes_excluded, '"', '')), '') as city_codes_excluded
    , json_extract_scalar(json_element, '$.min_markup') AS min_markup
    , json_extract_scalar(json_element, '$.max_markup') AS max_markup
    , id
    , priority
  from  `wego-cloud.hotel_services.pricing_rules` 
  left join unnest(split(trim(ts_codes_included, '[]'), '.')) as ts_code
  left join unnest(split(trim(ts_codes_excluded, '[]'), ',')) as ts_code_excluded
  left join unnest(split(trim(device_types_included, '[]'), ',')) as device_types_included
  left join unnest(split(trim(user_country_codes_included, '[]'), ',')) as user_country_codes_included
  left join unnest(split(trim(destination_country_codes_included, '[]'), ',')) as destination_country_codes_included
  left join unnest(split(trim(destination_country_codes_excluded, '[]'), ',')) as destination_country_codes_excluded
  left join unnest(split(trim(user_country_codes_excluded, '[]'), ',')) as user_country_codes_excluded
  left join unnest(split(trim(city_codes_included, '[]'), ',')) as city_codes_included
  left join unnest(split(trim(city_codes_excluded, '[]'), ',')) as city_codes_excluded
  left join unnest(json_extract_array(markup_ranges)) as json_element
  where ts_codes_included is not null
  and ts_code <> '*'
  and enabled
  and cast(json_extract_scalar(json_element, '$.min_markup') as float64) > 0
  and json_extract(json_element, '$.suppliers_included') = '["*"]'
)


-- select * from pricing_rules
-- where ts_code = '27a46'

, guardrail as
(
  select 
    *
    , row_number()over(partition by ts_code, device_types_included order by priority, user_country_codes_included) as rule_priority 
  from
  (
    select
      * except(min_markup,max_markup,device_types_included)
      , case when device_types_included = 'DESKTOP' then 'desktop-web'
            when device_types_included = 'MOBILE_APP' then 'android-app'
            when device_types_included = 'MOBILE_WEB' then 'smartphone-web'
        end as device_types_included
      , min(cast(min_markup as float64)) as min_markup 
      , max(cast(max_markup as float64)) as max_markup
    from pricing_rules 
    group by 1,2,3,4,5,6,7,8,9,10,11

    union all

    select
      * except(min_markup,max_markup,device_types_included)
      , 'tablet-web' device_types_included
      , min(cast(min_markup as float64)) as min_markup 
      , max(cast(max_markup as float64)) as max_markup
    from pricing_rules 
    where device_types_included = 'MOBILE_WEB' 
    group by 1,2,3,4,5,6,7,8,9,10,11

    union all

    select
      * except(min_markup,max_markup,device_types_included)
      , 'ios-app' device_types_included
      , min(cast(min_markup as float64)) as min_markup 
      , max(cast(max_markup as float64)) as max_markup
    from pricing_rules 
    where device_types_included = 'MOBILE_APP' 
    group by 1,2,3,4,5,6,7,8,9,10,11
  )
)

-- select * from guardrail
-- where ts_code = '27a46'

, sessions as
(
  select distinct
    ts_code
    , user_country_code
    , session_id -- cvr book/session
    , device_type
    , coalesce(
        regexp_extract(landing_url,r'wego_hotel_id=([^&]+)'),regexp_extract(landing_url,r'/(\d+)(?:/rooms|\?|$)')
      ) as hotel_id
  from `wego_analytics.sessions`
  where date(timestamp_trunc(_partitiontime, day)) >= current_date - interval '7' day
  and ts_code is not null
  and channel = 'distribution'
)

/*
===== SKYSCANNER LOGIC ====
*/

, skyscanner_report as
(
  select 
    partner_property_id as hotel_id
    , if(split(campaign_name, "-")[offset(0)] = 'UK','GB',split(campaign_name, "-")[offset(0)]) as user_country_code
    , case 
        when split(campaign_name, "-")[offset(1)] = 'APP' then 'android-app'
        when split(campaign_name, "-")[offset(1)] = 'DESKTOP' then 'desktop-web'
      end as device_type
    , sum(price_difference_gbp_pence_avg) as price_difference_gbp_pence_avg
    , sum(hotel_impressions) as hotel_impressions
    , sum(base_price_gbp_pence_avg) as base_price_gbp_pence_avg
    , sum(taxes_fees_gbp_pence_avg) as taxes_fees_gbp_pence_avg
  from `distribution_partner_reports_hotels.skyscanner_auction_insights_report`
  where date(timestamp_trunc(_partitiontime, day)) >= current_date - interval '7' day
  and display_rank > 0
  group by 1,2,3
)

, skyscanner_detail as
(
  select distinct
    *
    , c.code as hotel_city_code
    , d.code as hotel_country_code
  from skyscanner_report a
  left join   `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id 
)

, skyscanner_performance as
(
  select
    '6be92' as ts_code
    , hotel_id
    , hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , sum(hotel_impressions) as total_impressions
    , sum((price_difference_gbp_pence_avg * hotel_impressions) 
          / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
        / sum(hotel_impressions) as avg_price_diff
  from skyscanner_detail
  group by 1,2,3,4,5,6
  having sum(hotel_impressions) > 5

  union all

  select
    '6be92' as ts_code
    , cast(null as string) as hotel_id
    , hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , sum(hotel_impressions) as total_impressions
    , sum((price_difference_gbp_pence_avg * hotel_impressions) 
          / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
        / sum(hotel_impressions) as avg_price_diff
  from skyscanner_detail
  group by 1,2,3,4,5,6
  having sum(hotel_impressions) > 10

   union all

  select
    '6be92' as ts_code
    , cast(null as string) as hotel_id
    , cast(null as string) as hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , sum(hotel_impressions) as total_impressions
    , sum((price_difference_gbp_pence_avg * hotel_impressions) 
          / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
        / sum(hotel_impressions) as avg_price_diff
  from skyscanner_detail
  group by 1,2,3,4,5,6
  having sum(hotel_impressions) > 10

   union all

  select
    '6be92' as ts_code
    , cast(null as string) as hotel_id
    , cast(null as string) as hotel_city_code
    , cast(null as string) as hotel_country_code
    , device_type
    , user_country_code
    , sum(hotel_impressions) as total_impressions
    , sum((price_difference_gbp_pence_avg * hotel_impressions) 
          / (base_price_gbp_pence_avg + taxes_fees_gbp_pence_avg - price_difference_gbp_pence_avg)) * 100 
        / sum(hotel_impressions) as avg_price_diff
  from skyscanner_detail
  group by 1,2,3,4,5,6
  having sum(hotel_impressions) > 15
)

, skyscanner_pricing_scores as
(
  select
    * except(avg_price_diff)
    , avg_price_diff as d
    , 1 / (1 + exp(0.2 * greatest(-50, least(50, avg_price_diff)))) as pricing_score
  from skyscanner_performance
  where hotel_country_code is not null
  and device_type is not null
)

/*
===== VIO LOGIC ====
*/

, vio_session as
(
  select
    *
  from sessions
  where  ts_code = '27a46'
)

, session_detail as
(
  select
    a.*
    , c.code as hotel_city_code
    , d.code as hotel_country_code
  from vio_session a
  left join   `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
)

, vio_booking as
(
  select
    cast(hotel_id as string) as hotel_id
    , booking_id
    , session_id
  from `wego_analytics.hotels_bookings`
  where date(booking_at) >= current_date - interval '7' day
  and attribution_ts_code = '27a46'
  and conversions_adjusted > 0
)

, vio_funnel as
(
  select
    a.*
    , booking_id
  from session_detail a
  left join vio_booking b
    using(hotel_id,session_id)
)

, vio_performance as
(
  select
    ts_code
    , hotel_id
    , hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , count(distinct session_id) as total_impressions
    , least(safe_divide(count(distinct booking_id),count(distinct session_id)),1) as cvr
    , percent_rank()over(partition by hotel_country_code order by count(distinct session_id)) as percentile_click
  from vio_funnel
  group by 1,2,3,4,5,6

  union all

  select
    ts_code
    , cast(null as string) as hotel_id
    , hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , count(distinct session_id) as total_impressions
    , least(safe_divide(count(distinct booking_id),count(distinct session_id)),1) as cvr
    , percent_rank()over(order by count(distinct session_id)) as percentile_click
  from vio_funnel
  group by 1,2,3,4,5,6
  having count(distinct session_id) > 5
  
  union all

  select
    ts_code
    , cast(null as string) as hotel_id
    , cast(null as string) hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , count(distinct session_id) as total_impressions
    , least(safe_divide(count(distinct booking_id),count(distinct session_id)),1) as cvr
    , percent_rank()over( order by count(distinct session_id)) as percentile_click
  from vio_funnel
  group by 1,2,3,4,5,6
  having count(distinct session_id) > 10

   union all

  select
    ts_code
    , cast(null as string) as hotel_id
    , cast(null as string) as hotel_city_code
    , hotel_country_code
    , device_type
    , cast(null as string) as user_country_code 
    , count(distinct session_id) as total_impressions
    , least(safe_divide(count(distinct booking_id),count(distinct session_id)),1) as cvr
    , percent_rank()over(order by count(distinct session_id)) as percentile_click
  from vio_funnel
  group by 1,2,3,4,5,6
  having count(distinct session_id) > 15
)

, vio_pricing_scores as
(
  -- select 
  --   * except(cvr,cvr_log,min_log,max_log)
  --   , cvr as d
  --   , (cvr_log - min_log) / (max_log - min_log) AS pricing_score
  -- from(
  --   -- normalized using log due to skewed distribution
  --   select
  --     *
  --     , ln(case when cvr = 0 then 0.001 else cvr end) as cvr_log
  --     , min(ln(case when cvr = 0 then 0.001 else cvr end)) over () as min_log
  --     , max(ln(case when cvr = 0 then 0.001 else cvr end)) over () as max_log
  --   from vio_performance
  --   where hotel_country_code is not null
  --   and device_type is not null
  select 
    * except(cvr,percentile_click)
    , cvr as d
    , (cvr*0.7) + (percentile_click*0.3) as pricing_score
  from vio_performance
    where hotel_country_code is not null
    and device_type is not null
)

/*
====== MARKUP CALCULATION =====
*/


/*
===== GOOGLE HOTELS LOGIC ====
*/

, google_htc_raw as
(
  select
    cast(hotel_id as string) as hotel_id
    , sum(competing_price_diff * impressions) / sum(impressions) as weighted_price_diff
    , sum(impressions) as total_impressions
  from `wego-cloud.google_hotel_centre.raw_google_htc_v2`
  where created_at >= current_date - interval '7' day
    and impressions > 0
  group by 1
)

, google_htc_detail as
(
  select
    a.hotel_id
    , a.weighted_price_diff
    , a.total_impressions
    , c.code as hotel_city_code
    , d.code as hotel_country_code
  from google_htc_raw a
  left join `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
)

, google_htc_rollup as
(
  select * from (
    -- city level
    select
      cast(null as string) as hotel_id
      , hotel_city_code
      , hotel_country_code
      , sum(total_impressions) as total_impressions
      , sum(weighted_price_diff * total_impressions) / sum(total_impressions) as weighted_price_diff
    from google_htc_detail
    where hotel_country_code is not null
      and hotel_city_code is not null
    group by hotel_city_code, hotel_country_code
  ) where total_impressions > 10

  union all

  select * from (
    -- country level
    select
      cast(null as string) as hotel_id
      , cast(null as string) as hotel_city_code
      , hotel_country_code
      , sum(total_impressions) as total_impressions
      , sum(weighted_price_diff * total_impressions) / sum(total_impressions) as weighted_price_diff
    from google_htc_detail
    where hotel_country_code is not null
    group by hotel_country_code
  ) where total_impressions > 15
)

-- distinct user countries covered by GH guardrail rules
, google_htc_user_countries as
(
  select distinct trim(replace(trim(uc), '"', '')) as user_country_code
  from `wego-cloud.hotel_services.pricing_rules`
  left join unnest(split(trim(ts_codes_included, '[]'), '.')) as ts_code
  left join unnest(split(trim(user_country_codes_included, '[]'), ',')) as uc
  where ts_codes_included like '%2f2fc%'
    and enabled
    and trim(replace(trim(uc), '"', '')) not in ('', '*')
)

-- distinct device types covered by GH guardrail rules (normalized)
, google_htc_devices as
(
  select distinct
    case when trim(replace(trim(dt), '"', '')) = 'DESKTOP'    then 'desktop-web'
         when trim(replace(trim(dt), '"', '')) = 'MOBILE_APP' then 'android-app'
         when trim(replace(trim(dt), '"', '')) = 'MOBILE_WEB' then 'smartphone-web'
         else null
    end as device_type
  from `wego-cloud.hotel_services.pricing_rules`
  left join unnest(split(trim(ts_codes_included, '[]'), '.')) as ts_code
  left join unnest(split(trim(device_types_included, '[]'), ',')) as dt
  where ts_codes_included like '%2f2fc%'
    and enabled
    and trim(replace(trim(dt), '"', '')) not in ('', '*')
)

, google_htc_performance as
(
  -- hotel level — fan out to each user country × device type so guardrail join matches
  select
    '2f2fc' as ts_code
    , p.hotel_id
    , p.hotel_city_code
    , p.hotel_country_code
    , d.device_type
    , u.user_country_code
    , p.total_impressions
    , p.weighted_price_diff as d
    , 1 / (1 + exp(0.2 * greatest(-50, least(50, p.weighted_price_diff * 100)))) as pricing_score
  from google_htc_detail p
  cross join google_htc_user_countries u
  cross join google_htc_devices d
  where p.hotel_country_code is not null
    and p.total_impressions > 5

  union all

  -- city + country rollup — same fan-out
  select
    '2f2fc' as ts_code
    , p.hotel_id
    , p.hotel_city_code
    , p.hotel_country_code
    , d.device_type
    , u.user_country_code
    , p.total_impressions
    , p.weighted_price_diff as d
    , 1 / (1 + exp(0.2 * greatest(-50, least(50, p.weighted_price_diff * 100)))) as pricing_score
  from google_htc_rollup p
  cross join google_htc_user_countries u
  cross join google_htc_devices d
)


/*
===== TRIVAGO LOGIC ====
*/

-- POS mapping: AA (GCC) fans out to 7 countries; UK remaps to GB
, trivago_pos_map as
(
  select pos_raw, user_country_code
  from unnest([
    struct('AA' as pos_raw, 'SA' as user_country_code)
    , struct('AA', 'AE')
    , struct('AA', 'OM')
    , struct('AA', 'BH')
    , struct('AA', 'QA')
    , struct('AA', 'EG')
    , struct('AA', 'KW')
    , struct('AA', 'JO')
    , struct('AE', 'AE')
    , struct('CA', 'CA')
    , struct('MY', 'MY')
    , struct('TR', 'TR')
    , struct('UK', 'GB')
    , struct('US', 'US')
  ])
)

-- impression-weighted avg price diff per hotel × POS (last 7 days)
, trivago_raw as
(
  select
    cast(partnerref as string) as hotel_id
    , POS as pos_raw
    , sum(`accommodation rate difference %` * `hotel impressions`) / sum(`hotel impressions`) as avg_price_diff
    , sum(`hotel impressions`) as total_impressions
  from `wego-cloud.distribution_partner_reports_hotels.trivago_rate_insights_report`
  where date(timestamp_trunc(_partitiontime, day)) >= current_date - interval '7' day
    and `accommodation rate difference %` is not null
    and `hotel impressions` > 0
  group by 1, 2
)

-- join POS map + geo (city/country codes)
, trivago_base as
(
  select
    a.hotel_id
    , m.user_country_code
    , a.avg_price_diff
    , a.total_impressions
    , c.code as hotel_city_code
    , d.code as hotel_country_code
  from trivago_raw a
  join trivago_pos_map m
    on a.pos_raw = m.pos_raw
  left join `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
)

-- device fan-out: trivago report has no device dimension
, trivago_devices as
(
  select device_type
  from unnest(['desktop-web','android-app','smartphone-web','tablet-web','ios-app']) as device_type
)

-- city + country rollups (subquery wrapper avoids agg-of-agg on pre-aggregated column)
, trivago_rollup as
(
  select * from (
    select
      cast(null as string) as hotel_id
      , hotel_city_code
      , hotel_country_code
      , user_country_code
      , sum(total_impressions) as total_impressions
      , sum(avg_price_diff * total_impressions) / sum(total_impressions) as avg_price_diff
    from trivago_base
    where hotel_country_code is not null
      and hotel_city_code is not null
    group by hotel_city_code, hotel_country_code, user_country_code
  ) where total_impressions > 10

  union all

  select * from (
    select
      cast(null as string) as hotel_id
      , cast(null as string) as hotel_city_code
      , hotel_country_code
      , user_country_code
      , sum(total_impressions) as total_impressions
      , sum(avg_price_diff * total_impressions) / sum(total_impressions) as avg_price_diff
    from trivago_base
    where hotel_country_code is not null
    group by hotel_country_code, user_country_code
  ) where total_impressions > 15
)

, trivago_performance as
(
  -- hotel level (fan out by device)
  select
    '6a97e' as ts_code
    , p.hotel_id
    , p.hotel_city_code
    , p.hotel_country_code
    , d.device_type
    , p.user_country_code
    , p.total_impressions
    , p.avg_price_diff as d
    , 1 / (1 + exp(0.2 * greatest(-50, least(50, p.avg_price_diff)))) as pricing_score
  from trivago_base p
  cross join trivago_devices d
  where p.hotel_country_code is not null
    and p.total_impressions > 5

  union all

  -- city + country rollup (fan out by device)
  select
    '6a97e' as ts_code
    , p.hotel_id
    , p.hotel_city_code
    , p.hotel_country_code
    , d.device_type
    , p.user_country_code
    , p.total_impressions
    , p.avg_price_diff as d
    , 1 / (1 + exp(0.2 * greatest(-50, least(50, p.avg_price_diff)))) as pricing_score
  from trivago_rollup p
  cross join trivago_devices d
)

, trivago_pricing_scores as
(
  select
    ts_code, hotel_id, hotel_city_code, hotel_country_code
    , device_type, user_country_code
    , total_impressions, d, pricing_score
  from trivago_performance
  where hotel_country_code is not null
    and device_type is not null
)

, overall_pricing_score as
(
  select * from skyscanner_pricing_scores
  union all
  select * from vio_pricing_scores
  union all
  select * from trivago_pricing_scores
  union all
  select * from google_htc_performance
)

, prev_day as
(
  select
    ts_code
    , hotel_id
    , hotel_city_code
    , hotel_country_code
    , device_type
    , user_country_code
    , coalesce(proposed_markup,optimal_markup_pct) as prev_markup_pct 
  from `wego-cloud.pricing_engine.hotels_pricing_engine_distribution_calculation` a
  where date(created_at) between current_date - interval '14' day and current_date - interval '1' day  
  qualify row_number()over(partition by a.ts_code,a.hotel_id,a.hotel_city_code,a.hotel_country_code,a.device_type,a.user_country_code order by created_at desc) = 1
)

, optimal_markup AS (
  select * from
  (
    select distinct
      a.*
      , prev_markup_pct
      -- , coalesce(b.rule_priority,c.rule_priority,d.rule_priority,e.rule_priority,f.rule_priority,g.rule_priority,h.rule_priority) as rule_priority
      -- , coalesce(b.min_markup,c.min_markup,d.min_markup,e.min_markup,f.min_markup,g.min_markup,h.min_markup) + 
      --   (coalesce(b.max_markup,c.max_markup,d.max_markup,e.max_markup,f.max_markup,g.max_markup,h.max_markup) - 
      --   coalesce(b.min_markup,c.min_markup,d.min_markup,e.min_markup,f.min_markup,g.min_markup,h.min_markup)) * pricing_score as optimal_markup_pct
      -- , coalesce(b.min_markup,c.min_markup,d.min_markup,e.min_markup,f.min_markup,g.min_markup,h.min_markup) as min_markup
      -- , coalesce(b.max_markup,c.max_markup,d.max_markup,e.max_markup,f.max_markup,g.max_markup,h.max_markup) as max_markup
      , rule_priority
      , min_markup + ((max_markup - min_markup) * pricing_score) as optimal_markup_pct
      , min_markup
      , max_markup
    from overall_pricing_score a
    left join guardrail b
      on a.ts_code = b.ts_code
      and (a.user_country_code = b.user_country_codes_included
      or if(b.user_country_codes_included is null,true, false))
      and (b.user_country_codes_excluded is null OR a.user_country_code <> b.user_country_codes_excluded)
      and (a.hotel_country_code = b.destination_country_codes_included
      or if(b.destination_country_codes_included is null,true, false))
      and (b.destination_country_codes_excluded is null OR a.hotel_country_code <> b.destination_country_codes_excluded)
      and (a.hotel_city_code = b.city_codes_included
       or if(b.city_codes_included is null,true, false))
      and (b.city_codes_excluded is null OR a.hotel_city_code <> b.city_codes_excluded)
      and (a.device_type = b.device_types_included
      or if(b.device_types_included is null,true, false))
  
    left join prev_day i
      on coalesce(a.ts_code,'null') = coalesce(i.ts_code,'null')
      and coalesce(a.hotel_id,'null') = coalesce(i.hotel_id,'null')
      and coalesce(a.hotel_city_code,'null') = coalesce(i.hotel_city_code,'null')
      and coalesce(a.hotel_country_code,'null') = coalesce(i.hotel_country_code,'null')
      and coalesce(a.device_type,'null') = coalesce(i.device_type,'null')
      and coalesce(a.user_country_code,'null') = coalesce(i.user_country_code,'null') 
    
  ) a
  qualify row_number() over(partition by a.ts_code,a.hotel_id,a.hotel_city_code,a.hotel_country_code,a.device_type,a.user_country_code order by rule_priority) = 1
)

, adjustment_calc AS (
  select
    *
    , optimal_markup_pct - coalesce(prev_markup_pct,optimal_markup_pct) as markup_gap
    -- , case when 
    --    coalesce(prev_markup_pct,optimal_markup_pct) + ((optimal_markup_pct - coalesce(prev_markup_pct,optimal_markup_pct)) * 0.3) < min_markup
    --    then min_markup when
    --     coalesce(prev_markup_pct,optimal_markup_pct) + ((optimal_markup_pct - coalesce(prev_markup_pct,optimal_markup_pct)) * 0.3) > max_markup
    --    then max_markup else
    --     coalesce(prev_markup_pct,optimal_markup_pct) + ((optimal_markup_pct - coalesce(prev_markup_pct,optimal_markup_pct)) * 0.3)
    --   end as proposed_markup
    , optimal_markup_pct as proposed_markup
  from optimal_markup
)

select 
  ts_code	
  , hotel_id	
  , hotel_city_code	
  , hotel_country_code	
  , device_type	
  , user_country_code	
  , total_impressions	
  , d	
  , pricing_score	
  , prev_markup_pct	
  , optimal_markup_pct	
  , min_markup	
  , max_markup	
  , markup_gap	
  , proposed_markup	
  , datetime(current_timestamp(),'Asia/Singapore') as created_at
from adjustment_calc
{% endraw %}
