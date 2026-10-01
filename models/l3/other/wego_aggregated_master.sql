{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : wego_aggregated_master_append_version
-- Destination: analysis.wego_aggregated_master  (unchanged)
-- Schedule   : every day 02:20   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'flights_bookables') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'flights_searches') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookables') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
-- depends_on: {{ source('wego_analytics', 'hotels_searches') }}
-- depends_on: {{ source('wego_analytics', 'pageviews') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
declare start_date date default date_sub(current_date, interval 30 day);
declare end_date date default date_sub(current_date, interval 1 day);

begin transaction;

delete from `wego-cloud.analysis.wego_aggregated_master`
where date between start_date and end_date
;

insert into `wego-cloud.analysis.wego_aggregated_master`
  (
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , provider_code
    , provider_domain
    , trip_type
    , model
    , searches
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , gross_revenue_usd
    , booking_revenue_usd
    , sessions
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  )

with date_param as (
  select
    start_date
    , end_date
)

-- ============================================================================
-- session-grain tagging: one row per session, joined to all 8 legs
-- ============================================================================

, session_raw as (
  select
    session_id
    , date(created_at) as created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , app_version
    , user_country_code
    , market
    , site_code
    , locale
    , cf_bot_score
  from `wego-cloud.wego_analytics.sessions`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, session_click_flag as (
  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.flights_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
  union distinct
  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.hotels_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, session_revenue_flag as (

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.flights_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and revenue_in_usd > 0
  
  union distinct

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.hotels_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and revenue_in_usd > 0
  
  union distinct

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.flights_bookables`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and finance_revenue_usd > 0
  
  union distinct

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.hotels_bookables`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and finance_revenue_usd > 0
  
  union distinct

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.flights_bookings`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
    and conversions_tracked > 0
  
  union distinct

  select 
    distinct session_id
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.hotels_bookings`
  where date(created_at) between (select start_date from date_param) and (select end_date from date_param)
    and conversions_adjusted > 0
)

, session_tag as (
  select
    s.session_id
    , case
        when s.cf_bot_score is null then 'unscored'
        when s.cf_bot_score <= 10 then '01-10'
        when s.cf_bot_score <= 20 then '11-20'
        when s.cf_bot_score <= 29 then '21-29'
        when s.cf_bot_score <= 49 then '30-49'
        when s.cf_bot_score <= 60 then '50-60'
        when s.cf_bot_score <= 79 then '61-79'
        else '80-99'
      end as bot_score_bucket
    , s.created_date
    , coalesce(c.session_id is not null, false) as has_clicks
    , coalesce(r.session_id is not null, false) as has_revenue
  from session_raw s
  left join session_click_flag c
    on s.session_id = c.session_id
    and s.created_date = c.created_date
  left join session_revenue_flag r
    on s.session_id = r.session_id
    and s.created_date = r.created_date
)

-- session context each fact leg joins for channel / market / os_type / country
, session_context as (
  select
    session_id
    , channel
    , market
    , app_version
    , os_type
    , user_country_code
    , created_date
  from session_raw
)

-- ============================================================================
-- inlined builder 1/4: visualisation-aggregated_visits_static.sql
-- ============================================================================

, visits_session_base as (
  select
    created_date
    , session_id
    , case
        when client_type = 'app' then 'mobile_app'
        when channel = 'advertising' then 'display'
        when (channel like '%affiliate%' or channel like '%affliates%') then 'affiliates'
        when channel = 'dist-synd' then 'distribution'
        when (channel = 'organic-social' or channel = 'paid-social') then 'social'
        when channel like 'unknown%' then 'direct'
        when channel = 'test' then 'direct'
        else channel
      end as channel
    , if(lower(device) in ('desktop', 'smartphone', 'tablet'), lower(device), 'desktop') as device
    , case
        when lower(os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or os_type is null then lower(os_type)
        else 'others'
      end as os_type
    , if(lower(client_type) in ('app', 'web', 'api'), lower(client_type), 'web') as client_type
    , case
        when lower(client_type) = 'app' and lower(os_type) = 'android' and app_version like '%HP%' then 'huawei_preload'
        when lower(client_type) = 'app' and lower(os_type) = 'android' and app_version like '%SP%' then 'samsung_preload'
        else device_type
      end as device_type
    , user_country_code
    , market
    , site_code
    , locale
  from session_raw
)

, visits_trip_flights as (
  select
    session_id
    , trip_category
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.flights_searches`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, visits_trip_hotels_src as (
  select
    session_id
    , country_code
    , date(created_at) as created_date
  from `wego-cloud.wego_analytics.hotels_searches`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, visits_trip_hotels as (
  select
    h.session_id
    , case
        when s.user_country_code = h.country_code then 'domestic'
        else 'international'
      end as trip_category
    , h.created_date
  from visits_trip_hotels_src h
  left join session_context s
    on h.session_id = s.session_id
    and h.created_date = s.created_date
)

, visits_trip_union as (
  select session_id, trip_category, created_date from visits_trip_flights
  union all
  select session_id, trip_category, created_date from visits_trip_hotels
)

, visits_trip_agg as (
  select
    session_id
    , created_date
    , string_agg(trip_category, ', ') as trip_agg
  from visits_trip_union
  group by 1,2
)

, visits_pageview_raw as (
  select
    date(created_at) as created_date
    , session_id
    , product
    , max(user_hash) over (partition by client_id, session_id) as user_email
  from `wego-cloud.wego_analytics.pageviews`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, visits_pageview_session as (
  select
    created_date
    , session_id
    , if(user_email is not null, 'logged in', 'not logged in') as user_login
    , array_to_string(array_agg(distinct product), " : ") as products
  from visits_pageview_raw
  group by 1,2,3
)

, visits_pageview_pick as (
  select
    session_id
    , user_login
    , case
        when (products like '%flights%' and products like '%hotels%') then 'both'
        when products like '%flights%' then 'flights'
        when products like '%hotels%' then 'hotels'
        else 'homepage'
      end as product
    , created_date
  from visits_pageview_session
  qualify row_number() over (partition by session_id order by created_date desc) = 1
)

, static_visits as (
  select
    b.created_date
    , b.channel
    , b.device
    , b.os_type
    , b.client_type
    , b.device_type
    , b.user_country_code
    , b.market
    , b.site_code
    , b.locale
    , case
        when t.trip_agg is null then 'neither'
        when strpos(t.trip_agg, 'international') > 0 and strpos(t.trip_agg, 'domestic') > 0 then 'both'
        when strpos(t.trip_agg, 'international') > 0 then 'international'
        when strpos(t.trip_agg, 'domestic') > 0 then 'domestic'
        else null
      end as trip_category
    , pv.user_login
    , pv.product
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
    , sum(1) as visits
  from visits_session_base b
  left join visits_trip_agg t
    on b.session_id = t.session_id
    and b.created_date = t.created_date 
  left join visits_pageview_pick pv
    on b.session_id = pv.session_id
    and b.created_date = pv.created_date
  left join session_tag tag
    on b.session_id = tag.session_id
    and b.created_date = tag.created_date
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

-- ============================================================================
-- inlined builder 2/4: visualisation-aggregated_searches_static.sql
-- ============================================================================

, searches_flights_leg as (
  select
    date(f.created_at) as created_date
    , case
        when f.client_type = 'app' then 'mobile_app'
        when s.channel = 'advertising' then 'display'
        when (s.channel like '%affiliate%' or s.channel like '%affliates%') then 'affiliates'
        when s.channel = 'dist-synd' then 'distribution'
        when (s.channel = 'organic-social' or s.channel = 'paid-social') then 'social'
        when s.channel like 'unknown%' then 'direct'
        when s.channel = 'test' then 'direct'
        else s.channel
      end as channel
    , if(lower(f.device) in ('desktop', 'smartphone', 'tablet'), lower(f.device), 'desktop') as device
    , case
        when lower(s.os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or s.os_type is null then lower(s.os_type)
        else 'others'
      end as os_type
    , if(lower(f.client_type) in ('app', 'web', 'api'), lower(f.client_type), 'web') as client_type
    , case
        when lower(f.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(f.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else f.device_type
      end as device_type
    , s.user_country_code
    , s.market
    , f.site_code
    , f.locale
    , case
        when f.user_logged_in = true then 'logged in'
        when f.user_logged_in = false then 'not logged in'
      end as user_login
    , 'flights' as product
    , f.trip_category
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from `wego-cloud.wego_analytics.flights_searches` f
  left join session_context s
    on f.session_id = s.session_id
    and date(f.created_at) = s.created_date
  left join session_tag tag
    on f.session_id = tag.session_id
    and date(f.created_at) = tag.created_date
  where date(f._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, searches_hotels_leg as (
  select
    date(h.created_at) as created_date
    , case
        when h.client_type = 'app' then 'mobile_app'
        when s.channel = 'advertising' then 'display'
        when (s.channel like '%affiliate%' or s.channel like '%affliates%') then 'affiliates'
        when s.channel = 'dist-synd' then 'distribution'
        when (s.channel = 'organic-social' or s.channel = 'paid-social') then 'social'
        when s.channel like 'unknown%' then 'direct'
        when s.channel = 'test' then 'direct'
        else s.channel
      end as channel
    , if(lower(h.device) in ('desktop', 'smartphone', 'tablet'), lower(h.device), 'desktop') as device
    , case
        when lower(s.os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or s.os_type is null then lower(s.os_type)
        else 'others'
      end as os_type
    , if(lower(h.client_type) in ('app', 'web', 'api'), lower(h.client_type), 'web') as client_type
    , case
        when lower(h.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(h.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else h.device_type
      end as device_type
    , s.user_country_code
    , s.market
    , h.site_code
    , h.locale
    , case
        when h.user_logged_in = true then 'logged in'
        when h.user_logged_in = false then 'not logged in'
      end as user_login
    , 'hotels' as product
    , if(s.user_country_code = h.country_code, 'domestic', 'international') as trip_category
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from `wego-cloud.wego_analytics.hotels_searches` h
  left join session_context s
    on h.session_id = s.session_id
    and date(h.created_at) = s.created_date
  left join session_tag tag
    on h.session_id = tag.session_id
    and date(h.created_at) = tag.created_date
  where date(h._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, searches_union as (
  select
    created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product
    , trip_category
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from searches_flights_leg
  union all
  select
    created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product
    , trip_category
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from searches_hotels_leg
)

, static_searches as (
  select
    created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product
    , trip_category
    , bot_score_bucket
    , has_clicks
    , has_revenue
    , sum(1) as searches
  from searches_union
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)

-- ============================================================================
-- inlined builder 3/4: visualisation-aggregated_clicks_static.sql
-- ============================================================================

, clicks_flight_segments as (
  select
    click_id
    , sum(cast(segments as int64)) as segment_count
  from `wego-cloud.wego_analytics.flights_clicks`, unnest(split(legs_segments, '=')) as segments
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
  group by 1
)

, clicks_flights_leg as (
  select
    date(f.created_at) as created_date
    , case
        when f.client_type = 'app' then 'mobile_app'
        when s.channel = 'advertising' then 'display'
        when (s.channel like '%affiliate%' or s.channel like '%affliates%') then 'affiliates'
        when s.channel = 'dist-synd' then 'distribution'
        when (s.channel = 'organic-social' or s.channel = 'paid-social') then 'social'
        when s.channel like 'unknown%' then 'direct'
        when s.channel = 'test' then 'direct'
        else s.channel
      end as channel
    , if(lower(f.device) in ('desktop', 'smartphone', 'tablet'), lower(f.device), 'desktop') as device
    , case
        when lower(s.os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or s.os_type is null then lower(s.os_type)
        else 'others'
      end as os_type
    , if(lower(f.client_type) in ('app', 'web', 'api'), lower(f.client_type), 'web') as client_type
    , case
        when lower(f.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(f.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else f.device_type
      end as device_type
    , s.user_country_code
    , s.market
    , f.site_code
    , f.locale
    , case
        when f.user_logged_in = true then 'logged in'
        when f.user_logged_in = false then 'not logged in'
      end as user_login
    , 'flights' as product
    , f.provider_code
    , f.provider_domain
    , f.trip_category
    , f.trip_type
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
    , cast(if(f.conversions_tracked > 0, seg.segment_count * f.conversions_tracked, 0) as int64) as tracked_booking_segments
    , f.conversions_tracked
    , f.conversions_adjusted
    , if(f.conversions_tracked > 0, f.total_price_usd * f.conversions_tracked, 0) as tracked_booking_gmv
    , f.revenue_in_usd
    , f.finance_revenue_usd
    , f.booking_revenue_usd
  from `wego-cloud.wego_analytics.flights_clicks` f
  left join session_context s
    on f.session_id = s.session_id
    and date(f.created_at) = s.created_date
  left join clicks_flight_segments seg
    on f.click_id = seg.click_id
  left join session_tag tag
    on f.session_id = tag.session_id
    and date(f.created_at) = tag.created_date
  where date(f._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, clicks_hotels_leg as (
  select
    date(h.created_at) as created_date
    , case
        when h.client_type = 'app' then 'mobile_app'
        when s.channel = 'advertising' then 'display'
        when (s.channel like '%affiliate%' or s.channel like '%affliates%') then 'affiliates'
        when s.channel = 'dist-synd' then 'distribution'
        when (s.channel = 'organic-social' or s.channel = 'paid-social') then 'social'
        when s.channel like 'unknown%' then 'direct'
        when s.channel = 'test' then 'direct'
        else s.channel
      end as channel
    , if(lower(h.device) in ('desktop', 'smartphone', 'tablet'), lower(h.device), 'desktop') as device
    , case
        when lower(s.os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or s.os_type is null then lower(s.os_type)
        else 'others'
      end as os_type
    , if(lower(h.client_type) in ('app', 'web', 'api'), lower(h.client_type), 'web') as client_type
    , case
        when lower(h.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(h.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else h.device_type
      end as device_type
    , s.user_country_code
    , s.market
    , h.site_code
    , h.locale
    , case
        when h.user_logged_in = true then 'logged in'
        when h.user_logged_in = false then 'not logged in'
      end as user_login
    , 'hotels' as product
    , h.provider_code
    , h.provider_domain
    , case
        when s.user_country_code = h.country_code then 'domestic'
        else 'international'
      end as trip_category
    , cast(null as string) as trip_type
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
    , 0 as tracked_booking_segments
    , h.conversions_tracked
    , h.conversions_adjusted
    , if(h.conversions_tracked > 0, h.total_price_usd * h.conversions_tracked, 0) as tracked_booking_gmv
    , h.revenue_in_usd
    , h.finance_revenue_usd
    , h.booking_revenue_usd
  from `wego-cloud.wego_analytics.hotels_clicks` h
  left join session_context s
    on h.session_id = s.session_id
    and date(h.created_at) = s.created_date
  left join session_tag tag
    on h.session_id = tag.session_id
    and date(h.created_at) = tag.created_date
  where date(h._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, clicks_union as (
  select
    created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product
    , provider_code
    , provider_domain
    , trip_category
    , trip_type
    , bot_score_bucket
    , has_clicks
    , has_revenue
    , tracked_booking_segments
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
  from clicks_flights_leg
  union all
  select
    created_date
    , channel
    , device
    , os_type
    , client_type
    , device_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product
    , provider_code
    , provider_domain
    , trip_category
    , trip_type
    , bot_score_bucket
    , has_clicks
    , has_revenue
    , tracked_booking_segments
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
  from clicks_hotels_leg
)

, static_clicks as (
  select
    created_date, channel, device, os_type, client_type, device_type
    , user_country_code, market, site_code, locale, user_login, product
    , provider_code, provider_domain, trip_category, trip_type
    , bot_score_bucket, has_clicks, has_revenue
    , sum(tracked_booking_segments) as tracked_booking_segments
    , sum(1) as clicks
    , sum(conversions_tracked) as conversions_tracked
    , sum(conversions_adjusted) as conversions_adjusted
    , sum(tracked_booking_gmv) as tracked_booking_gmv
    , sum(revenue_in_usd) as revenue_in_usd
    , sum(finance_revenue_usd) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
  from clicks_union
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

-- ============================================================================
-- inlined builder 4/4: visualisation-aggregated_bookables_static.sql
-- ============================================================================

, bookables_flights_leg as (
  select
    date(f.created_at) as created_date
    , f.session_id
    , f.client_type
    , f.device
    , f.device_type
    , f.site_code
    , f.locale
    , cast(null as string) as user_login
    , 'flights' as product
    , f.provider_code
    , f.trip_category
    , f.finance_revenue_usd as revenue_in_usd
  from `wego-cloud.wego_analytics.flights_bookables` f
  where date(f._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, bookables_hotels_leg as (
  select
    date(h.created_at) as created_date
    , h.session_id
    , h.client_type
    , h.device
    , h.device_type
    , h.site_code
    , h.locale
    , cast(null as string) as user_login
    , 'hotels' as product
    , h.provider_code
    , if(s.user_country_code = h.country_code, 'domestic', 'international') as trip_category
    , h.finance_revenue_usd as revenue_in_usd
  from `wego-cloud.wego_analytics.hotels_bookables` h
  left join session_context s
    on h.session_id = s.session_id
    and date(h.created_at) = created_date
  where date(h._partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, bookables_union as (
  select
    created_date
    , session_id
    , client_type
    , device
    , device_type
    , site_code
    , locale
    , user_login
    , product
    , provider_code
    , trip_category
    , revenue_in_usd
  from bookables_flights_leg
  union all
  select
    created_date
    , session_id
    , client_type
    , device
    , device_type
    , site_code
    , locale
    , user_login
    , product
    , provider_code
    , trip_category
    , revenue_in_usd
  from bookables_hotels_leg
)

, static_bookables as (
  select
    b.created_date
    , case
        when b.client_type = 'app' then 'mobile_app'
        when s.channel = 'advertising' then 'display'
        when (s.channel like '%affiliate%' or s.channel like '%affliates%') then 'affiliates'
        when s.channel = 'dist-synd' then 'distribution'
        when (s.channel = 'organic-social' or s.channel = 'paid-social') then 'social'
        when s.channel like 'unknown%' then 'direct'
        when s.channel = 'test' then 'direct'
        else s.channel
      end as channel
    , if(lower(b.device) in ('desktop', 'smartphone', 'tablet'), lower(b.device), 'desktop') as device
    , case
        when lower(s.os_type) in ('android', 'ios', 'windows', 'osx', 'linux') or s.os_type is null then lower(s.os_type)
        else 'others'
      end as os_type
    , if(lower(b.client_type) in ('app', 'web', 'api'), lower(b.client_type), 'web') as client_type
    , case
        when lower(b.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(b.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else b.device_type
      end as device_type
    , s.user_country_code
    , s.market
    , b.site_code
    , b.locale
    , b.user_login
    , b.product
    , b.provider_code
    , b.trip_category
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
    , sum(1) as clicks
    , sum(b.revenue_in_usd) as revenue_in_usd
  from bookables_union b
  left join session_context s
    on b.session_id = s.session_id
    and b.created_date = s.created_date 
  left join session_tag tag
    on b.session_id = tag.session_id
    and b.created_date =  tag.created_date
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

-- leg 1/8: sessions (grain source for the `sessions` metric)
, leg_sessions as (
  select
    created_date as date
    , channel
    , device
    , os_type
    , client_type
    , upper(user_country_code) as user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product as product_vertical
    , trip_category
    , concat(product, '_', trip_category) as vertical_x_trip_category
    , device_type
    , 0 as searches
    , cast(null as string) as provider_code
    , cast(null as string) as provider_domain
    , cast(null as string) as trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , 0 as conversions_tracked
    , 0 as conversions_adjusted
    , 0 as tracked_booking_gmv
    , 0 as revenue_in_usd
    , 0 as finance_revenue_usd
    , 0 as booking_revenue_usd
    , cast(null as string) as model
    , sum(visits) as visits
    , 0 as gross_revenue_usd
    , 0 as brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from static_visits
  -- where date(created_at) between (select start_date from date_param) and (select end_date from date_param)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 2/8: searches
, leg_searches as (
  select
    created_date as date
    , channel
    , device
    , os_type
    , client_type
    , upper(user_country_code) as user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product as product_vertical
    , trip_category
    , concat(product, '_', trip_category) as vertical_x_trip_category
    , device_type
    , sum(searches) as searches
    , cast(null as string) as provider_code
    , cast(null as string) as provider_domain
    , cast(null as string) as trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , 0 as conversions_tracked
    , 0 as conversions_adjusted
    , 0 as tracked_booking_gmv
    , 0 as revenue_in_usd
    , 0 as finance_revenue_usd
    , 0 as booking_revenue_usd
    , cast(null as string) as model
    , 0 as visits
    , 0 as gross_revenue_usd
    , 0 as brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from static_searches
  -- where created_date between (select start_date from date_param) and (select end_date from date_param)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 3/8: meta clicks + meta revenue (post-decouple: bow no longer nulled out here)
, leg_clicks_main as (
  select
    created_date as date
    , channel
    , device
    , os_type
    , client_type
    , upper(user_country_code) as user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product as product_vertical
    , trip_category
    , concat(product, '_', trip_category) as vertical_x_trip_category
    , device_type
    , 0 as searches
    , provider_code
    , provider_domain
    , trip_type
    , sum(tracked_booking_segments) as tracked_booking_segments
    , sum(clicks) as clicks
    , sum(conversions_tracked) as conversions_tracked
    , sum(conversions_adjusted) as conversions_adjusted
    , sum(tracked_booking_gmv) as tracked_booking_gmv
    , sum(revenue_in_usd) as revenue_in_usd
    , sum(finance_revenue_usd) as finance_revenue_usd
    , sum(booking_revenue_usd) as booking_revenue_usd
    , case
        when provider_domain like "%wego.com-%" then "FB"
        else "meta"
      end as model
    , 0 as visits
    , sum(finance_revenue_usd) as gross_revenue_usd
    , 0 as brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from static_clicks
  -- where created_date between (select start_date from date_param) and (select end_date from date_param)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 4/8 part a: flights click-level gmv
, gmv_flights_clicks as (
  select
    created_at
    , session_id
    , device
    , client_type
    , site_code
    , locale
    , if(user_logged_in = true, "logged in", "not logged in") as user_login
    , "flights" as product_vertical
    , trip_category
    , arrival_country_code as destination
    , device_type
    , provider_code
    , provider_domain
    , trip_type
    , case
        when provider_domain like "%wego.com-%" then "FB"
        else "meta"
      end as model
    , booking_value_adjusted
  from `wego-cloud.wego_analytics.flights_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

-- leg 4/8 part b: hotels click-level gmv
, gmv_hotels_clicks as (
  select
    created_at
    , session_id
    , device
    , client_type
    , site_code
    , locale
    , if(user_logged_in = true, "logged in", "not logged in") as user_login
    , "hotels" as product_vertical
    , cast(null as string) as trip_category
    , country_code as destination
    , device_type
    , provider_code
    , provider_domain
    , cast(null as string) as trip_type
    , case
        when provider_domain like "%wego.com-%" then "FB"
        else "meta"
      end as model
    , booking_value_adjusted
  from `wego-cloud.wego_analytics.hotels_clicks`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

, gmv_clicks as (
  select
    created_at
    , session_id
    , device
    , client_type
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , destination
    , device_type
    , provider_code
    , provider_domain
    , trip_type
    , model
    , booking_value_adjusted
  from gmv_flights_clicks
  union all
  select
    created_at
    , session_id
    , device
    , client_type
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , destination
    , device_type
    , provider_code
    , provider_domain
    , trip_type
    , model
    , booking_value_adjusted
  from gmv_hotels_clicks
)

-- session context for the gmv leg: supplies channel / market / user_country_code / os_type
, gmv_session_context as (
  select distinct
    date(created_at) as created_date
    , session_id
    , user_country_code
    , market
    , channel
    , os_type
  from `wego-cloud.wego_analytics.sessions`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
)

-- leg 4/8: clicks brr gmv
, leg_clicks_brr_gmv as (
  select
    date(c.created_at) as date
    , s.channel
    , c.device
    , s.os_type
    , c.client_type
    , upper(s.user_country_code) as user_country_code
    , s.market
    , c.site_code
    , c.locale
    , c.user_login
    , c.product_vertical
    , if(c.product_vertical = "flights", c.trip_category, if(s.user_country_code != c.destination, "international", "domestic")) as trip_category
    , concat(c.product_vertical, '_', if(c.product_vertical = "flights", c.trip_category, if(s.user_country_code != c.destination, "international", "domestic"))) as vertical_x_trip_category
    , c.device_type
    , 0 as searches
    , c.provider_code
    , c.provider_domain
    , c.trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , 0 as conversions_tracked
    , 0 as conversions_adjusted
    , 0 as tracked_booking_gmv
    , 0 as revenue_in_usd
    , 0 as finance_revenue_usd
    , 0 as booking_revenue_usd
    , c.model
    , 0 as visits
    , 0 as gross_revenue_usd
    , sum(c.booking_value_adjusted) as brr_gmv
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from gmv_clicks c
  left join gmv_session_context s
    on c.session_id = s.session_id
    and date(c.created_at) = s.created_date
  left join session_tag tag
    on c.session_id = tag.session_id
    and date(c.created_at) = tag.created_date
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 5/8: cau (bookables)
, leg_cau as (
  select
    created_date as date
    , channel
    , device
    , os_type
    , client_type
    , upper(user_country_code) as user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product as product_vertical
    , trip_category
    , concat(product, '_', trip_category) as vertical_x_trip_category
    , device_type
    , 0 as searches
    , provider_code
    , cast(null as string) as provider_domain
    , cast(null as string) as trip_type
    , 0 as tracked_booking_segments
    , sum(clicks) as clicks
    , 0 as conversions_tracked
    , 0 as conversions_adjusted
    , 0 as tracked_booking_gmv
    , sum(revenue_in_usd) as revenue_in_usd
    , sum(revenue_in_usd) as finance_revenue_usd
    , 0 as booking_revenue_usd
    , "CAU" as model
    , 0 as visits
    , sum(revenue_in_usd) as gross_revenue_usd
    , 0 as brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from static_bookables
  where created_date between (select start_date from date_param) and (select end_date from date_param)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- insurance revenue rolled to one row per booking, for leg 6/8
, flights_insurance_by_booking as (
  select
    booking_id
    , sum(finance_revenue_usd) as insurance_revenue
    , sum(gross_revenue_in_usd) as insurance_gross_revenue
    , date(min(created_at)) as created_date
  from `wego-cloud.wego_analytics.flights_insurance`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
  group by 1
)

-- session context for the bow booking legs: supplies os_type / client_type / app_version
, booking_session_context as (
  select
    session_id
    , date(min(created_at)) as created_date
    , max(os_type) as os_type
    , max(client_type) as client_type
    , max(app_version) as app_version
  from `wego-cloud.wego_analytics.sessions`
  where date(_partitiontime) between (select start_date from date_param) and (select end_date from date_param)
  group by 1
)

-- as discussed with Ned, the bow bookings that come from wego metasearch should be counted on both
-- sides, the clicks tables and the bookings tables. this means revenue_in_usd and finance_revenue_usd
-- will not double count, but gross_revenue_usd, conversions_tracked, conversions_adjusted,
-- tracked_booking_gmv and brr_gmv will be double counted.

-- leg 6/8: bow flights main
, leg_bow_flights_main as (
  select
    date(b.created_at) as date
    , b.channel
    , right(b.device_type, 3) as device
    , s.os_type
    , s.client_type
    , b.user_country_code
    , b.market
    , b.site_code
    , b.locale
    , cast(null as string) as user_login
    , "flights" as product_vertical
    , b.trip_category
    , concat("flights_", b.trip_category) as vertical_x_trip_category
    , case
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else b.device_type
      end as device_type
    , 0 as searches
    , "wego.com" as provider_code
    , "wego.com" as provider_domain
    , b.trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , sum(b.conversions_tracked) as conversions_tracked
    , sum(b.conversions_adjusted) as conversions_adjusted
    , sum(b.total_price_usd) as tracked_booking_gmv
    , sum(if(date(b.created_at) > '2022-08-18' and b.charged_currency_code = "SAR", (b.finance_revenue_usd + ifnull(i.insurance_revenue, 0)) + b.blended_total_gateway_fees_usd - b.est_total_gateway_fees_usd, b.finance_revenue_usd + ifnull(i.insurance_revenue, 0))) as revenue_in_usd
    , sum(if(date(b.created_at) > '2022-08-18' and b.charged_currency_code = "SAR", (b.finance_revenue_usd + ifnull(i.insurance_revenue, 0)) + b.blended_total_gateway_fees_usd - b.est_total_gateway_fees_usd, b.finance_revenue_usd + ifnull(i.insurance_revenue, 0))) as finance_revenue_usd
    , sum(if(date(b.created_at) > '2022-08-18' and b.charged_currency_code = "SAR", (b.finance_revenue_usd + ifnull(i.insurance_revenue, 0)) + b.blended_total_gateway_fees_usd - b.est_total_gateway_fees_usd, b.finance_revenue_usd + ifnull(i.insurance_revenue, 0))) as booking_revenue_usd
    , "BOW" as model
    , 0 as visits
    , sum(b.gross_revenue_in_usd) + sum(ifnull(i.insurance_gross_revenue, 0)) as gross_revenue_usd
    , sum(b.total_price_usd) as brr_gmv
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from `wego-cloud.wego_analytics.flights_bookings` b
  left join flights_insurance_by_booking i
    on b.booking_id = i.booking_id
  left join booking_session_context s
    on s.session_id = b.session_id
    and date(b.created_at) = s.created_date
  left join session_tag tag
    on b.session_id = tag.session_id
    and date(b.created_at) = tag.created_date
  where date(b.created_at) between (select start_date from date_param) and (select end_date from date_param)
    and b.conversions_tracked > 0
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 7/8: bow flights unused tickets
, leg_bow_flights_unused_tickets as (
  select
    date(b.unused_tickets_refunded_date) as date
    , b.channel
    , right(b.device_type, 3) as device
    , s.os_type
    , s.client_type
    , b.user_country_code
    , b.market
    , b.site_code
    , b.locale
    , cast(null as string) as user_login
    , "flights" as product_vertical
    , b.trip_category
    , concat("flights_", b.trip_category) as vertical_x_trip_category
    , case
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else b.device_type
      end as device_type
    , 0 as searches
    , "wego.com" as provider_code
    , "wego.com" as provider_domain
    , b.trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , 0 as conversions_tracked
    , 0 as conversions_adjusted
    , 0 as tracked_booking_gmv
    , sum(b.unused_tickets_amount_usd) as revenue_in_usd
    , sum(b.unused_tickets_amount_usd) as finance_revenue_usd
    , sum(b.unused_tickets_amount_usd) as booking_revenue_usd
    , "BOW" as model
    , 0 as visits
    , sum(b.unused_tickets_amount_usd) as gross_revenue_usd
    , 0 as brr_gmv
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from `wego-cloud.wego_analytics.flights_bookings` b
  left join booking_session_context s
    on s.session_id = b.session_id
    and date(b.created_at) = s.created_date
  left join session_tag tag
    on b.session_id = tag.session_id
    and date(b.created_at) = tag.created_date
  where date(b.unused_tickets_refunded_date) between (select start_date from date_param) and (select end_date from date_param)
    and b.conversions_tracked > 0
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

-- leg 8/8: bow hotels
, leg_bow_hotels as (
  select
    date(b.created_at) as date
    , b.channel
    , right(b.device_type, 3) as device
    , s.os_type
    , s.client_type
    , b.user_country_code
    , b.market
    , b.site_code
    , b.locale
    , cast(null as string) as user_login
    , "hotels" as product_vertical
    , if(b.user_country_code != b.hotel_country_code, "international", "domestic") as trip_category
    , concat("hotels_", if(b.user_country_code != b.hotel_country_code, "international", "domestic")) as vertical_x_trip_category
    , case
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%HP%' then 'huawei_preload'
        when lower(s.client_type) = 'app' and lower(s.os_type) = 'android' and s.app_version like '%SP%' then 'samsung_preload'
        else b.device_type
      end as device_type
    , 0 as searches
    , "hotels.wego.com" as provider_code
    , "hotels.wego.com" as provider_domain
    , cast(null as string) as trip_type
    , 0 as tracked_booking_segments
    , 0 as clicks
    , sum(b.conversions_tracked) as conversions_tracked
    , sum(b.conversions_adjusted) as conversions_adjusted
    , sum(b.wego_total_price_usd) as tracked_booking_gmv
    , sum(b.revenue_in_usd) as revenue_in_usd
    , sum(b.finance_revenue_usd) as finance_revenue_usd
    , sum(b.finance_revenue_usd) as booking_revenue_usd
    , "BOW" as model
    , 0 as visits
    , sum(b.gross_revenue_usd) as gross_revenue_usd
    , sum(b.wego_total_price_usd) as brr_gmv
    , tag.bot_score_bucket
    , tag.has_clicks
    , tag.has_revenue
  from `wego-cloud.wego_analytics.hotels_bookings` b
  left join booking_session_context s
    on s.session_id = b.session_id
    and date(b.created_at) = s.created_date
  left join session_tag tag
    on b.session_id = tag.session_id
    and date(b.created_at) = tag.created_date
  where date(b.created_at) between (select start_date from date_param) and (select end_date from date_param)
    and b.conversions_adjusted > 0
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,27,31,32,33
)

, all_legs as (
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_sessions
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_searches
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_clicks_main
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_clicks_brr_gmv
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_cau
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_bow_flights_main
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_bow_flights_unused_tickets
  union all
  select
    date
    , channel
    , device
    , os_type
    , client_type
    , user_country_code
    , market
    , site_code
    , locale
    , user_login
    , product_vertical
    , trip_category
    , vertical_x_trip_category
    , device_type
    , searches
    , provider_code
    , provider_domain
    , trip_type
    , tracked_booking_segments
    , clicks
    , conversions_tracked
    , conversions_adjusted
    , tracked_booking_gmv
    , revenue_in_usd
    , finance_revenue_usd
    , booking_revenue_usd
    , model
    , visits
    , gross_revenue_usd
    , brr_gmv
    , bot_score_bucket
    , has_clicks
    , has_revenue
  from leg_bow_hotels
)

select
  date
  , channel
  , device
  , os_type
  , client_type
  , user_country_code
  , market
  , upper(site_code) as site_code
  , locale
  , user_login
  , product_vertical
  , trip_category
  , vertical_x_trip_category
  , device_type
  , if(provider_code = "hotels.wego.com", "wego.com", provider_code) as provider_code
  , provider_domain
  , trip_type
  , model
  , sum(searches) as searches
  , sum(tracked_booking_segments) as tracked_booking_segments
  , sum(clicks) as clicks
  , sum(conversions_tracked) as conversions_tracked
  , sum(conversions_adjusted) as conversions_adjusted
  , sum(tracked_booking_gmv) as tracked_booking_gmv
  , sum(revenue_in_usd) as revenue_in_usd
  , sum(finance_revenue_usd) as finance_revenue_usd
  , sum(gross_revenue_usd) as gross_revenue_usd
  , sum(booking_revenue_usd) as booking_revenue_usd
  , sum(visits) as sessions
  , sum(brr_gmv) as brr_gmv
  , bot_score_bucket
  , has_clicks
  , has_revenue
from all_legs
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,31,32,33
;

commit transaction;
{% endraw %}
