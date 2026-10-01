{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : marketing_landing_pages_daily_append
-- Destination: analysis.marketing_landing_pages  (unchanged)
-- Schedule   : every day 01:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
with
  date_param as (
    -- prod: the scheduled query builds yesterday. pin to a literal to rebuild one past day.
    select
      date_sub(current_date(), interval 1 day) as target_date
    , date_sub(current_date(), interval 2 day) as target_date_prev
  )

, landing_pageview as (
    -- first pageview of each session, used to label the landing page
    select distinct
      session_id
    , concat(session_id, client_id) as session_client_id
    , page_url
    , page_type
    , page_subtype
    from `wego-cloud.wego_analytics.pageviews`
    where date(_partitiontime) = (select target_date from date_param)
    qualify row_number() over (
      partition by session_id, client_id, date(created_at)
      order by created_at
    ) = 1
  )

, clients_daily as (
    select distinct
      date(created_at) as date
      , client_id
      , if(first_visit = true, "new", "returning") as new_returning_status
      -- new additions 2023-11-20
      , device_brand
      , device_model
      , marketing_name as device_name
      , ua_source
      , ua_medium
      , ua_campaign
      , ua_adgroup
      , ua_content
    from
      `wego-cloud.wego_analytics.clients`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, app_rt_sessions as (
    select
    client_id
    , case when session_id like "%-wg-%" then split(session_id, "-wg-")[1] else session_id end as session_id
    from `wego-cloud.marketing_analytics.sessions`
    where timestamp_trunc(engagement_timestamp, day) >= timestamp((select target_date_prev from date_param))
    and source = "re-engagement"
    group by 1,2
  )

, sessions as (
  select distinct
      date(created_at) as date
    , advertiser_id
    , client_type
    , device_type
    , os_type
    , app_version
    , user_country_code
    , site_code
    , locale
    , channel
    , wg_source
    , wg_medium
    , wg_campaign
    -- new fields:
    , wg_adgroup
    , wg_content
    , wg_term
    , wg_misc
    , device_version
    , os_version
    , network_type
    , network_carrier_name
    , ts_code
    , source
    , market
    , case when app_rt.session_id is not null then app_rt_medium end as app_rt_medium
    , case when app_rt.session_id is not null then app_rt_source end as app_rt_source
    , case when app_rt.session_id is not null then app_rt_campaign end as app_rt_campaign
    , case when app_rt.session_id is not null then app_rt_adgroup end as app_rt_adgroup
    , first_value(page_type) over (partition by s.session_id order by created_at) as page_type
    , first_value(page_subtype) over (partition by s.session_id order by created_at) as page_subtype
    , first_value(page_url) over (partition by s.session_id order by created_at) as page_url
    , new_returning_status
    , device_brand
    , device_model
    , device_name
    , ua_source
    , ua_medium
    , ua_campaign
    , ua_adgroup
    , ua_content
    , s.session_id
    , s.cf_bot_score
    , s.client_id
    , concat(s.session_id, s.client_id) as session_client_id
  from `wego-cloud.wego_analytics.sessions` as s
  left join landing_pageview as p
    on  concat(s.session_id, s.client_id) = p.session_client_id
    and s.landing_url                     = p.page_url
  left join clients_daily as c
    on  s.client_id = c.client_id
    and c.date      = date(s.created_at)
  left join app_rt_sessions as app_rt
    on  s.session_id = app_rt.session_id
    and s.client_id  = app_rt.client_id
  where date(_partitiontime) = (select target_date from date_param)
    and s.session_id not like "%python-requests%"
  )

, bounced_sessions as (
    select
      date(created_at) as date
      , session_id
      , concat(session_id, client_id) as session_client_id
    , if(count(distinct pageview_id) = 1, 1, 0) as bounced_sessions
    from
      `wego-cloud.wego_analytics.pageviews`
    where
      date(_partitiontime) = (select target_date from date_param)
    group by 1,2,3
  )

, pageviews as (
  select
date(created_at) as date
  , p.session_id
  , concat(p.session_id, p.client_id) as session_client_id
  , max(
  if(page_type like "%search_results%", concat(p.session_id, p.client_id), null)) as search_results_views
  , max(
  if(page_type = "flights_search_results", concat(p.session_id, p.client_id), null)) as flights_search_results_views
  , max(
  if(page_type = "hotels_search_results", concat(p.session_id, p.client_id), null)) as hotels_search_results_views
  , max(
  if(page_type like "%detail%", concat(p.session_id, p.client_id), null)) as detail_views
  , max(
  if(page_type = "flights_detail_page", concat(p.session_id, p.client_id), null)) as flights_detail_views
  , max(
  if(page_type = "hotels_detail_page", concat(p.session_id, p.client_id), null)) as hotels_detail_views
  , max(
  if(page_type like "%_booking", concat(p.session_id, p.client_id), null)) as bow_checkout_views
  , max(
  if(page_type = "flights_booking", concat(p.session_id, p.client_id), null)) as bow_flights_checkout_views
  , max(
  if(page_type = "hotels_booking", concat(p.session_id, p.client_id), null)) as bow_hotels_checkout_views
  , max(
  if(page_type like "%_booking_confirmation", concat(p.session_id, p.client_id), null)) as bow_booking_success_views
  , max(
  if(page_type = "flights_booking_confirmation", concat(p.session_id, p.client_id), null)) as bow_flights_booking_success_views
  , max(
  if(page_type = "hotels_booking_confirmation", concat(p.session_id, p.client_id), null)) as bow_hotels_booking_success_views
  , max(
  if(length(user_hash) > 1, concat(p.session_id, p.client_id), null)) as session_logged_in
  , bounced_sessions
  from `wego-cloud.wego_analytics.pageviews` as p
  left join bounced_sessions as b
    on  date(p.created_at)                       = b.date
    and concat(p.session_id, p.client_id)        = b.session_client_id
  where date(_partitiontime) = (select target_date from date_param)
  group by 1,2,3,17
  )

, flights_insurance_daily as (
    select
      booking_id
    , sum(finance_revenue_usd) as insurance_revenue
    from `wego-cloud.wego_analytics.flights_insurance`
    where date(created_at) = (select target_date from date_param)
    group by 1
  )

, rev_flights_meta as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "flights" as product_vertical
      , "meta" as model
      , 1 as clicks
      , if(provider_code != "wego.com", conversions_tracked, 0) as bookings
      , finance_revenue_usd as revenue
    , if(conversions_tracked is not null, 1, 0) as tracked_clicks
    from
      `wego-cloud.wego_analytics.flights_clicks`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, rev_hotels_meta as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "hotels" as product_vertical
      , "meta" as model
      , 1 as clicks
      -- , if(provider_code != "hotels.wego.com", conversions_tracked, 0) as bookings
      , 0 as bookings
      , finance_revenue_usd as revenue
    , if(conversions_tracked is not null, 1, 0) as tracked_clicks
    from
      `wego-cloud.wego_analytics.hotels_clicks`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, rev_flights_cau as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "flights" as product_vertical
      , "CAU" as model
      , 1 as clicks
      , 0 as bookings
      , finance_revenue_usd as revenue
      , 0 as tracked_clicks
    from
      `wego-cloud.wego_analytics.flights_bookables`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, rev_hotels_cau as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "hotels" as product_vertical
      , "CAU" as model
      , 1 as clicks
      , 0 as bookings
      , finance_revenue_usd as revenue
      , 0 as tracked_clicks
    from
      `wego-cloud.wego_analytics.hotels_bookables`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, rev_flights_bow as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "flights" as product_vertical
      , "BOW" as model
      , 0 as clicks
      , conversions_adjusted as bookings
      , finance_revenue_usd + coalesce(insurance_revenue, 0) as revenue
      , 0 as tracked_clicks
    from
      `wego-cloud.wego_analytics.flights_bookings` as b
    left join flights_insurance_daily as i
    on
      b.booking_id = i.booking_id
    where
      date(created_at) = (select target_date from date_param)
      and conversions_adjusted > 0
  )

, rev_hotels_bow as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "hotels" as product_vertical
      , "BOW" as model
      , 0 as clicks
      , conversions_adjusted as bookings
      , finance_revenue_usd as revenue
      , 0 as tracked_clicks
    from
      `wego-cloud.wego_analytics.hotels_bookings`
    where
      date(created_at) = (select target_date from date_param)
        and conversions_tracked > 0
  )

, revenue_events as (
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_flights_meta

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_hotels_meta

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_flights_cau

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_hotels_cau

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_flights_bow

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , clicks
    , bookings
    , revenue
    , tracked_clicks
    from rev_hotels_bow
  )

, revenue as (
  select distinct
    date
    , session_id
    , session_client_id
    , concat(product_vertical
      , "_"
      , model) as model_vertical
    , sum(clicks) as clicks
    , sum(tracked_clicks) as tracked_clicks
    , sum(bookings) as bookings
    , sum(revenue) as revenue
  from revenue_events
  group by 1,2,3,4
  having session_id not like "%python-requests%"
  )

, search_flights as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "flights" as product_vertical
      , "searches" as model
      , 1 as searches
    from
      `wego-cloud.wego_analytics.flights_searches`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, search_hotels as (
    select
      session_id
      , concat(session_id, client_id) as session_client_id
      , date(created_at) as date
      , "hotels" as product_vertical
      , "searches" as model
      , 1 as searches
    from
      `wego-cloud.wego_analytics.hotels_searches`
    where
      date(_partitiontime) = (select target_date from date_param)
  )

, search_events as (
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , searches
    from search_flights

    union all
    select
      session_id
    , session_client_id
    , date
    , product_vertical
    , model
    , searches
    from search_hotels
  )

, searches as (
  select
    date
    , session_id
    , session_client_id
    , concat(product_vertical
      , "_"
      , model) as model_vertical
    , sum(searches) as searches
  from search_events
  group by 1,2,3,4
  having session_id not like "%python-requests%"
  )

, session_rev_flags as (
    select
      r.session_client_id
    , r.date
    , logical_or(coalesce(r.clicks,  0) > 0) as has_clicks
    , logical_or(coalesce(r.revenue, 0) > 0) as has_revenue
    from revenue as r
    group by 1,2
  )

select
  sessions.date
  , client_type
  , device_type
  , os_type
  , app_version
  , user_country_code
  , site_code
  , locale
  , channel
  , wg_source
  , wg_medium
  , wg_campaign
  -- new fields:
  , wg_adgroup
  , wg_content
  , wg_term
  , wg_misc
  , device_version
  , os_version
  , network_type
  , network_carrier_name
  , ts_code
  , source
  , market
  , app_rt_source
  , app_rt_medium
  , app_rt_campaign
  , app_rt_adgroup
  , page_type
  , page_subtype
  , concat(regexp_extract(page_url, r"(?:[a-zA-Z]+://)?([a-zA-Z0-9-.]+)/?"), "/", coalesce(regexp_extract(page_url, r"(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+)/{1}([a-zA-Z0-9-./]+)"), "")) as page_host_path
  , regexp_extract(page_url, r"(?:[a-zA-Z]+://)?([a-zA-Z0-9-.]+)/?") as page_host
  , regexp_extract(page_url, r"(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+)/{1}([a-zA-Z0-9-./]+)") as page_path
  , new_returning_status
  -- new fields 2023-11-20:
  , device_brand
  , device_model
  , device_name
  , ua_source
  , ua_medium
  , ua_campaign
  , ua_adgroup
  , ua_content
  , count(distinct sessions.session_client_id) as sessions
  , count(distinct
    case
      when lower(page_type) like "%flight%" then sessions.session_client_id
      when searches.model_vertical = "flights_searches"
    and searches.searches > 0 then sessions.session_client_id
      when revenue.model_vertical like "%flights%" and revenue.clicks > 0 then sessions.session_client_id
  end
    ) as flights_sessions
  , count(distinct
    case
      when lower(page_type) like "%hotel%" then sessions.session_client_id
      when searches.model_vertical = "hotels_searches"
    and searches.searches > 0 then sessions.session_client_id
      when revenue.model_vertical like "%hotels%" and revenue.clicks > 0 then sessions.session_client_id
  end
    ) as hotels_sessions
  , count(distinct
  if(searches.searches > 0, sessions.session_client_id, null)) as searched_sessions
  , count(distinct
  if(searches.model_vertical = "flights_searches"
      and searches.searches > 0, sessions.session_client_id, null)) as flights_searched_sessions
  , count(distinct
  if(searches.model_vertical = "hotels_searches"
      and searches.searches > 0, sessions.session_client_id, null)) as hotels_searched_sessions
  , count(distinct
  if(revenue.clicks > 0, sessions.session_client_id, null)) as overall_clicked_sessions
  -- new fields:
  , count(distinct
  if(revenue.model_vertical like "%meta%"
      or revenue.model_vertical like "%BOW%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as meta_bow_clicked_sessions
  , count(distinct
  if(revenue.model_vertical like "%meta%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as meta_clicked_sessions
  , count(distinct
  if(revenue.model_vertical like "%BOW%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as bow_clicked_sessions
  , count(distinct
  if(revenue.model_vertical like "%CAU%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as cau_clicked_sessions
  , count(distinct
  if(pageviews.search_results_views is not null, sessions.session_client_id, null)) as search_results_sessions
  , count(distinct
  if(pageviews.flights_search_results_views is not null, sessions.session_client_id, null)) as flights_search_results_sessions
  , count(distinct
  if(pageviews.hotels_search_results_views is not null, sessions.session_client_id, null)) as hotels_search_results_sessions
  , count(distinct
  if(pageviews.detail_views is not null, sessions.session_client_id, null)) as detail_sessions
  , count(distinct
  if(pageviews.flights_detail_views is not null, sessions.session_client_id, null)) as flights_detail_sessions
  , count(distinct
  if(pageviews.hotels_detail_views is not null, sessions.session_client_id, null)) as hotels_detail_sessions
  , count(distinct
  if(pageviews.bow_checkout_views is not null, sessions.session_client_id, null)) as bow_checkout_sessions
  , count(distinct
  if(pageviews.bow_flights_checkout_views is not null, sessions.session_client_id, null)) as bow_flights_checkout_sessions
  , count(distinct
  if(pageviews.bow_hotels_checkout_views is not null, sessions.session_client_id, null)) as bow_hotels_checkout_sessions
  , count(distinct
  if(pageviews.bow_booking_success_views is not null, sessions.session_client_id, null)) as bow_booking_success_sessions
  , count(distinct
  if(pageviews.bow_flights_booking_success_views is not null, sessions.session_client_id, null)) as bow_flights_booking_success_sessions
  , count(distinct
  if(pageviews.bow_hotels_booking_success_views is not null, sessions.session_client_id, null)) as bow_hotels_booking_success_sessions
  , count(distinct
  if(pageviews.session_logged_in is not null, sessions.session_client_id, null)) as logged_in_sessions
  -- new new fields:
  , count(distinct
  if(revenue.model_vertical in ("flights_meta"
        , "flights_BOW")
      and revenue.clicks > 0, sessions.session_client_id, null)) as flights_meta_bow_clicked_sessions
  , count(distinct
  if(revenue.model_vertical in ("hotels_meta"
        , "hotels_BOW")
      and revenue.clicks > 0, sessions.session_client_id, null)) as hotels_meta_bow_clicked_sessions
  , count(distinct
  if(revenue.model_vertical = "flights_CAU"
      and revenue.clicks > 0, sessions.session_client_id, null)) as flights_cau_clicked_sessions
  , count(distinct
  if(revenue.model_vertical = "hotels_CAU"
      and revenue.clicks > 0, sessions.session_client_id, null)) as hotels_cau_clicked_sessions
  , count(distinct
  if(revenue.model_vertical like "%flights%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as flights_overall_clicked_sessions
  , count(distinct
  if(revenue.model_vertical like "%hotels%"
      and revenue.clicks > 0, sessions.session_client_id, null)) as hotels_overall_clicked_sessions
  , count(distinct
  if(revenue.bookings > 0, sessions.session_client_id, null)) as booked_sessions
  , count(distinct
  if(revenue.model_vertical like "%flights%"
      and revenue.bookings > 0, sessions.session_client_id, null)) as flights_booked_sessions
  , count(distinct
  if(revenue.model_vertical = "flights_BOW"
      and revenue.bookings > 0, sessions.session_client_id, null)) as flights_bow_booked_sessions
      , count(distinct
  if(revenue.model_vertical = "hotels_BOW"
      and revenue.bookings > 0, sessions.session_client_id, null)) as hotels_bow_booked_sessions
  , count(distinct
  if(revenue.model_vertical like "%hotels%"
      and revenue.bookings > 0, sessions.session_client_id, null)) as hotels_booked_sessions
  , count(distinct
  if(bounced_sessions = 1, sessions.session_client_id, null)) as bounced_sessions
  , count(distinct sessions.client_id) as users
  , sum(
  if(searches.model_vertical = "flights_searches", searches.searches, 0)) as flights_searches
  , sum(
  if(searches.model_vertical = "hotels_searches", searches.searches, 0)) as hotels_searches
  , sum(searches.searches) as total_searches
  , sum(
  if(revenue.model_vertical = "flights_meta", revenue.clicks, 0)) as flights_meta_clicks
  , sum(
  if(revenue.model_vertical = "hotels_meta", revenue.clicks, 0)) as hotels_meta_clicks
  , sum(
  if(revenue.model_vertical = "flights_BOW", revenue.clicks, 0)) as flights_bow_clicks
  , sum(
  if(revenue.model_vertical = "hotels_BOW", revenue.clicks, 0)) as hotels_bow_clicks
  , sum(
  if(revenue.model_vertical = "flights_CAU", revenue.clicks, 0)) as flights_cau_clicks
  , sum(
  if(revenue.model_vertical = "hotels_CAU", revenue.clicks, 0)) as hotels_cau_clicks
  , sum(
  if(revenue.model_vertical = "flights_meta"
      or revenue.model_vertical = "flights_BOW"
      or revenue.model_vertical = "flights_CAU", revenue.clicks, 0)) as flights_total_clicks
  , sum(
  if(revenue.model_vertical = "hotels_meta"
      or revenue.model_vertical = "hotels_BOW"
      or revenue.model_vertical = "hotels_CAU", revenue.clicks, 0)) as hotels_total_clicks
  , sum(clicks) as total_clicks
  , sum(
  if(revenue.model_vertical = "flights_meta", revenue.tracked_clicks, 0)) as flights_meta_tracked_clicks
  , sum(
  if(revenue.model_vertical = "hotels_meta", revenue.tracked_clicks, 0)) as hotels_meta_tracked_clicks
  , sum(
  if(revenue.model_vertical = "flights_BOW", revenue.tracked_clicks, 0)) as flights_bow_tracked_clicks
  , sum(
  if(revenue.model_vertical = "hotels_BOW", revenue.tracked_clicks, 0)) as hotels_bow_tracked_clicks
  , sum(
  if(revenue.model_vertical = "flights_meta"
      or revenue.model_vertical = "flights_BOW", revenue.tracked_clicks, 0)) as flights_total_tracked_clicks
  , sum(
  if(revenue.model_vertical = "hotels_meta"
      or revenue.model_vertical = "hotels_BOW", revenue.tracked_clicks, 0)) as hotels_total_tracked_clicks
  , sum(tracked_clicks) as total_tracked_clicks
  , sum(
  if(revenue.model_vertical = "flights_meta", revenue.bookings, 0)) as flights_meta_bookings
  , sum(
  if(revenue.model_vertical = "hotels_meta", revenue.bookings, 0)) as hotels_meta_bookings
  , sum(
  if(revenue.model_vertical = "flights_BOW", revenue.bookings, 0)) as flights_bow_bookings
  , sum(
  if(revenue.model_vertical = "hotels_BOW", revenue.bookings, 0)) as hotels_bow_bookings
  , sum(
  if(revenue.model_vertical = "flights_meta"
      or revenue.model_vertical = "flights_BOW", revenue.bookings, 0)) as flights_total_bookings
  , sum(
  if(revenue.model_vertical = "hotels_meta"
      or revenue.model_vertical = "hotels_BOW", revenue.bookings, 0)) as hotels_total_bookings
  , sum(bookings) as total_bookings
  , sum(
  if(revenue.model_vertical = "flights_meta", revenue.revenue, 0)) as flights_meta_revenue
  , sum(
  if(revenue.model_vertical = "hotels_meta", revenue.revenue, 0)) as hotels_meta_revenue
  , sum(
  if(revenue.model_vertical = "flights_BOW", revenue.revenue, 0)) as flights_bow_revenue
  , sum(
  if(revenue.model_vertical = "hotels_BOW", revenue.revenue, 0)) as hotels_bow_revenue
  , sum(
  if(revenue.model_vertical = "flights_CAU", revenue.revenue, 0)) as flights_cau_revenue
  , sum(
  if(revenue.model_vertical = "hotels_CAU", revenue.revenue, 0)) as hotels_cau_revenue
  , sum(
  if(revenue.model_vertical = "flights_meta"
      or revenue.model_vertical = "flights_BOW"
      or revenue.model_vertical = "flights_CAU", revenue.revenue, 0)) as flights_total_revenue
  , sum(
  if(revenue.model_vertical = "hotels_meta"
      or revenue.model_vertical = "hotels_BOW"
      or revenue.model_vertical = "hotels_CAU", revenue.revenue, 0)) as hotels_total_revenue
  , sum(revenue.revenue) as total_revenue
  -- bot-awareness dimensions, positioned 114-116 to mirror the physical table
  -- (added there by `alter table ... add column`)
  , case
    when sessions.cf_bot_score is null then "unscored"
    when sessions.cf_bot_score <= 10 then "01-10"
    when sessions.cf_bot_score <= 20 then "11-20"
    when sessions.cf_bot_score <= 29 then "21-29"
    when sessions.cf_bot_score <= 49 then "30-49"
    when sessions.cf_bot_score <= 60 then "50-60"
    when sessions.cf_bot_score <= 79 then "61-79"
    else "80-99"
  end as bot_score_bucket
  , coalesce(session_rev_flags.has_clicks, false) as has_clicks
  , coalesce(session_rev_flags.has_revenue, false) as has_revenue
from sessions
left join revenue
  on  sessions.session_client_id = revenue.session_client_id
  and sessions.date              = revenue.date
left join searches
  on  sessions.session_client_id = searches.session_client_id
  and sessions.date              = searches.date
left join pageviews
  on  sessions.session_client_id = pageviews.session_client_id
  and sessions.date              = pageviews.date
left join session_rev_flags
  on  sessions.session_client_id = session_rev_flags.session_client_id
  and sessions.date              = session_rev_flags.date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,114,115,116
{% endraw %}
