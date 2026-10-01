{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : client_session_aggregated_delsert
-- Destination: analysis.clients_sessions_aggregated_daily  (unchanged)
-- Schedule   : every day 00:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
declare win_start date default date_sub(current_date(), interval 7 day);
declare win_end   date default date_sub(current_date(), interval 1 day);

merge `wego-cloud.analysis.clients_sessions_aggregated_daily` t
using (

  -- one row per (client_id, session_id, created_at_date) across the window.
  -- this is exactly what seven consecutive nightly runs of the old daily logic
  -- would produce, but computed against today's (restated) source data.
  with sessions_window as (
    select
      session_id
      , client_id
      , date(created_at) as created_at_date
      , created_at
      , device_type
      , os_type
      , app_version
      , user_country_code
      , user_city
      , referrer_url
      , site_code
      , locale
      , channel
      , wg_source
      , wg_medium
      , wg_campaign
      , wg_adgroup
      , wg_content
      , ts_code
      , source
      , market
      , app_rt_source
      , app_rt_medium
      , app_rt_campaign
      , app_rt_adgroup
      , app_rt_content
      , cf_bot_score
    from `wego-cloud.wego_analytics.sessions`
    where date(_partitiontime) between win_start and win_end
      and created_at is not null
    qualify row_number() over (
      partition by client_id, session_id, date(created_at)
      order by created_at asc
    ) = 1
  )

  -- ------------------------------------------------------------ searches --

  , flights_searches_leg as (
    select
      client_id
      , session_id
      , date(_partitiontime) as d
      , count(distinct search_id) as flights_searches
      , 0 as hotels_searches
    from `wego-cloud.wego_analytics.flights_searches`
    where date(_partitiontime) between win_start and win_end
    group by 1, 2, 3
  )

  , hotels_searches_leg as (
    select
      client_id
      , session_id
      , date(_partitiontime) as d
      , 0 as flights_searches
      , count(distinct search_id) as hotels_searches
    from `wego-cloud.wego_analytics.hotels_searches`
    where date(_partitiontime) between win_start and win_end
    group by 1, 2, 3
  )

  , searches as (
    select
      client_id
      , session_id
      , d
      , sum(flights_searches) as flights_searches
      , sum(hotels_searches) as hotels_searches
      , sum(flights_searches + hotels_searches) as total_searches
    from (
      select * from flights_searches_leg
      union all
      select * from hotels_searches_leg
    )
    group by 1, 2, 3
  )

  -- --------------------------------------------- clicks and bookings legs --

  , flights_clicks_leg as (
    select
      client_id
      , session_id
      , date(_partitiontime) as d
      , "flights" as product
      , count(distinct click_id) as clicks
      , count(distinct if(provider_code != "wego.com" and conversions_tracked > 0, click_id, null)) as bookings
    from `wego-cloud.wego_analytics.flights_clicks`
    where date(_partitiontime) between win_start and win_end
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1, 2, 3
  )

  , hotels_clicks_leg as (
    select
      client_id
      , session_id
      , date(_partitiontime) as d
      , "hotels" as product
      , count(distinct click_id) as clicks
      , count(distinct if(provider_code != "hotels.wego.com" and conversions_tracked > 0, click_id, null)) as bookings
    from `wego-cloud.wego_analytics.hotels_clicks`
    where date(_partitiontime) between win_start and win_end
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1, 2, 3
  )

  , bow_flights_leg as (
    select
      client_id
      , session_id
      , date(_partitiontime) as d
      , "BOW flights" as product
      , 0 as clicks
      , sum(conversions_adjusted) as bookings
    from `wego-cloud.wego_analytics.flights_bookings`
    where date(_partitiontime) between win_start and win_end
      and conversions_adjusted > 0
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1, 2, 3
  )

  , bow_hotels_leg as (
    select
      client_id
      , session_id
      , date(created_at) as d
      , "BOW hotels" as product
      , 0 as clicks
      , sum(conversions_adjusted) as bookings
    from `wego-cloud.wego_analytics.hotels_bookings`
    where date(created_at) between win_start and win_end
      and conversions_adjusted > 0
      and client_id is not null
      and client_id not in ('', "00000000-0000-0000-0000-000000000000")
    group by 1, 2, 3
  )

  , clicks_sessions_aggregated as (
    select * from flights_clicks_leg
    union all
    select * from hotels_clicks_leg
    union all
    select * from bow_flights_leg
    union all
    select * from bow_hotels_leg
  )

  , clicks_sessions_aggregated_final as (
    select
      session_id
      , client_id
      , d
      , sum(clicks) as total_clicks
      , sum(bookings) as total_bookings
      , sum(if(product = "flights", clicks, 0)) as flights_clicks
      , sum(if(product = "flights", bookings, 0)) + sum(if(product = "BOW flights", bookings, 0)) as total_flights_bookings
      , sum(if(product = "flights", bookings, 0)) as meta_flights_bookings
      , sum(if(product = "BOW flights", bookings, 0)) as BOW_flights_bookings
      , sum(if(product = "hotels", clicks, 0)) as hotels_clicks
      , sum(if(product = "hotels", bookings, 0)) + sum(if(product = "BOW hotels", bookings, 0)) as total_hotels_bookings
      , sum(if(product = "hotels", bookings, 0)) as meta_hotels_bookings
      , sum(if(product = "BOW hotels", bookings, 0)) as BOW_hotels_bookings
    from clicks_sessions_aggregated
    group by 1, 2, 3
  )

  -- ------------------------- sessions that booked with no session row that day --
  -- without this, date-filtering the BOW legs silently drops bookings whose
  -- session was last seen on an earlier day. measured 1.69% of bookings.
  -- the row is dated to the BOOKING's day; attributes are inherited from the
  -- session's most recent appearance on or before it.

  , booked_but_not_seen as (
    select distinct c.client_id, c.session_id, c.d
    from clicks_sessions_aggregated c
    left join sessions_window s
      on c.client_id = s.client_id
      and c.session_id = s.session_id
      and c.d = s.created_at_date
    where c.product in ("BOW flights", "BOW hotels")
      and c.bookings > 0
      and s.client_id is null
  )

  , booked_but_not_seen_attributes as (
    select
      b.session_id
      , b.client_id
      , b.d as created_at_date
      , s.created_at
      , s.device_type, s.os_type, s.app_version, s.user_country_code, s.user_city
      , s.referrer_url, s.site_code, s.locale, s.channel
      , s.wg_source, s.wg_medium, s.wg_campaign, s.wg_adgroup, s.wg_content
      , s.ts_code, s.source, s.market
      , s.app_rt_source, s.app_rt_medium, s.app_rt_campaign, s.app_rt_adgroup, s.app_rt_content
      , s.cf_bot_score
    from booked_but_not_seen b
    join `wego-cloud.wego_analytics.sessions` s
      on b.client_id = s.client_id
      and b.session_id = s.session_id
    where date(s._partitiontime) between date_sub(win_start, interval 30 day) and win_end
      and s.created_at is not null
    qualify row_number() over (
      partition by b.client_id, b.session_id, b.d order by s.created_at desc
    ) = 1
  )

  , sessions_all as (
    select * from sessions_window
    union all
    select * from booked_but_not_seen_attributes
  )

  select
    a.*
    , ifnull(b.flights_searches, 0) as flights_searches
    , ifnull(b.hotels_searches, 0) as hotels_searches
    , ifnull(b.total_searches, 0) as total_searches
    , ifnull(c.total_clicks, 0) as total_clicks
    , ifnull(c.total_bookings, 0) as total_bookings
    , ifnull(c.flights_clicks, 0) as flights_clicks
    , ifnull(c.total_flights_bookings, 0) as total_flights_bookings
    , ifnull(c.meta_flights_bookings, 0) as meta_flights_bookings
    , ifnull(c.BOW_flights_bookings, 0) as BOW_flights_bookings
    , ifnull(c.hotels_clicks, 0) as hotels_clicks
    , ifnull(c.total_hotels_bookings, 0) as total_hotels_bookings
    , ifnull(c.meta_hotels_bookings, 0) as meta_hotels_bookings
    , ifnull(c.BOW_hotels_bookings, 0) as BOW_hotels_bookings
  from sessions_all as a
  left join searches as b
    on a.session_id = b.session_id
    and a.client_id = b.client_id
    and a.created_at_date = b.d
  left join clicks_sessions_aggregated_final as c
    on a.session_id = c.session_id
    and a.client_id = c.client_id
    and a.created_at_date = c.d

) s
on  t.client_id       = s.client_id
and t.session_id      = s.session_id
and t.created_at_date = s.created_at_date
and t.created_at_date between win_start and win_end   -- prunes the target scan

when matched then update set
    t.created_at             = s.created_at
  , t.device_type            = s.device_type
  , t.os_type                = s.os_type
  , t.app_version            = s.app_version
  , t.user_country_code      = s.user_country_code
  , t.user_city              = s.user_city
  , t.referrer_url           = s.referrer_url
  , t.site_code              = s.site_code
  , t.locale                 = s.locale
  , t.channel                = s.channel
  , t.wg_source              = s.wg_source
  , t.wg_medium              = s.wg_medium
  , t.wg_campaign            = s.wg_campaign
  , t.wg_adgroup             = s.wg_adgroup
  , t.wg_content             = s.wg_content
  , t.ts_code                = s.ts_code
  , t.source                 = s.source
  , t.market                 = s.market
  , t.app_rt_source          = s.app_rt_source
  , t.app_rt_medium          = s.app_rt_medium
  , t.app_rt_campaign        = s.app_rt_campaign
  , t.app_rt_adgroup         = s.app_rt_adgroup
  , t.app_rt_content         = s.app_rt_content
  , t.cf_bot_score           = s.cf_bot_score
  , t.flights_searches       = s.flights_searches
  , t.hotels_searches        = s.hotels_searches
  , t.total_searches         = s.total_searches
  , t.total_clicks           = s.total_clicks
  , t.total_bookings         = s.total_bookings
  , t.flights_clicks         = s.flights_clicks
  , t.total_flights_bookings = s.total_flights_bookings
  , t.meta_flights_bookings  = s.meta_flights_bookings
  , t.BOW_flights_bookings   = s.BOW_flights_bookings
  , t.hotels_clicks          = s.hotels_clicks
  , t.total_hotels_bookings  = s.total_hotels_bookings
  , t.meta_hotels_bookings   = s.meta_hotels_bookings
  , t.BOW_hotels_bookings    = s.BOW_hotels_bookings

-- !! explicit column list, NOT `insert row`. `insert row` is POSITIONAL, and
-- !! the source puts cf_bot_score next to the other session attributes
-- !! (position 27) while the target has it appended last (position 40).
-- !! with `insert row` the 1,840 inserted rows landed misaligned, which made
-- !! the MERGE non-idempotent: run 1 gave BOW 25,526, run 2 gave 26,003.
when not matched then insert (
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
  , cf_bot_score
)
values (
  s.session_id
  , s.client_id
  , s.created_at_date
  , s.created_at
  , s.device_type
  , s.os_type
  , s.app_version
  , s.user_country_code
  , s.user_city
  , s.referrer_url
  , s.site_code
  , s.locale
  , s.channel
  , s.wg_source
  , s.wg_medium
  , s.wg_campaign
  , s.wg_adgroup
  , s.wg_content
  , s.ts_code
  , s.source
  , s.market
  , s.app_rt_source
  , s.app_rt_medium
  , s.app_rt_campaign
  , s.app_rt_adgroup
  , s.app_rt_content
  , s.flights_searches
  , s.hotels_searches
  , s.total_searches
  , s.total_clicks
  , s.total_bookings
  , s.flights_clicks
  , s.total_flights_bookings
  , s.meta_flights_bookings
  , s.BOW_flights_bookings
  , s.hotels_clicks
  , s.total_hotels_bookings
  , s.meta_hotels_bookings
  , s.BOW_hotels_bookings
  , s.cf_bot_score
)
;
{% endraw %}
