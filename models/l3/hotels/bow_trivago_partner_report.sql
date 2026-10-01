{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_trivago_partner_report
-- Destination: analysis.bow_trivago_partner_report  (unchanged)
-- Schedule   : every day 10:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create or replace table `wego-cloud.analysis.bow_trivago_partner_report` 
-- partition by report_date as
with date_param as
(
  -- select date("2026-03-24") as dates
  select current_date - interval '1' day as dates
)

, meta as (
  select
    parse_date('%Y%m%d', `date ymd`) as report_date
    , cast(partnerref as string) as partner_property_id
    , cast(trivagoid as string) as trivago_property_id
    , `hotel name` as hotel_name
    , region
    , country
    , city
    , stars
    , rating
    , pos
    , `traffic source` as traffic_source
    , `partner id` as partner_id
    , `bidding type` as bidding_type
    , `property group` as property_group
    , `cpa input` as cpa_input
    , `modifier applied` as modifier_applied
    , `ttt breakout` as days_to_travel
    , `ttt modifier` as days_to_travel_modifier
    , `los breakout` as los_breakout
    , `los modifier` as los_modifier
    , `group size breakout` as group_size_breakout
    , `group size modifier` as group_size_modifier
    , `default date breakout` as default_date_breakout
    , `default date modifier` as default_date_modifier
    , `base bid` as base_bid
    , clicks
    , bookings
    , `booking amount` as booking_amount
    , `total cost modified` as total_cost_modified
    , `hotel impr` as hotel_impr
    , `advertiser impr` as advertiser_impr
    , `top position impr` as top_position_impr
    , `beat impr` as beat_impr
    , `meet impr` as meet_impr
    , `lose imp` as lose_impr
    , `unavailability impr` as unavailability_impr
    , `outbid impr` as outbid_impr
    , `beat vs champ imp` as beat_vs_champ_impr
    , `meet vs champ imp` as meet_vs_champ_impr
    , `lose vs champ imp` as lose_vs_champ_impr
    , `slideout impressions` as slideout_impressions
    , averagecpc
    , maximumcpc
  from `wego-cloud.distribution_partner_reports_hotels.trivago_meta_performance_report`
  where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = (select TIMESTAMP(dates) from date_param) 
  --  >= timestamp('2025-11-01')
  -- = TIMESTAMP(current_date - interval 1 day) 
)

, fx_rates as (
  select
    date(effective) as effective
    , amount
    , _table_suffix
  from `wego-cloud.analytics.exchange_rates*`
  where _table_suffix =  (select format_date('%Y%m%d',dates) from date_param)
  --  format_date('%Y%m%d', current_date - interval 1 day)
  and base = 'EUR'
  and quote = 'USD'
)

, rate_insights as (
  select
    report_date
    , trivago_property_id
    , partner_property_id
    , pos
    , avg(cheapest_price_usd) as cheapest_price_usd
    , avg(wego_price_usd) as wego_price_usd
    , avg(price_difference_usd) as price_difference_usd
  from (
    select
      date(_PARTITIONTIME) as report_date
      , cast(trivagoid as string) as trivago_property_id
      , cast(partnerref as string) as partner_property_id
      , `cheapest market rate`/100*amount as cheapest_price_usd
      , pos
      , (`cheapest market rate` + `accommodation rate difference`)/100*amount as wego_price_usd
      , `accommodation rate difference`/100*amount as price_difference_usd
    from `wego-cloud.distribution_partner_reports_hotels.trivago_rate_insights_report`
    left join fx_rates
      on effective = date(_PARTITIONTIME) 
    where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = (select TIMESTAMP(dates) from date_param) 
    --  = TIMESTAMP(current_date - interval 1 day) 
  )
  group by 1,2,3,4
)

select
  m.report_date
  , m.trivago_property_id
  , m.partner_property_id
  , m.hotel_name
  , m.region
  , m.country
  , m.city
  , m.stars
  , m.rating
  , m.pos
  , m.traffic_source

  , m.bidding_type
  , m.property_group
  , m.cpa_input
  , m.modifier_applied
  , m.days_to_travel
  , m.days_to_travel_modifier
  , m.los_breakout
  , m.los_modifier
  , m.group_size_breakout
  , m.group_size_modifier
  , m.default_date_breakout
  , m.default_date_modifier
  , m.base_bid
  , m.clicks
  , m.bookings
  , m.booking_amount
  , m.total_cost_modified
  , m.hotel_impr
  , m.advertiser_impr
  , m.top_position_impr
  , m.beat_impr
  , m.meet_impr
  , m.lose_impr
  , m.unavailability_impr
  , m.outbid_impr
  , m.beat_vs_champ_impr
  , m.meet_vs_champ_impr
  , m.lose_vs_champ_impr
  , m.slideout_impressions
  , m.averagecpc
  , m.maximumcpc

  , r.cheapest_price_usd
  , r.wego_price_usd
  , r.price_difference_usd
from meta m
left join rate_insights r
  on m.report_date = r.report_date
  and m.trivago_property_id = r.trivago_property_id
  and m.pos = r.pos
{% endraw %}
