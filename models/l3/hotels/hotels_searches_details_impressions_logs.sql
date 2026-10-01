{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_impressions_logs_daily_append
-- Destination: wego_analytics.hotels_searches_details_impressions_logs  (unchanged)
-- Schedule   : every day 00:30   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
create temp function get_keys(input string) returns array<string> language js as """
  return Object.keys(JSON.parse(input));
  """;
create temp function get_values(input string) returns array<string> language js as """ 
  return Object.values(JSON.parse(input));
  """;
create temp function get_leaves(input string) returns string language js as '''
  function flattenObj(obj, parent = '', res = {}){
    for(let key in obj){
        let propName = parent ? parent + '.' + key : key;
        if(typeof obj[key] == 'object'){
            flattenObj(obj[key], propName, res);
        } else {
            res[propName] = obj[key];
        }
    }
    return JSON.stringify(res);
  }
  return flattenObj(JSON.parse(input));
  ''';

create or replace temp table run_date_sessions
partition by (created_at) as (
select 
  session_id,
  date(created_at) as created_at,
  client_id, 
  advertiser_id, 
  user_agent, 
  client_type, 
  device, 
  device_type, 
  os_type, 
  app_version, 
  device_version, 
  os_version, 
  user_country_code, 
  user_city, 
  referrer_url as session_referrer_url, 
  site_code, 
  locale, 
  channel, 
  wg_source, 
  wg_medium, 
  wg_campaign, 
  wg_adgroup, 
  wg_content, 
  wg_term, 
  wg_misc, 
  ts_code
from `wego_analytics.sessions`
where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(DATE_SUB(@run_date, INTERVAL 1 DAY))
);

create or replace temp table run_date_impressions_log
partition by (created_at) as (
  select * except(`rn`)
  from (
    select
    distinct
      concat(il.id, "-", il.search_id, "-", obj.id, "-", obj.`order`) as search_details_log_id,
      il.id as impression_log_id,
      DATE(il.created_at) as created_at,
      il.search_id,
      il.client.session_id as session_id,
      il.impression.page as impression_page,
      il.impression.trigger as impression_trigger,
      il.page_view_id,
      il.event_id,
      obj.id as object_id,
      obj.parent_id as parent_id,
      obj.itinerary_category,
      obj.`order` as displayed_order,
      obj.order_label as displayed_order_label,
      obj.cheapest_price_usd,
      obj.clicked_price_usd,
      obj.num_providers,
      obj.top_provider,
      obj.clicked_provider,
      obj.ad_above, 
      row_number() over(partition by il.id, il.search_id, obj.id, obj.`order` order by date(il.created_at) desc) as rn
    from `wego-cloud.services_genzo.impressions_logs*` as il
      , unnest(il.objects) as obj

    where il._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
    and DATE(il.created_at) = DATE_SUB(@run_date, INTERVAL 1 DAY)
    and il.impression.page like ("%hotels%")           -- change this to hotels for all the impressions logs for hotel_searches and hotels_details
  )
  where rn = 1
);

-- create table wego_analytics.hotels_searches_details_impressions_logs 
-- partition by date(created_at) as
insert into `wego_analytics.hotels_searches_details_impressions_logs`
select distinct
  il.search_details_log_id,
  il.impression_log_id,
  timestamp_trunc(il.created_at, DAY) as created_at,
  il.search_id,
  -- sessions details
  ss.session_id,
  ss.client_id, 
  ss.advertiser_id, 
  ss.user_agent, 
  ss.client_type, 
  ss.device, 
  ss.device_type, 
  ss.os_type, 
  ss.app_version, 
  ss.device_version, 
  ss.os_version, 
  ss.user_country_code, 
  ss.user_city, 
  ss.session_referrer_url,
  ss.site_code, 
  ss.locale, 
  ss.channel, 
  ss.wg_source, 
  ss.wg_medium, 
  ss.wg_campaign, 
  ss.wg_adgroup, 
  ss.wg_content, 
  ss.wg_term, 
  ss.wg_misc, 
  ss.ts_code,
  -- pageview details
  pv.id as pageview_id,
  il.impression_page,
  il.impression_trigger,
  pv.event_id,
  pv.former_event_id,
  pv.page.url as page_url,
  pv.page.referrer_url as pageview_referrer_url,
  pv.page.product as product,
  pv.page.base_type as page_type,
  pv.page.sub_type as page_subtype,
  pv.page.name as page_name,
  pv.external_services.lotame_id as pageview_lotame_id,
  pv.external_services.crm_id as pageview_crm_id,
  -- event action details
  ea.event_action_id,
  ea.event_category,
  ea.event_object,
  ea.event_action,
  ea.event_value,
  ea.filter_name,
  -- impressions details
  il.object_id,
  il.parent_id as parent_id,
  il.itinerary_category,
  il.displayed_order,
  il.displayed_order_label,
  il.cheapest_price_usd,
  il.clicked_price_usd,
  il.num_providers,
  il.top_provider,
  il.clicked_provider,
  il.ad_above

from run_date_impressions_log as il

left join `wego-cloud.services_genzo.pages_views*` as pv
on pv.id = il.page_view_id
and pv._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))

left join (
  select distinct
    id as event_action_id,
    event.id as pageview_id,
    TIMESTAMP_TRUNC(created_at, DAY) as created_at, 
    event.category as event_category,
    event.object as event_object,
    event.action as event_action,
    event.value as event_value,
    string_agg(distinct keys[safe_offset(0)], " ," order by keys[safe_offset(0)]) as filter_name,

  from `wego-cloud.services_genzo.events_actions*`
    , unnest([struct(get_leaves(format(event.value)) as leaves)])
    , unnest(get_keys(leaves)) key with offset

  join unnest(get_values(leaves)) val with offset using(offset),
  unnest([struct(split(key, '.') as keys)]),
  unnest(split(translate(val, "[]", ""), ",")) as str_array

  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
  and event.object  in  ("filter", "sort")
  and event.value like ("{%")
  -- and TIMESTAMP_TRUNC(created_at, DAY) = timestamp(date_sub(@run_date, interval 1 day))
  and event.id in (
    select distinct page_view_id 
    from run_date_impressions_log 
    where impression_trigger in ('sort', 'filter')
    and impression_page in ("hotels_search_results")
  )
  group by 1, 2, 3, 4, 5, 6, 7
) as ea
on ea.pageview_id = il.page_view_id
and ea.event_action_id = il.event_id
and il.impression_trigger in ('sort', 'filter')
and il.impression_page in ("hotels_search_results")

left join run_date_sessions as ss
on ss.session_id = il.session_id
and date(ss.created_at) = date(il.created_at)
{% endraw %}
