{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_impressions_logs_daily_append_clustered
-- Destination: wego_analytics.hotels_searches_details_impressions_logs_clustered  (unchanged)
-- Schedule   : every day 00:45   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_genzo', 'events_actions') }}
-- depends_on: {{ source('services_genzo', 'pages_views') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
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





-- CREATE OR REPLACE TABLE `wego_analytics.hotels_searches_details_impressions_logs_clustered`
-- PARTITION BY DATE(created_at)
-- CLUSTER BY impression_page, impression_trigger, displayed_order
INSERT INTO `wego_analytics.hotels_searches_details_impressions_logs_clustered`
-- AS
-- (
  WITH run_date_impressions_log AS (
  SELECT * EXCEPT(`rn`)
  FROM (
    SELECT
    DISTINCT
    CONCAT(il.id, "-", il.search_id, "-", obj.id, "-", obj.`order`) as search_details_log_id,
    il.id as impression_log_id,
    il.created_at,
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
    ROW_NUMBER() over(partition by il.id, il.search_id, obj.id, obj.`order` order by date(il.created_at) desc) as rn
    FROM`wego-cloud.services_genzo.impressions_logs*` as il,
    UNNEST(il.objects) as obj
    WHERE il._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND DATE(il.created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND il.impression.page LIKE ("%hotels%")
  )
  WHERE rn = 1
)

SELECT DISTINCT
il.search_details_log_id,
il.impression_log_id,
il.created_at,
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

FROM run_date_impressions_log as il

LEFT JOIN `wego-cloud.services_genzo.pages_views*` as pv
ON pv.id = il.page_view_id
AND pv._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))

LEFT JOIN 

(SELECT DISTINCT
id as event_action_id,
event.id as pageview_id,
created_at as created_at, 
event.category as event_category,
event.object as event_object,
event.action as event_action,
event.value as event_value,
STRING_AGG(DISTINCT keys[SAFE_OFFSET(0)], " ," ORDER BY keys[SAFE_OFFSET(0)]) as filter_name,

FROM `wego-cloud.services_genzo.events_actions*`,
UNNEST([struct(get_leaves(format(event.value)) as leaves)]), 
UNNEST(get_keys(leaves)) key with offset

JOIN UNNEST(get_values(leaves)) val with offset using(offset),
UNNEST([struct(split(key, '.') as keys)]),
UNNEST(split(translate(val, "[]", ""), ",")) as str_array

WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
AND event.object  in  ("filter", "sort")
AND event.value like ("{%")
-- AND TIMESTAMP_TRUNC(created_at, DAY) = timestamp(date_sub(CURRENT_DATE(), interval 1 day))
AND event.id in (SELECT DISTINCT page_view_id 
FROM run_date_impressions_log 
WHERE impression_trigger in ('sort', 'filter')
AND impression_page in ("hotels_search_results")
)
GROUP BY 1,2,3,4,5,6,7
) as ea
on ea.pageview_id = il.page_view_id
AND ea.event_action_id = il.event_id
AND il.impression_trigger in ('sort', 'filter')
AND il.impression_page in ("hotels_search_results")

LEFT JOIN 

(SELECT 
session_id,
created_at as created_at,
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
FROM `wego_analytics.sessions`
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
) as ss
ON ss.session_id = il.session_id
AND date(ss.created_at) = date(il.created_at)
-- );
{% endraw %}
