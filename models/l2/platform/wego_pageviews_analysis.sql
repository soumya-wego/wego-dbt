{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : wego_pageviews_analysis_daily_append
-- Destination: analysis.wego_pageviews_analysis  (unchanged)
-- Schedule   : every day 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_genzo', 'events_actions') }}
-- depends_on: {{ source('wego_analytics', 'clients') }}
-- depends_on: {{ source('wego_analytics', 'pageviews') }}
-- depends_on: {{ source('wego_analytics', 'sessions') }}
{% raw %}
-- -- Backfill:
-- create table 
-- analysis.wego_pageviews_analysis
-- partition by date(created_at)
-- as
-- WITH wa_pageviews AS
--   (SELECT DISTINCT created_at,
--                    client_created_at,
--                    DATE(_PARTITIONTIME) AS date_partitiontime,
--                    pageview_id,
--                    session_id,
--                    client_id,
--                    event_id,
--                    LAG(event_id, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS former_event_id,
--                    site_code,
--                    locale,
--                    -- SPLIT(page_url, '?')[safe_ordinal(1)] AS page_url_split,
--                    page_name,
--                    page_type,
--                    page_subtype,
--                    LAG(page_name, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_name,
--                    LAG(page_type, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_type,
--                    LAG(page_subtype, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_subtype,
--                    LEAD(page_name, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_name,
--                    LEAD(page_type, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_type,
--                    LEAD(page_subtype, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_subtype,
--                    first_value(page_type) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS first_page_type,
--                    first_value(page_subtype) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS first_page_subtype,
--                    last_value(page_type) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS last_page_type,
--                    last_value(page_subtype) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS last_page_subtype,
--                    page_url,
--                    referrer_url,
--                    user_hash,
--                    product,
--                    advertiser_id
--                    -- LEAD(page_url, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at) AS following_url,
--    FROM `wego-cloud.wego_analytics.pageviews`
--    WHERE DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--          ),

--      sessions AS
--   (SELECT DISTINCT created_at,
--           session_id,
--           user_city,
--           user_country_code,
--           market,
--           channel,
--           if(device_type like "%HP%", "android-app-preload", device_type) as device_type,
--           app_version,
--           ts_code,
--           wg_source,
--           wg_medium,
--           wg_campaign,
--           source,
--           landing_url as session_landing_url
--    FROM `wego-cloud.wego_analytics.sessions`
--    WHERE DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--          ),

--        clients AS
--   (SELECT DISTINCT created_at,
--           client_id,
--           if(first_visit is true, "new", "return") as new_vs_returning_status
--    FROM `wego-cloud.wego_analytics.clients`
--    WHERE DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--          ),

--          actions AS
--   (SELECT DISTINCT 
--           TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
--           id as event_action_id,
--           event.id as event_id,
--           event.category as event_category,
--           event.object as event_object,
--           event.action as event_action,
--           event.value as event_value,
--    FROM `wego-cloud.services_genzo.events_actions*`
--    WHERE _TABLE_SUFFIX BETWEEN "20210101" AND (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--          )

--   SELECT pv.created_at,
--           pv.client_created_at,
--           pv.pageview_id, 
--           pv.session_id,
--           ss.user_city,
--           ss.user_country_code,
--           ss.market,
--           pv.site_code,
--           pv.locale,
--           ss.channel,
--           ss.device_type,
--           ss.ts_code,
--           ss.app_version,
--           ss.wg_source,
--           ss.wg_medium,
--           ss.wg_campaign,
--           ss.source,
--           ss.session_landing_url,
--           cl.new_vs_returning_status,
--           pv.* EXCEPT(created_at, client_created_at, pageview_id, session_id, site_code, locale, date_partitiontime, user_hash, product, advertiser_id, page_url, referrer_url),
--           if(page_url like "%https://play.google%", null, page_url) as page_url,
--           if(referrer_url like "%https://play.google%", null, referrer_url) as referrer_url,
--           a.* EXCEPT(created_at, event_id),
--           pv.user_hash,
--           pv.product,
--           pv.advertiser_id
--    FROM wa_pageviews AS pv 
--    LEFT JOIN sessions AS ss ON pv.session_id = ss.session_id AND DATE(pv.created_at) = DATE(ss.created_at)
--    LEFT JOIN clients AS cl ON  cl.client_id = pv.client_id AND DATE(pv.created_at) = DATE(cl.created_at)
--    LEFT JOIN actions AS a ON  a.event_id = pv.pageview_id AND DATE(a.created_at) = DATE(pv.created_at)


-- Daily append:

WITH wa_pageviews AS
  (SELECT DISTINCT created_at,
                   client_created_at,
                   DATE(_PARTITIONTIME) AS date_partitiontime,
                   pageview_id,
                   session_id,
                   client_id,
                   event_id,
                   LAG(event_id, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS former_event_id,
                   site_code,
                   locale,
                   -- SPLIT(page_url, '?')[safe_ordinal(1)] AS page_url_split,
                   page_name,
                   page_type,
                   page_subtype,
                   LAG(page_name, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_name,
                   LAG(page_type, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_type,
                   LAG(page_subtype, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS referrer_page_subtype,
                   LEAD(page_name, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_name,
                   LEAD(page_type, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_type,
                   LEAD(page_subtype, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS following_page_subtype,
                   first_value(page_type) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS first_page_type,
                   first_value(page_subtype) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS first_page_subtype,
                   last_value(page_type) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS last_page_type,
                   last_value(page_subtype) OVER(PARTITION BY client_id, session_id ORDER BY created_at, client_created_at) AS last_page_subtype,
                   page_url,
                   referrer_url,
                   user_hash,
                   product,
                   advertiser_id
                   -- LEAD(page_url, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at) AS following_url,
   FROM `wego-cloud.wego_analytics.pageviews`
   WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
         ),

     sessions AS
  (SELECT distinct created_at,
          session_id,
          user_city,
          user_country_code,
          market,
          channel,
          if(device_type like "%HP%", "android-app-preload", device_type) as device_type,
          app_version,
          ts_code,
          wg_source,
          wg_medium,
          wg_campaign,
          source,
          landing_url as session_landing_url
   FROM `wego-cloud.wego_analytics.sessions`
   WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
         ),

       clients AS
  (SELECT created_at,
          client_id,
          if(first_visit is true, "new", "return") as new_vs_returning_status
   FROM `wego-cloud.wego_analytics.clients`
   WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
         ),

       actions AS
  (SELECT distinct 
          TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
          id as event_action_id,
          event.id as event_id,
          event.category as event_category,
          event.object as event_object,
          event.action as event_action,
          event.value as event_value,
   FROM `wego-cloud.services_genzo.events_actions*`
   WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
         )

  SELECT pv.created_at,
          pv.client_created_at,
          pv.pageview_id, 
          pv.session_id,
          ss.user_city,
          ss.user_country_code,
          ss.market,
          pv.site_code,
          pv.locale,
          ss.channel,
          ss.device_type,
          ss.ts_code,
          ss.app_version,
          ss.wg_source,
          ss.wg_medium,
          ss.wg_campaign,
          ss.source,
          ss.session_landing_url,
          cl.new_vs_returning_status,
          pv.* EXCEPT(created_at, client_created_at, pageview_id, session_id, site_code, locale, date_partitiontime, user_hash, product, advertiser_id, page_url, referrer_url),
          if(page_url like "%https://play.google%", null, page_url) as page_url,
          if(referrer_url like "%https://play.google%", null, referrer_url) as referrer_url,
          a.* EXCEPT(created_at, event_id),
          pv.user_hash,
          pv.product,
          pv.advertiser_id
   FROM wa_pageviews AS pv 
   LEFT JOIN sessions AS ss ON pv.session_id = ss.session_id AND DATE(pv.created_at) = DATE(ss.created_at)
   LEFT JOIN clients AS cl ON  cl.client_id = pv.client_id AND DATE(pv.created_at) = DATE(cl.created_at)
   LEFT JOIN actions AS a ON  a.event_id = pv.pageview_id AND DATE(a.created_at) = DATE(pv.created_at)
{% endraw %}
