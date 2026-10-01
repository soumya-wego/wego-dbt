{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : shopcash_pageviews_analysis_daily_append
-- Destination: shopcash_analytics.shopcash_pageviews_analysis  (unchanged)
-- Schedule   : every day 01:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- Backfill:
-- create or replace table
-- shopcash_analytics.shopcash_pageviews_analysis
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
--                    CASE WHEN user_hash IS NOT NULL THEN 1
--                    ELSE 0 END AS logged_in_user,
--                    product,
--                    advertiser_id
--                    -- LEAD(page_url, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at) AS following_url,
--    FROM `wego-cloud.shopcash_analytics.pageviews`
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
--           utm_source,
--           utm_medium,
--           utm_campaign,
--           source
--    FROM `wego-cloud.shopcash_analytics.sessions`
--    WHERE DATE(_PARTITIONTIME) BETWEEN '2021-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
--          ),
 
--          actions AS
--   (SELECT * EXCEPT(deal_type, banner_collection_type),
--   CASE WHEN deal_type LIKE 'without%' THEN 'without_coupon'
--   WHEN LOWER(deal_type) LIKE '%agnostic' THEN 'coupon_agnostic'
--   WHEN deal_type in ('with-coupon', 'withCoupon') THEN 'with_coupon'
--   ELSE deal_type END AS deal_type,
--   CASE WHEN banner_collection_type IN ("mid-page", "mid_page", "mid-page-2", "mid_page-2") THEN "mid_page"
--   ELSE banner_collection_type END AS banner_collection_type
 
 
--   FROM
--     (SELECT DISTINCT
--           TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
--           event.id as event_id,
--           CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search_bar'
--           ELSE event.category END as event_category,
--           CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search'
--           ELSE event.object END as event_object,
--           CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search_result'
--           ELSE event.action END as event_action,
--           event.value as event_value,
--           CASE WHEN event.category = 'login' THEN event.value END AS login_step,
--           CASE WHEN event.category = 'wallet' AND event.object = 'referral' AND event.action = 'click' THEN event.value END AS referral_code,
--           CASE WHEN event.category = 'wallet' AND event.object IN ('cashback_history', 'click_history') AND event.action = 'click' THEN event.value END AS cashback_history_accordion_date_tracked,
--           CASE WHEN event.category = 'wallet' AND event.object = 'withdrawal_method' AND event.action = 'click' THEN event.value END AS withdrawal_method,
--           CASE WHEN event.category = 'withdraw' THEN event.value END AS withdrawal_step,
--           CASE WHEN event.category = 'notifications' AND event.object IN ('icon', 'button') AND event.action = 'click' THEN event.value END AS notification_value,
--           CASE WHEN event.category = 'account' AND event.object = 'receive_email' AND event.action = 'click' THEN event.value END AS email_preferences,
--           CASE WHEN event.category = 'home' AND event.object = 'user_education' AND event.action = 'click' THEN event.value END AS video_url,
--           CASE WHEN event.category IN ('home', 'coupons') AND event.object = 'category_list' THEN event.value END AS category_id,
--           CASE WHEN event.category IN ('home', 'deals') AND event.object = 'all_deals' AND event.action = 'load_more' THEN event.value END AS all_deals_load_more_click,
--           CASE WHEN event.category = 'home' AND event.object = 'store_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.store_collection') END AS store_collection_id,
--           CASE WHEN event.category = 'home' AND event.object = 'banner_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.banner_collection') END AS banner_collection_type,
--           CASE WHEN event.category = 'home' AND event.object = 'banner_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.banner') END AS banner_id,
--           CASE WHEN event.category = 'home' AND event.object = 'deal_collections' AND JSON_EXTRACT_SCALAR(event.value, '$.deal_collection') != '' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal_collection') END AS deal_type,
--           CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.type') END AS search_result_type,
--           CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result') END AS search_result_clicked,
 
        
--           CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'type' THEN event.value
--                WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_term')
--                END AS search_term,
 
 
--           CASE WHEN event.category = 'store' AND event.object IN ('visit_store', 'shop_now') AND event.action = 'click' THEN event.value
--                WHEN event.category = 'home' AND event.object IN ('category_list_stores', 'recent', 'discover_stores', 'trending_stores') THEN event.value
--                WHEN event.category = 'home' AND event.object = 'store_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.store')
--                WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' AND JSON_EXTRACT_SCALAR(event.value, '$.type') = 'store' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result')
--                WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN event.value
--                END AS store_id,
 
        
--           CASE WHEN event.category = 'store' AND event.object IN ('code_get', 'deal_get') AND event.action = 'click' THEN event.value
--                WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
--                WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
--                WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
--                WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
--                WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' AND JSON_EXTRACT_SCALAR(event.value, '$.type') = 'deal' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result')
--                END AS deal_id,
 
--           CASE WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
--                WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
--                WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
--                WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
--                END AS deal_cashback,
 
--           CASE WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
--                WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
--                WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
--                WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
--                END AS deal_modal_action,
 
--           CASE WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.has_code')
--                WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.has_code')
--                END AS deal_has_code
--    FROM `wego-cloud.services_shopcash.events_actions*`
--    WHERE _TABLE_SUFFIX BETWEEN "20210101" AND (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
--          )),
 
--          stores AS
 
--          (SELECT DISTINCT id as store_id, name as store_name
--          FROM `wego-cloud.shopcash.stores`
--          ),
 
--          deals AS
--          (SELECT DISTINCT id as deal_id, title as deal_title, start_at as deal_start_at, end_at as deal_end_at, coupon_code, store_id
--          FROM `wego-cloud.shopcash.deals`),
 
--          categories AS
--          (SELECT DISTINCT id as category_id, name as category_name, position as category_position
--          FROM `wego-cloud.shopcash.categories`),
 
--          banners AS
--          (SELECT DISTINCT id as banner_id, name as banner_name, start_at as banner_start_at, end_at as banner_end_at, store_id
--          FROM `wego-cloud.shopcash.banners`),
 
--          store_collections AS
--          (SELECT DISTINCT id as store_collection_id, title as store_collection_title, position as store_collection_position
--          FROM `wego-cloud.shopcash.store_collections`)
 
--   SELECT * EXCEPT(total_pageviews_home, total_pageviews_home_rank, total_distinct_session_id, total_distinct_session_id_rank),
--   IF(MIN(IF(page_type = 'home',total_pageviews_home_rank, NULL)) OVER (PARTITION BY DATE(created_at), site_code, device_type, logged_in_user) = total_pageviews_home_rank, total_pageviews_home, NULL) as total_pageviews_home,
--   IF(COALESCE(MIN(IF(page_type = 'home',total_distinct_session_id_rank, NULL)) OVER (PARTITION BY DATE(created_at), site_code, device_type, logged_in_user),1) = total_distinct_session_id_rank, total_distinct_session_id, NULL) as total_distinct_session_id,
--     CASE WHEN event_object IN ('banner_collections', 'category_list_stores', 'recent', 'trending_stores', 'store_collections', 'discover_stores') AND store_name IS NOT NULL THEN 'store'
--     WHEN event_object IN ('deal_collections', 'deals_coupons') AND deal_modal_action = 'store_logo' THEN 'store'
--     WHEN event_object = 'deal_collections' AND deal_modal_action = 'store_logo' THEN 'store'
--     WHEN event_object = 'all_deals' AND event_action = 'modal_click' AND deal_modal_action NOT IN ('shop_now', 'copy_code') THEN 'store'
--     WHEN event_action = 'search_result' THEN 'store'
--     WHEN event_object = 'deal_collections' AND deal_modal_action IN ('shop_now', 'visit_store') THEN 'handoff'
--     WHEN event_object = 'all_deals' AND (deal_has_code = 'false' OR deal_modal_action = 'shop_now') THEN 'handoff'
--     WHEN event_object IN ('get_deal', 'visit_store', 'shop_now') AND event_category = 'store' THEN 'handoff'
--     ELSE NULL
--     END AS lead_to_page,
--     CASE WHEN page_type = 'store' AND page_name != 'Store - Popular Stores' THEN 'store'
--     ELSE page_name
--     END AS page_category
 


 
--   FROM
--   (
 
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
--           ss.utm_source,
--           ss.utm_medium,
--           ss.utm_campaign,
--           ss.source,
--           pv.* EXCEPT(created_at, client_created_at, pageview_id, session_id, site_code, locale, date_partitiontime, user_hash, product, advertiser_id),
--           a.* EXCEPT(created_at, event_id, store_id),
--           COALESCE(a.store_id, st2.store_id, st3.store_id) as store_id,
--           COALESCE(st.store_name, st2.store_name, st3.store_name) as store_name,
--           d.* EXCEPT(deal_id, store_id),
--           c.* EXCEPT(category_id),
--           b.* EXCEPT(banner_id, store_id),
--           sc.* EXCEPT(store_collection_id),
--           pv.user_hash,
--           pv.product,
--           pv.advertiser_id,
--           ROW_NUMBER() OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_pageviews_home_rank,
--           COUNT(DISTINCT(IF(page_type = 'home', pageview_id, NULL))) OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_pageviews_home,
--           ROW_NUMBER() OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_distinct_session_id_rank,
--           COUNT(DISTINCT(pv.session_id)) OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_distinct_session_id,
 
--    FROM wa_pageviews AS pv
--    LEFT JOIN sessions AS ss ON pv.session_id = ss.session_id AND DATE(pv.created_at) = DATE(ss.created_at)
--    LEFT JOIN actions AS a ON  a.event_id = pv.pageview_id AND DATE(a.created_at) = DATE(pv.created_at)
--    LEFT JOIN stores AS st ON st.store_id = a.store_id
--    LEFT JOIN deals AS d ON d.deal_id = a.deal_id
--    LEFT JOIN stores AS st2 ON d.store_id = st2.store_id
--    LEFT JOIN categories AS c ON c.category_id = a.category_id
--    LEFT JOIN banners AS b ON b.banner_id = a.banner_id
--    LEFT JOIN stores AS st3 ON b.store_id = st3.store_id
--    LEFT JOIN store_collections AS sc ON sc.store_collection_id = a.store_collection_id
--   )
 
 
-- Daily append
 
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
                  CASE WHEN user_hash IS NOT NULL THEN 1
                  ELSE 0 END AS logged_in_user,
                  product,
                  advertiser_id
                  -- LEAD(page_url, 1) OVER(PARTITION BY client_id, session_id ORDER BY created_at) AS following_url,
  FROM `wego-cloud.shopcash_analytics.pageviews`
  WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ),
 
    sessions AS
 (SELECT DISTINCT created_at,
         session_id,
         user_city,
         user_country_code,
         market,
         channel,
         if(device_type like "%HP%", "android-app-preload", device_type) as device_type,
         app_version,
         ts_code,
         utm_source,
         utm_medium,
         utm_campaign,
         source
  FROM `wego-cloud.shopcash_analytics.sessions`
  WHERE DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        ),
 
        actions AS
 (SELECT * EXCEPT(deal_type, banner_collection_type),
 CASE WHEN deal_type LIKE 'without%' THEN 'without_coupon'
 WHEN LOWER(deal_type) LIKE '%agnostic' THEN 'coupon_agnostic'
 WHEN deal_type in ('with-coupon', 'withCoupon') THEN 'with_coupon'
 ELSE deal_type END AS deal_type,
 CASE WHEN banner_collection_type IN ("mid-page", "mid_page", "mid-page-2", "mid_page-2") THEN "mid_page"
 ELSE banner_collection_type END AS banner_collection_type
 FROM
    (SELECT DISTINCT
         TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR) as created_at,
         event.id as event_id,
         CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search_bar'
         ELSE event.category END as event_category,
         CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search'
         ELSE event.object END as event_object,
         CASE WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN 'search_result'
         ELSE event.action END as event_action,
         event.value as event_value,
         CASE WHEN event.category = 'login' THEN event.value END AS login_step,
         CASE WHEN event.category = 'wallet' AND event.object = 'referral' AND event.action = 'click' THEN event.value END AS referral_code,
         CASE WHEN event.category = 'wallet' AND event.object IN ('cashback_history', 'click_history') AND event.action = 'click' THEN event.value END AS cashback_history_accordion_date_tracked,
         CASE WHEN event.category = 'wallet' AND event.object = 'withdrawal_method' AND event.action = 'click' THEN event.value END AS withdrawal_method,
         CASE WHEN event.category = 'withdraw' THEN event.value END AS withdrawal_step,
         CASE WHEN event.category = 'notifications' AND event.object IN ('icon', 'button') AND event.action = 'click' THEN event.value END AS notification_value,
         CASE WHEN event.category = 'account' AND event.object = 'receive_email' AND event.action = 'click' THEN event.value END AS email_preferences,
         CASE WHEN event.category = 'home' AND event.object = 'user_education' AND event.action = 'click' THEN event.value END AS video_url,
         CASE WHEN event.category IN ('home', 'coupons') AND event.object = 'category_list' THEN event.value END AS category_id,
         CASE WHEN event.category IN ('home', 'deals') AND event.object = 'all_deals' AND event.action = 'load_more' THEN event.value END AS all_deals_load_more_click,
         CASE WHEN event.category = 'home' AND event.object = 'store_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.store_collection') END AS store_collection_id,
         CASE WHEN event.category = 'home' AND event.object = 'banner_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.banner_collection') END AS banner_collection_type,
         CASE WHEN event.category = 'home' AND event.object = 'banner_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.banner') END AS banner_id,
         CASE WHEN event.category = 'home' AND event.object = 'deal_collections' AND JSON_EXTRACT_SCALAR(event.value, '$.deal_collection') != '' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal_collection') END AS deal_type,
         CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.type') END AS search_result_type,
         CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result') END AS search_result_clicked,
 
        
         CASE WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'type' THEN event.value
              WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_term')
              END AS search_term,
 
 
         CASE WHEN event.category = 'store' AND event.object IN ('visit_store', 'shop_now') AND event.action = 'click' THEN event.value
              WHEN event.category = 'home' AND event.object IN ('category_list_stores', 'recent', 'discover_stores', 'trending_stores') THEN event.value
              WHEN event.category = 'home' AND event.object = 'store_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.store')
              WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' AND JSON_EXTRACT_SCALAR(event.value, '$.type') = 'store' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result')
              WHEN event.category = 'search' AND event.object IN ('recent', 'trending', 'results') THEN event.value
              END AS store_id,
 
        
         CASE WHEN event.category = 'store' AND event.object IN ('code_get', 'deal_get') AND event.action = 'click' THEN event.value
              WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
              WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
              WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
              WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.deal')
              WHEN event.category = 'search_bar' AND event.object = 'search' AND event.action = 'search_result' AND JSON_EXTRACT_SCALAR(event.value, '$.type') = 'deal' THEN JSON_EXTRACT_SCALAR(event.value, '$.search_result')
              END AS deal_id,
 
         CASE WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
              WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
              WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
              WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.cashback')
              END AS deal_cashback,
 
         CASE WHEN event.category = 'store' AND event.object = 'code_get' AND event.action IN ('modal_click', 'signed_in_modal_load') THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
              WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
              WHEN event.category = 'home' AND event.object = 'deal_collections' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
              WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.click')
              END AS deal_modal_action,
 
         CASE WHEN event.category IN ('deals', 'home') AND event.object = 'all_deals' AND event.action != 'load_more' THEN JSON_EXTRACT_SCALAR(event.value, '$.has_code')
              WHEN event.category = 'coupons' AND event.object = 'deals_coupons' THEN JSON_EXTRACT_SCALAR(event.value, '$.has_code')
              END AS deal_has_code
 
  FROM `wego-cloud.services_shopcash.events_actions*`
  WHERE _TABLE_SUFFIX = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
    )
 ),
 
        stores AS
 
        (SELECT DISTINCT id as store_id, name as store_name
        FROM `wego-cloud.shopcash.stores`
        ),
 
        deals AS
        (SELECT DISTINCT id as deal_id, title as deal_title, start_at as deal_start_at, end_at as deal_end_at, coupon_code, store_id
        FROM `wego-cloud.shopcash.deals`),
 
        categories AS
        (SELECT DISTINCT id as category_id, name as category_name, position as category_position
        FROM `wego-cloud.shopcash.categories`),
 
        banners AS
        (SELECT DISTINCT id as banner_id, name as banner_name, start_at as banner_start_at, end_at as banner_end_at, store_id
        FROM `wego-cloud.shopcash.banners`),
 
        store_collections AS
        (SELECT DISTINCT id as store_collection_id, title as store_collection_title, position as store_collection_position
        FROM `wego-cloud.shopcash.store_collections`)
 
 
 SELECT * EXCEPT(total_pageviews_home, total_pageviews_home_rank, total_distinct_session_id, total_distinct_session_id_rank),
   IF(MIN(IF(page_type = 'home',total_pageviews_home_rank, NULL)) OVER (PARTITION BY DATE(created_at), site_code, device_type, logged_in_user) = total_pageviews_home_rank, total_pageviews_home, NULL) as total_pageviews_home,
   IF(COALESCE(MIN(IF(page_type = 'home',total_distinct_session_id_rank, NULL)) OVER (PARTITION BY DATE(created_at), site_code, device_type, logged_in_user),1) = total_distinct_session_id_rank, total_distinct_session_id, NULL) as total_distinct_session_id,
   CASE WHEN event_object IN ('banner_collections', 'category_list_stores', 'recent', 'trending_stores', 'store_collections', 'discover_stores') AND store_name IS NOT NULL THEN 'store'
   WHEN event_object IN ('deal_collections', 'deals_coupons') AND deal_modal_action = 'store_logo' THEN 'store'
   WHEN event_object = 'deal_collections' AND deal_modal_action = 'store_logo' THEN 'store'
   WHEN event_object = 'all_deals' AND event_action = 'modal_click' AND deal_modal_action NOT IN ('shop_now', 'copy_code') THEN 'store'
   WHEN event_action = 'search_result' THEN 'store'
   WHEN event_object = 'deal_collections' AND deal_modal_action IN ('shop_now', 'visit_store') THEN 'handoff'
   WHEN event_object = 'all_deals' AND (deal_has_code = 'false' OR deal_modal_action = 'shop_now') THEN 'handoff'
   WHEN event_object IN ('get_deal', 'visit_store', 'shop_now') AND event_category = 'store' THEN 'handoff'
   ELSE NULL
   END AS lead_to_page,
  CASE WHEN page_type = 'store' AND page_name != 'Store - Popular Stores' THEN 'store'
  ELSE page_name
  END AS page_category
 
 
 FROM
 (
 
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
         ss.utm_source,
         ss.utm_medium,
         ss.utm_campaign,
         ss.source,
         pv.* EXCEPT(created_at, client_created_at, pageview_id, session_id, site_code, locale, date_partitiontime, user_hash, product, advertiser_id),
         a.* EXCEPT(created_at, event_id, store_id),
         COALESCE(a.store_id, st2.store_id, st3.store_id) as store_id,
         COALESCE(st.store_name, st2.store_name, st3.store_name) as store_name,
         d.* EXCEPT(deal_id, store_id),
         c.* EXCEPT(category_id),
         b.* EXCEPT(banner_id, store_id),
         sc.* EXCEPT(store_collection_id),
         pv.user_hash,
         pv.product,
         pv.advertiser_id,
          ROW_NUMBER() OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_pageviews_home_rank,
          COUNT(DISTINCT(IF(page_type = 'home', pageview_id, NULL))) OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_pageviews_home,
          ROW_NUMBER() OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_distinct_session_id_rank,
          COUNT(DISTINCT(pv.session_id)) OVER (PARTITION BY DATE(pv.created_at), site_code, device_type, logged_in_user) AS total_distinct_session_id,
 
  FROM wa_pageviews AS pv
  LEFT JOIN sessions AS ss ON pv.session_id = ss.session_id AND DATE(pv.created_at) = DATE(ss.created_at)
  LEFT JOIN actions AS a ON  a.event_id = pv.pageview_id AND DATE(a.created_at) = DATE(pv.created_at)
  LEFT JOIN stores AS st ON st.store_id = a.store_id
  LEFT JOIN deals AS d ON d.deal_id = a.deal_id
  LEFT JOIN stores AS st2 ON d.store_id = st2.store_id
  LEFT JOIN categories AS c ON c.category_id = a.category_id
  LEFT JOIN banners AS b ON b.banner_id = a.banner_id
  LEFT JOIN stores AS st3 ON b.store_id = st3.store_id
  LEFT JOIN store_collections AS sc ON sc.store_collection_id = a.store_collection_id
 )
{% endraw %}
