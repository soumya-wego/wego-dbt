{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_hotels_skyscanner_clicks_conversions
-- Destination: analysis.bow_hotels_skyscanner_clicks_conversions  (unchanged)
-- Schedule   : every day 08:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
--create table analysis.bow_hotels_skyscanner_clicks_conversions as 

with cte as 
(
SELECT
  session_id,
  REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') AS redirect_id,
  created_at,
  FORMAT('%s-%s-%s-%s-%s',
    SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 1, 8),
    SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 9, 4),
    SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 13, 4),
    SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 17, 4),
    SUBSTR(TO_HEX(FROM_BASE64(REPLACE(REPLACE(REGEXP_EXTRACT(landing_url, r'skyscanner_redirectid=([^&]+)') || '==', '-', '+'), '_', '/'))), 21, 12)
  ) AS decoded_redirect_id from 
  `wego-cloud.wego_analytics.sessions`
WHERE
  TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-04-25")
  AND landing_url LIKE "%skyscanner%" and ts_code = "6be92"
  group by 1,2,3,4
),

cte_1 as    
(

select a.*,b.country_code as destination_country_code from cte as a 
left join (
SELECT session_id,country_code FROM `wego-cloud.wego_analytics.hotels_searches` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-04-25") 
qualify row_number() over(partition by session_id order by created_at asc) = 1
) as b 
on a.session_id = b.session_id

),

skyscanner_clicks as     
(
 SELECT a.*,b.destination_country_code FROM `wego-cloud.distribution_partner_reports_hotels.skyscanner_click_report` as a 
 left join (select decoded_redirect_id,max(destination_country_code) as destination_country_code from cte_1 group by 1) as b 
 on a.redirect_id = b.decoded_redirect_id

),


bookings as  
(
SELECT a.*,b.decoded_redirect_id FROM `wego-cloud.wego_analytics.hotels_bookings` as a inner join cte as  b 
on a.session_id = b.session_id
where conversions_tracked > 0 
and conversions_adjusted > 0
)



select a.*,b.hotel_details_id,b.supplier_name,c.session_id as session_id from skyscanner_clicks as a left join  bookings as b 
on a.redirect_id = b.decoded_redirect_id
left join (select decoded_redirect_id,session_id from cte qualify row_number() over(partition by decoded_redirect_id order by created_at asc) = 1) as c     
on a.redirect_id = c.decoded_redirect_id;
{% endraw %}
