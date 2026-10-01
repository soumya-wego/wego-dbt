{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_partner_request_responses_daily_append
-- Destination: analysis.hotels_partner_requests_responses  (unchanged)
-- Schedule   : every day 02:15   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- CREATE TABLE `wego-cloud.analysis.hotels_partner_requests_responses`
-- PARTITION BY DATE(created_at) AS

with city_country_places as (
  SELECT
    airports.* EXCEPT(location_id)
    , places.*
  FROM (
    SELECT 
      code as airport_code
      , base_name as airport_name
      , location_id as location_id
	 	FROM `wego-cloud.place_services.airports`
	) as airports
	left join (
    SELECT
      location.id as location_id
      , location.code as city_code
      , location.base_name as city_name
      , cou.code as country_code, cou.base_name as country_name
    FROM `place_services.locations` as location
    
    LEFT JOIN `place_services.countries` as cou 
    ON cou.id=location.country_id
  ) as places on airports.location_id = places.location_id
)

select distinct 
  pr.search.id as search_id,
  Timestamp(date(pr.search.created_at)) as created_at,
  pr.search.* except(`rooms`, `id`, `created_at`, `device_type`, `user_locale`, `client_id`, `user_site_code`),
  CASE
    WHEN LOWER(pr.search.device_type)='mobile' AND LOWER(pr.search.app_type)='ios-app' THEN 'ios-app'
    WHEN LOWER(pr.search.device_type)='mobile' AND LOWER(pr.search.app_type)='android-app' THEN 'android-app'
    WHEN LOWER(pr.search.device_type) = 'mobile' AND LOWER(pr.search.app_type)='mobile_web_app' THEN 'smartphone-web'
    WHEN LOWER(pr.search.device_type) = 'tablet' or lower(pr.search.app_type) like ('%tablet') THEN 'tablet-web'
    WHEN LOWER(pr.search.device_type) = 'desktop' THEN 'destop-web'
    WHEN LOWER(pr.search.device_type) = 'ipad' THEN 'tablet-web'
    WHEN LOWER(pr.search.device_type) LIKE '%ios%' THEN 'smartphone-web'
    WHEN LOWER(pr.search.device_type) LIKE '%iphone%' THEN 'smartphone-web'
    WHEN LOWER(pr.search.device_type) LIKE '%android%' THEN 'smartphone-web'
    WHEN LOWER(pr.search.device_type) LIKE '%blackberry%' THEN 'smartphone-web'
    WHEN LOWER(pr.search.device_type) LIKE '%symbian%' THEN 'smartphone-web'
    ELSE 'desktop-web' 
  END as device_type,

  loc.city_name as hotel_city_name ,
  loc.city_code as hotel_city_code,
  loc.country_name as hotel_country_name,
  loc.country_code as hotel_country_code,
  pr.provider_code,
  -- http_request to partner 
  pr.http_request.url as request_url,
  pr.http_request.method as request_method,
  -- pr.http_request.request_body as request_body,
  pr.http_request.content_length as request_length,
  pr.http_request.host_name as request_host,
  pr.http_request.sent_at as request_sent_at,
  -- http_response from partner
  pr.http_response.status_code as response_status_code,
  pr.http_response.status_text as response_status_text,
  -- pr.http_response.response_body as response_body,
  cast(pr.http_response.response_time as INT64) as response_time_ms,
  cast(pr.http_response.response_size as INT64) as response_size,
  
  -- request_details
  p_rps.* except(`created_at`, `provider_code`)

from `services_akasha.partner_requests*` as pr

-- partner reponses to above request table
left join (
  select
    prs.search.id as id,
    date(prs.search.created_at) as created_at,
    prs.is_cached,
    prs.provider_code,
    prs.total,
    prs.valid,
    prs.below_cpc,
    prs.no_bucket,
    prs.unmatched_hotels,
    prs.unknown_currency,
    prs.processing_time
  from `services_akasha.partner_responses*` as prs
  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
) as p_rps
on p_rps.id = pr.search.id
and date(p_rps.created_at) = date(pr.search.created_at)
and p_rps.provider_code = pr.provider_code

left join city_country_places as loc
on loc.city_code = pr.search.city_code

where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY));
{% endraw %}
