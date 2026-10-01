{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_partner_requests_responses_daily_append
-- Destination: analysis.flights_partner_requests_responses  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- CREATE TABLE `wego-cloud.analysis.flights_partner_requests_responses`
 -- PARTITION BY DATE(created_at) AS

/*
ALTER TABLE `wego-cloud.analysis.flights_partner_requests_responses`
ADD COLUMN app_type STRING;
*/

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
  pr.search.* 
    except(`id`, `legs`, `created_at`, `device_type`, 
    `app_build`, `user_locale`, `user_site_code`, `client_id`, 
    `source`, `original_id`),
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
  dpt.airport_name as departure_airport_name,
  arv.airport_name as arrival_airport_name,
  lgs.departure_airport_code as departure_airport_code,
  lgs.arrival_airport_code as arrival_airport_code,
  dpt.city_name as departure_city_name ,
  arv.city_name as arrival_city_name,
  dpt.city_code as departure_city_code,
  arv.city_code as arrival_city_code,
  dpt.country_name as departure_country_name,
  arv.country_name as arrival_country_name,
  dpt.country_code as departure_country_code,
  arv.country_code as arrival_country_code,
  lgs.outbound_date,
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
  pr.http_response.response_time as response_time_ms,
  pr.http_response.response_size as response_size,
  -- request_details
  pr.errors_count as errors_count_flag,
  p_rps.is_cached,
  p_rps.total,
  p_rps.valid_provider_fares,
  p_rps.valid,
  p_rps.processing_time as processing_time_ms,
  --newly added columns 
  p_rps.curiosity_api_process_request_latency as curiosity_api_process_request_latency_ms,
  p_rps.search_message_latency as search_message_latency_ms,
  p_rps.integration_worker_prepare_search_latency as integration_worker_prepare_search_latency_ms,
  p_rps.integration_worker_process_result_latency as integration_worker_process_result_latency_ms,
  p_rps.completed_message_latency as completed_message_latency_ms,
  p_rps.curiosity_worker_process_result_latency as curiosity_worker_process_result_latency_ms
  

from `wego-cloud.services_curiosity.partner_requests*` as pr
, unnest(pr.search.legs) as lgs
-- partner reponses to above request table
left join (
  select
    prs.search.id as id,
    date(prs.search.created_at) as created_at,
    prs.is_cached,
    prs.total,
    prs.valid_provider_fares,
    prs.valid,
    prs.processing_time,
    prs.provider_code,
	--newly added columns 
	prs.curiosity_api_process_request_latency,
	prs.search_message_latency,
	prs.integration_worker_prepare_search_latency,
	prs.integration_worker_process_result_latency,
	prs.completed_message_latency,
	prs.curiosity_worker_process_result_latency
	
  from `wego-cloud.services_curiosity.partner_responses*` as prs
  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
) as p_rps
on p_rps.id = pr.search.id
and date(p_rps.created_at) = date(pr.search.created_at)
and p_rps.provider_code = pr.provider_code

left join city_country_places as dpt
on dpt.city_code = lgs.departure_city_code

left join city_country_places as arv
on arv.city_code = lgs.arrival_city_code

where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
and lgs.`order` = 0;
{% endraw %}
