{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_flights_search_requests_daily_append
-- Destination: analysis.bow_flights_search_requests  (unchanged)
-- Schedule   : every day 01:15   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.bow_flights_search_requests
-- partition by date(created_at)
-- AS

-- ALTER TABLE analysis.bow_flights_search_requests
-- ADD COLUMN error_messages STRING;

-- update analysis.bow_flights_search_requests as u
-- SET 
--   u.error_messages = errors
-- from (
--   select distinct search.id, array_to_string(errors, ', ') as errors, provider_code, date(timestamp_add(search.created_at, interval 8 hour)) as created_at, http_request.content_length
--   from `wego-cloud.services_curiosity.wego_partner_requests*` 
--   WHERE _TABLE_SUFFIX between FORMAT_DATE('%Y%m%d', "2022-09-25") and FORMAT_DATE('%Y%m%d', "2023-04-06") 
-- ) as b

-- WHERE u.id = b.id
-- and date(u.created_at) = date(b.created_at)
-- and u.provider_code = b.provider_code
-- and u.content_length = b.content_length
-- ;

-- insert into analysis.bow_flights_search_requests

SELECT DISTINCT 
  search.id,
  search.client_id,
  if(`order` = 0, departure_city_code, null) as departure_city_code,
  if(`order` = 0, arrival_city_code, null) as arrival_city_code,
  if(`order` = 0, outbound_date, null) as outbound_date,
  search.adults_count,
  search.children_count,
  search.infants_count,
  search.cabin,
  search.site_code,
  search.locale,
  search.currency_code,
  search.device_type,
  search.app_type,
  search.user_logged_in,
  search.trip_key,
  search.trip_type,
  search.number_of_legs,
  COALESCE(
  search.ipcc,
  SPLIT((SELECT h.value FROM UNNEST(http_request.headers) h
  WHERE h.name = 'TVP-PCC-CORE'), '_')[SAFE_OFFSET(0)]
  ) AS ipcc,
  search.request_options,
  search.search_type,
  search.is_account_code_request,
  timestamp_add(search.created_at, interval 8 hour) as created_at,
  search.created_at as created_at_utc,
  provider_code,
  http_request.url,
  http_request.method,
  http_request.content_length,
  http_request.host_name,
  http_request.sent_at,
  http_response.status_code,
  http_response.status_text,
  http_response.response_time,
  http_response.response_size,
  errors_count,
  array_to_string(errors, ', ') error_messages
FROM
  `wego-cloud.services_curiosity.wego_partner_requests*`, unnest(search.legs)
  -- WHERE _TABLE_SUFFIX between "20220926" and FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
  AND `order` = 0
{% endraw %}
