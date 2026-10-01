{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_supplier_hotel_requests_daily_append
-- Destination: analysis.bow_supplier_hotel_requests  (unchanged)
-- Schedule   : every day 01:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.bow_supplier_hotel_requests
-- partition by DATE(created_at) AS
SELECT  
  rqst.* except(`timestamp`),
  ss.currency,
  ss.user_city,
  ss.user_country_code,
  ss.site_code,
  ss.user_logged_in,
  ss.check_in_date, 
  ss.check_out_date, 
  ss.rooms_count, 
  ss.adults_count, 
  ss.child_count, 
  ss.device_type, 
  ss.app_type, 
  ss.device,
  ss.ip, 
  ss.language_code, 
  ss.cross_sell, 
  ss.ts_code, 
  ss.bow_only, 
  ss.reference, 
  ss.integration_device_type,
  err.error_name,
  err.error_message,
  akasha.city_code,
  akasha.country_code,
  akasha.wa_session_id,
  akasha.search_type,
  akasha.locale,
  timestamp_add(rqst.`timestamp`, interval 8 hour) as created_at

FROM `wego-cloud.ib_hotels_supplier_worker.supplier_request_analytics*` as rqst
left join `wego-cloud.ib_hotels.sessions*` as ss
on ss.id = rqst.session_id
and ss._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
left join `wego-cloud.ib_hotels_supplier_worker.supplier_request_error_analytics*` as err
on err.request_id = rqst.request_id
left join (
  SELECT
    id as search_id,
    client_id,
    search_type,
    city_code,
    country_code,
    check_in,
    check_out,
    rooms_count,
    guests_count,
    site_code,
    locale,
    currency_code,
    device_type,
    app_type,
    user_logged_in,
    user_country_code,
    session_id as wa_session_id,
    app_version,
    ts_code
  FROM `wego-cloud.services_akasha.searches*`
  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
) as akasha
on akasha.search_id = rqst.search_id

where rqst._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
{% endraw %}
