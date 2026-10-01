{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : bow_supplier_hotels_price_check_daily_append
-- Destination: analysis.bow_supplier_hotel_price_check  (unchanged)
-- Schedule   : every day 01:15   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('hotel_services', 'hotels') }}
-- depends_on: {{ source('ib_hotels', 'price_check') }}
-- depends_on: {{ source('ib_hotels', 'sessions') }}
-- depends_on: {{ source('ib_hotels_supplier_worker', 'supplier_search_analytics') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
{% raw %}
-- create table `wego-cloud.analysis.bow_supplier_hotel_price_check`
-- partition by date(created_at)
-- AS
SELECT
  concat(prc.session_id, ":", COALESCE(prc.search_id, "-1"), ":", prc.supplier_code, ":", COALESCE(prc.prebook_id, "-1")) as unique_id,
  prc.session_id,
  prc.search_id,
  prc.prebook_id,
  prc.supplier_code,
  prc.status as price_check_status,
  prc.message as price_check_message,
  prc.hotel_id,
  prc.supplier_hotel_id,
  prc.price_difference_percentage,
  prc.price_tolerance_percentage,
  case when prc.price_difference_percentage > prc.price_tolerance_percentage then "yes" else "no" end as `popup displayed`,
  prc.channel_used,
  seson.check_in_date,
  seson.check_out_date,
  date_diff(CAST(seson.check_in_date as DATE), date(prc.timestamp), DAY) as lead_time,
  htls.name_en as hotel_name,
  htls.city_code,
  bokngs.booking_id,
  bokngs.conversions_tracked,
  date(prc.`timestamp`) as created_at_UTC,
  timestamp_add(prc.`timestamp`, interval 8 hour) as created_at,
  seson.* except(`id`, `search_id`, `rooms`, `displayed_price_by_hotel_id`, `created_at`, `hotel_ids`, `adults_count`, `child_count`, `currency`, `city_code`, `check_in_date`, `check_out_date`),
  fix.supplier_name

FROM `wego-cloud.ib_hotels.price_check*` as prc

LEFT JOIN `wego-cloud.ib_hotels.sessions*` AS seson 
ON seson.id = prc.session_id
and date(prc.`timestamp`) = date(seson.created_at)

inner join `wego-cloud.hotel_services.hotels` as htls
on htls.id = cast(prc.hotel_id as integer)

LEFT JOIN `wego-cloud.wego_analytics.hotels_bookings` as bokngs
on bokngs.prebook_id = prc.prebook_id
and date(prc.`timestamp`) = date(bokngs.created_at)
and bokngs.conversions_tracked = 1

left join (
  select distinct 
    supplier_name,
    supplier_code
  from `ib_hotels_supplier_worker.supplier_search_analytics*`
  where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
) as fix 
on fix.supplier_code = prc.supplier_code

where prc._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
{% endraw %}
