{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : bow_supplier_hotel_searches_daily_append
-- Destination: analysis.bow_supplier_hotel_searches  (unchanged)
-- Schedule   : every day 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels', 'sessions') }}
-- depends_on: {{ source('ib_hotels_supplier_worker', 'supplier_search_analytics') }}
{% raw %}
-- create table analysis.bow_supplier_hotel_searches
-- partition by date(created_at)
-- AS

-- ALTER TABLE analysis.bow_supplier_hotel_searches
-- ADD COLUMN unique_id STRING;

-- UPDATE analysis.bow_supplier_hotel_searches as c
-- SET c.unique_id = concat(c.session_id, ":", c.search_id, ":", c.supplier_code) 
-- where 1=1; 

SELECT
  concat(seson.id, ":", seson.search_id, ":", srchs.supplier_code) as unique_id,
  seson.id as session_id,
  seson.* except(`id`,`rooms`, `displayed_price_by_hotel_id`, `created_at`, `hotel_ids`),
  srchs.* except(`timestamp`, `requested_hotel_ids`, `available_hotel_ids`, `search_id`, `session_id`),
  date(seson.`created_at`) as created_at_UTC,
  timestamp_add(seson.`created_at`, interval 8 hour) as created_at,
FROM `wego-cloud.ib_hotels.sessions*` as seson

inner join `wego-cloud.ib_hotels_supplier_worker.supplier_search_analytics*` as srchs
on seson.search_id = srchs.search_id
and seson.id = srchs.session_id
and date(srchs.`timestamp`) = date(seson.created_at)
and srchs._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))

where seson._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
{% endraw %}
