{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_pricing_engine_ota
-- Destination: pricing_engine.hotels_pricing_engine_ota  (unchanged)
-- Schedule   : every day 01:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('hotels_pricing_engine_ota_calculation') }}
{% raw %}
-- create or replace table `wego-cloud.pricing_engine.hotels_pricing_engine_ota` 
-- partition by date(created_at) as

select
  site_code
  , city_code as hotel_city
  , hotel_country_code as hotel_country
  , margin_percentage as markup_percentage
  , 0 as nightly_rate
  , upper(channel_type) as wego_channel
  , supplier_code as supplier
  , created_at
from `wego-cloud.pricing_engine.hotels_pricing_engine_ota_calculation` 
where date(created_at) = current_date()
and site_code is not null
and hotel_country_code is not null
and margin_percentage is not null
{% endraw %}
