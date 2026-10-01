{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_pricing_engine_ota
-- Destination: pricing_engine.hotels_pricing_engine_ota  (unchanged)
-- Schedule   : every day 01:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
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
