-- Migrated from BigQuery scheduled query: autopricing_ab_test
-- Original destination: analysis.autopricing_ab_test
-- Changes vs the original SQL:
--   * upstream tables declared via source() so dbt knows the dependencies
--   * incremental by day-partition: each run replaces only the day it computes
--   * the day is parameterizable for backfills:
--       uv run dbt build --vars '{run_date: 2026-09-16}' --profiles-dir .
--     (without the var it defaults to yesterday, like the legacy schedule)

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'created_at', 'data_type': 'date'}
) }}

with sessions as (
    select date(created_at) as created_at_date, session_id, variant, experiment
    from {{ source('wego_analytics', 'ab_testing') }}
    where timestamp_trunc(created_at, day) >= timestamp("2026-06-01")
    group by 1, 2, 3, 4
),

searches as (
    -- original used select *; only these two columns are used downstream,
    -- and BigQuery bills per column read
    select session_id, search_id
    from {{ source('wego_analytics', 'flights_searches') }}
    where timestamp_trunc(_PARTITIONTIME, day) >= timestamp("2026-06-01")
),

searches_experiment as (
    select a.*, b.search_id
    from sessions as a
    left join searches as b on a.session_id = b.session_id
),

base_table as (
    select
        a.*,
        concat(first_departure_airport_code, "-", first_arrival_airport_code) as airport_route_revised,
        b.variant, b.experiment, b.session_id
    from {{ source('analysis', 'pricing_fares_analysis') }} as a
    left join searches_experiment as b
        on a.search_id = b.search_id
    where a.created_at = coalesce(
              safe.parse_date('%Y-%m-%d', '{{ var("run_date", "") }}'),
              current_date - 1
          )
      and a.provider_code = 'wego.com'
      and a.providers_status = 'multiple providers'
      and a.trip_type = 'oneway'
      and a.first_stop_count = 0
      and a.bow_original_total_fare_usd is not null
),

dim as (
    select
        concat(flights_pricing_fares_analysis_v2.first_departure_airport_code, "-", flights_pricing_fares_analysis_v2.first_arrival_airport_code) as airport_route,
        flights_pricing_fares_analysis_v2.first_airline as airline,
        flights_pricing_fares_analysis_v2.site_code as site_code,
        coalesce(sum(flights_pricing_fares_analysis_v2.click), 0) as total_clicks,
        sum(coalesce(sum(flights_pricing_fares_analysis_v2.click), 0)) over (order by coalesce(sum(flights_pricing_fares_analysis_v2.click), 0) desc) as cum_sum,
        sum(coalesce(sum(flights_pricing_fares_analysis_v2.click), 0)) over () as total
    from {{ source('analysis', 'pricing_fares_analysis') }} as flights_pricing_fares_analysis_v2
    where flights_pricing_fares_analysis_v2.created_at >= date('2026-01-01')
      and flights_pricing_fares_analysis_v2.created_at < date('2026-06-30')
      and provider_code = 'wego.com'
      and providers_status = 'multiple providers'
      and trip_type = 'oneway'
      and first_stop_count = 0
      and bow_original_total_fare_usd is not null
    group by 1, 2, 3
    order by 4 desc
)

select a.*
from base_table as a
inner join (select * from dim where cum_sum / total <= 0.60) as b
    on a.airport_route_revised = b.airport_route
    and a.first_airline = b.airline
    and a.site_code = b.site_code
