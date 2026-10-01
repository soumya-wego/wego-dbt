{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : trivago_hotel_bid_list
-- Destination: analysis.trivago_bid_file_upload  (unchanged)
-- Schedule   : every mon 09:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('bowh_supplier_availability_aggregated') }}
-- depends_on: {{ source('hotel_services', 'hotels') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
{% raw %}
/*
CAMPAIGN REFERENCE

locale	campaign
AA	1
AE	2
UK	3
US	4
TR	5
CA	6
IE	7
NZ	8
AU	9
IN	10
*/

with campaign_reference as (
  select 'AA' as locale, 1 as campaign union all
  select 'AE', 2 union all
  select 'UK', 3 union all
  select 'US', 4 union all
  select 'TR', 5 union all
  select 'CA', 6 union all
  select 'IE', 7 union all
  select 'NZ', 8 union all
  select 'AU', 9 union all
  select 'IN', 10
)

, supplier_availability as (
  select
    hotel_id
    , count(distinct supplier_name) as total_supplier
    , sum(total_available) as total_available
    , sum(total_request) as total_request
    , max(case when supplier_code = 'wgobsdgtyetx' then 1 else 0 end) as has_wegobeds_supplier
    , safe_divide(sum(total_available), sum(total_request)) as avail_rate
  from `wego-cloud.analysis.bowh_supplier_availability_aggregated`
  where date >= current_date - interval '7' day
  and supplier_channel in ('b2c','meta')
  and supplier_code not in ('epsbkjh9ro','boognik69qsa')
  and page = 'Feed'
  group by 1
  having safe_divide(total_available, total_request) > 0
)

, hotel_list as
(
  select
    a.*
    , c.code as city_code
    , c.base_name as city_name
    , d.code as country_code
    , name_en
  from supplier_availability a
  left join `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = cast(b.id as string)
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
  where 
  -- ( has_wegobeds_supplier = 1
  --   or total_supplier >= 2)
  --   and 
    not disabled
)

, final_list as
(
  select 
    locale
    , hotel_id as partner_reference
    , campaign  
  from campaign_reference
  cross join hotel_list
)

select * from final_list
{% endraw %}
