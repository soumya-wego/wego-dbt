{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_inventory_matching_analysis_daily_run
-- Destination: analysis.hotels_inventory_matching_analysis  (unchanged)
-- Schedule   : every day 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
SELECT countries.base_name as country_name,
countries.code as country_code,
city.base_name as city_name,
city.code as city_code,
providers.code as provider_code,
providers.name as provider_name,
case when lower(property_type.name_en) like '%hotel%' then 'Hotels'
when lower(property_type.name_en) like '%resort%' then 'Resorts'
when property_type.name_en is not null then 'Others'
else null end as property_type,
count(distinct(case when hotel_id is null then base.id end)) as non_matched_hotels,
count(distinct(case when hotel_id is not null then base.id end)) as matched_hotels,
count(distinct(base.id)) as total_hotels
from  `wego-cloud.hotels.provider_hotels` as base 
left join `hotels.provider_locations` as loc
on base.provider_location_id = loc.id
left join `hotels.provider_countries` as Pcountries 
on loc.provider_country_id = pcountries.id
left join `place_services.countries` as countries
on pcountries.country_id = countries.id
left join `hotels.providers` as providers 
on base.provider_id = providers.id
left join `place_services.locations` as city 
on loc.location_id = city.id
left join `hotels.provider_property_types` as property_type
on base.provider_property_type_id = property_type.id
group by 1,2,3,4,5,6,7;
{% endraw %}
