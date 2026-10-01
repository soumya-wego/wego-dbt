{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_partner_requests_responses_aggregated_append
-- Destination: analysis.hotels_partner_requests_responses_aggregated  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('hotels_partner_requests_responses') }}
{% raw %}
-- create table `wego-cloud.analysis.hotels_partner_requests_responses_aggregated`
-- PARTITION BY created_at AS\
--INSERT INTO `wego-cloud.analysis.hotels_partner_requests_responses_aggregated`


with cte as 
(select 
app_type,
below_cpc,
check_in,
check_out,
date(created_at) as created_at,
currency_code,
hotel_city_code,
hotel_city_name,
hotel_country_code,
hotel_country_name,
district_id,
guests_count,
hotel_id,
is_cached,
--latitude,
locale,
--longitude,
no_bucket,
processing_time,
provider_code,
radius,
region_id,
rental_only,
--request_host,
--request_length,
request_method,
--request_sent_at,
--request_url,
--response_size,
response_status_code,
rooms_count,
search_type,
site_code,
total,
unknown_currency,
unmatched_hotels,
user_country_code,
user_logged_in,
valid,
count(search_id) as searches,
sum(response_time_ms) as response_time

from `wego-cloud.analysis.hotels_partner_requests_responses` where 
date(created_at)  between date(current_date() - 2) and date(current_date() - 1)
--between TIMESTAMP("2025-01-01") and TIMESTAMP("2025-03-24")
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32)


select * from cte;
{% endraw %}
