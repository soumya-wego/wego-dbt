{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : meta_hotels_display_price_matching_insert_job
-- Destination: analysis.meta_hotels_display_price_matching_master  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('google_hotel_meta_services_production', 'searches') }}
-- depends_on: {{ source('services_akasha', 'gh_prices') }}
{% raw %}
-- create table 
-- analysis.meta_hotels_display_price_matching_master
-- partition by created_at_date
-- as



with base_table as 
(select 
gh_search_id,
hotel_id,
search_id,
fed_price.currency_code as displayed_price_currency_code,
fed_price.amount as displayed_amount,
fed_price.tax_amount as displayed_tax_amount,
fed_price.other_fees as displayed_other_fees,
fed_price.amount + fed_price.tax_amount + fed_price.other_fees as displayed_total_amount,
live_price.rate_id as rate_id,
live_price.total_amount as total_amount,
live_price.total_amount_usd as total_amount_usd,
live_price.total_tax_amount as total_tax_amount,
live_price.total_tax_amount_usd as total_tax_amount_usd,
live_price.currency_code as currency_code,
provider_code,
tolerance_percentage,
cause,
datetime(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) as created_at,
date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) as created_at_date,
price_match,
price_diff_percentage
from `wego-cloud.services_akasha.gh_prices*` 
where 
-- _table_suffix >= "20240101"
_table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day))))
and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
and date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) = date_sub(current_date(), interval 1 day)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21),

base_table_deduped as 
(select * from base_table where rate_id is not null
union all   
select a.* 
from base_table as a 
inner join 
(select search_id,count(rate_id) as rates from base_table group by 1 having count(rate_id) = 0 ) as b 
on a.search_id = b.search_id
qualify row_number() over(partition by a.search_id order by a.created_at asc) = 1
),


searches_base as
(
Select 
id,
adult_count,
check_in,
check_out,
children_count,
deadline_ms,
device_type,
live_query,
locale,
nights_count,
single_occupancy,
ts_code,
user_country_code
from `wego-cloud.google_hotel_meta_services_production.searches*`
where
_table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 7 day))))
and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select a.*,b.* from base_table_deduped as a left join searches_base as b on a.gh_search_id = b.id;
{% endraw %}
