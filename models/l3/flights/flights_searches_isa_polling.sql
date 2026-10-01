{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_searches_isa_polling
-- Destination: analysis.flights_searches_isa_polling  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.flights_searches_isa_polling 
-- partition by created_at
-- as

with cte as
(
select search.id as search_id,
 TIMESTAMP_ADD(TIMESTAMP(search.created_at), INTERVAL 8 HOUR) as created_at,
sponsor.ad_candidate_count as ad_candidate_count,
sponsor.ad_request_count as ad_request_count,
sponsor.ad_return_count as ad_return_count,

timestamp,
ad_candidates.fare_id as fare_id,
ad_candidates.ad_ecpc
FROM `wego-cloud.services_curiosity.polling_requests*`
LEFT JOIN unnest(sponsor.ad_candidates) as ad_candidates
where _table_suffix >= (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 2 day))))
  and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
),

base_table as 

(select *
FROM cte 
qualify row_number() over(partition by search_id,fare_id order by timestamp desc) = 1
),


base_table_clicks as (

select a.*,b.click_id from base_table as a 
left join  
(SELECT * FROM `wego-cloud.wego_analytics.flights_clicks` WHERE date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) >=  date_sub(current_date(), interval 2 day)) as b 
on a.fare_id = b.fare_id
and a.search_id = b.search_id)

select date(created_at) as created_at,created_at as created,search_id,ad_candidate_count,ad_request_count,ad_return_count,sum(case when click_id is not null then ad_ecpc end) as isa_revenue ,count(distinct(click_id)) as clicks from base_table_clicks
where date(created_at) = date_sub(current_date(), interval 1 day)


group by 1,2,3,4,5,6
{% endraw %}
