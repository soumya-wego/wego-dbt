{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : user_hash_client_id_mapping
-- Destination: analysis.user_hash_client_id_mapping  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
--create table analysis.user_hash_client_id_mapping as 

with cte as 
(SELECT client_id,user_hash,count(distinct(session_id)) as sessions FROM `wego-cloud.wego_analytics.pageviews` group by 1,2)

select client_id,user_hash from cte 
qualify row_number() over(partition by client_id order by sessions desc) = 1
{% endraw %}
