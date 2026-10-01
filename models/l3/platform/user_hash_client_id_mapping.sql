{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : user_hash_client_id_mapping
-- Destination: analysis.user_hash_client_id_mapping  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'pageviews') }}
{% raw %}
--create table analysis.user_hash_client_id_mapping as 

with cte as 
(SELECT client_id,user_hash,count(distinct(session_id)) as sessions FROM `wego-cloud.wego_analytics.pageviews` group by 1,2)

select client_id,user_hash from cte 
qualify row_number() over(partition by client_id order by sessions desc) = 1
{% endraw %}
