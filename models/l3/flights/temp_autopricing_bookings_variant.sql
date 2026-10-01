{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : temp_autopricing_bookings_variant
-- Destination: analysis.temp_autopricing_bookings_variant  (unchanged)
-- Schedule   : every 2 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_genzo', 'experiments_events') }}
{% raw %}
--drop table analysis.temp_autopricing_bookings_variant;

--create table analysis.temp_autopricing_bookings_variant as 


SELECT 

client.session_id as session_id,
experiment_list.experiment as experiment,
experiment_list.variant as variant
FROM `wego-cloud.services_genzo.experiments_events*`,
UNNEST(experiments) as experiment_list
WHERE  _table_suffix >= "20260707"
and experiment = "FS690 Dynamic Multi Experiments Auto Pricing v1"
GROUP BY 1,2,3
{% endraw %}
