{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : temp_autopricing_bookings_variant
-- Destination: analysis.temp_autopricing_bookings_variant  (unchanged)
-- Schedule   : every 2 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
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
