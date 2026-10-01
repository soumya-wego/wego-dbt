{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : netsuite_group_pnl
-- Destination: analysis.netsuite_group_pnl  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
--create table analysis.netsuite_group_pnl as 

SELECT * FROM `wego-cloud.aaaaa_temporary_export_folder.gmv_cogs_flights_hotels_unpivoted` 
union all     
select *  from aaaaa_temporary_export_folder.commission_incentive_flights_hotels_unpivoted 
union all 
SELECT * FROM `wego-cloud.aaaaa_temporary_export_folder.uatp_flights_hotels_unpivoted`
union all 
SELECT * FROM `wego-cloud.aaaaa_temporary_export_folder.vcc_flights_hotels_unpivoted`
{% endraw %}
