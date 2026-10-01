{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : netsuite_group_pnl
-- Destination: analysis.netsuite_group_pnl  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'commission_incentive_flights_hotels_unpivoted') }}
-- depends_on: {{ ref('gmv_cogs_flights_hotels_unpivoted') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'uatp_flights_hotels_unpivoted') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'vcc_flights_hotels_unpivoted') }}
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
