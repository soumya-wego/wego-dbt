{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : ab testing
-- Destination: wego_analytics.ab_testing  (unchanged)
-- Schedule   : every day 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_genzo', 'experiments_events') }}
{% raw %}
-- Notes:
-- Given tables are clustered by experiment & variant, need to use INSERT, SELECT in Daily append statement as they don't allow to specify clustering in "Update Scheduled Query". Don't specify table as the script DDL statement already does that.
-- Distinct at a client_id, session_id, experiment, variant level, created_at taken at minimum time as that would be the first record of when the session occurred.
-- created_at converted to Singapore Time UTC +08:00
-- experiments_events genzo currently fired for App. Need to integrate with Web.


-- Backfill:
-- CREATE OR REPLACE TABLE wego_analytics.ab_testing
-- PARTITION BY DATE(created_at)
-- CLUSTER BY experiment, variant
-- AS
-- SELECT 
-- MIN(TIMESTAMP(DATETIME(created_at, '+8:00'))) as created_at,
-- client.id as client_id,
-- client.session_id as session_id,
-- experiment_list.experiment as experiment,
-- experiment_list.variant as variant
-- FROM `wego-cloud.services_genzo.experiments_events*`,
-- UNNEST(experiments) as experiment_list
-- WHERE DATE(created_at, '+8:00') BETWEEN '2022-01-01' AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- GROUP BY client_id, session_id, experiment, variant

-- Daily append
INSERT INTO wego_analytics.ab_testing
SELECT 
MIN(TIMESTAMP(DATETIME(created_at, '+8:00'))) as created_at,
client.id as client_id,
client.session_id as session_id,
experiment_list.experiment as experiment,
experiment_list.variant as variant
FROM `wego-cloud.services_genzo.experiments_events*`,
UNNEST(experiments) as experiment_list
WHERE DATE(created_at, '+8:00') = DATE_SUB(@run_date, INTERVAL 1 DAY)
GROUP BY client_id, session_id, experiment, variant
{% endraw %}
