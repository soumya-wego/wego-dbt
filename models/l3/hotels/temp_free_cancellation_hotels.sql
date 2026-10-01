{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Marketing_Feeds_Free_Cancellation_Hotels_List
-- Destination: marketing_feeds.temp_free_cancellation_hotels  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels_supplier_worker', 'raw_rates') }}
-- depends_on: {{ source('marketing_feeds', 'free_breakfast_hotels') }}
-- depends_on: {{ source('marketing_feeds', 'free_cancellation_hotels') }}
-- depends_on: {{ source('marketing_feeds', 'temp_free_breakfast_hotels') }}
{% raw %}
-- Temporary table to hold updated data
CREATE OR REPLACE TABLE `marketing_feeds.temp_free_cancellation_hotels` AS
SELECT wego_hotel_id, MAX(full_refund_max_ts) AS full_refund_max_ts --Update temp table with the latest free cancellation date
FROM (
  (SELECT wego_hotel_id,full_refund_max_ts FROM `marketing_feeds.free_cancellation_hotels`
  WHERE full_refund_max_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) --Select cases where free cancellation was present in the last 7 days
  UNION ALL
  (SELECT wego_hotel_id, MAX(timestamp) AS full_refund_max_ts
  FROM `wego-cloud.ib_hotels_supplier_worker.raw_rates*` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND refund_term='full_refund' --Select cases where free cancellation was present yesterday
  GROUP BY 1)
)
GROUP BY wego_hotel_id;

-- Replace original table with the temporary table
CREATE OR REPLACE TABLE `marketing_feeds.free_cancellation_hotels` AS
SELECT * FROM `marketing_feeds.temp_free_cancellation_hotels`;


-- Temporary table to hold updated data
CREATE OR REPLACE TABLE `marketing_feeds.temp_free_breakfast_hotels` AS
SELECT wego_hotel_id, MAX(free_breakfast_max_ts) AS free_breakfast_max_ts --Update temp table with the latest free breakfast date
FROM (
  (SELECT wego_hotel_id,free_breakfast_max_ts FROM `marketing_feeds.free_breakfast_hotels`
  WHERE free_breakfast_max_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) --Select cases where free breakfast was present in the last 7 days
  UNION ALL
  (SELECT wego_hotel_id, MAX(timestamp) AS free_breakfast_max_ts
  FROM `wego-cloud.ib_hotels_supplier_worker.raw_rates*` 
  WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND board_basis IN ('Breakfast', 'Half Board', 'All Inclusive', 'Full Board') --Select cases where free breakfast was available yesterday
  GROUP BY 1)
)
GROUP BY wego_hotel_id;

-- Replace original table with the temporary table
CREATE OR REPLACE TABLE `marketing_feeds.free_breakfast_hotels` AS
SELECT * FROM `marketing_feeds.temp_free_breakfast_hotels`;
{% endraw %}
