{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : bow_flights_supplier_fare_analysis_v3
-- Destination: analysis.bow_flights_supplier_fare_analysis_v3  (unchanged)
-- Schedule   : every day 05:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('bow_flights_supplier_fare_analysis') }}
{% raw %}
INSERT INTO `wego-cloud.analysis.bow_flights_supplier_fare_analysis_v3`
WITH supplier_tiers AS (
  SELECT
    fare_ipcc,
    COUNT(*) AS row_cnt,
    CASE
      WHEN COUNT(*) > 1000000 THEN 20   -- top supplier: 5% sample
      WHEN COUNT(*) > 100000  THEN 10   -- mid supplier: 10% sample
      ELSE 1                            -- small supplier: keep all
    END AS sample_mod
  FROM `wego-cloud.analysis.bow_flights_supplier_fare_analysis`
  WHERE created_at = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY fare_ipcc
)
SELECT a.*
FROM `wego-cloud.analysis.bow_flights_supplier_fare_analysis` a
LEFT JOIN supplier_tiers t USING (fare_ipcc)
WHERE a.created_at = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND MOD(ABS(FARM_FINGERPRINT(a.search_id)), IFNULL(t.sample_mod, 10)) = 0;
{% endraw %}
