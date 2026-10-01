{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : ipcc_top5_selection
-- Destination: analysis.ipcc_top5_selection  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('ipcc_scorecard_30d') }}
{% raw %}
/* Top 5 IPCC per route x airline, from the rolling 28-day scorecard.
   Gates and sort live in one place: the scorecard defines the metrics, this only
   applies thresholds and cuts at 5. Re-running an as_of_date replaces it. */

-- CREATE TABLE IF NOT EXISTS `wego-cloud.analysis.ipcc_top5_selection` (
--   report_date DATE, window_days INT64, route STRING, airline STRING,
--   rank INT64, ipcc STRING, status STRING,
--   coverage_pct FLOAT64, win_rate_pct FLOAT64, wins INT64, priced INT64,
--   avg_competitors FLOAT64, checker_net_min FLOAT64,
--   prod_cand_rows INT64, prod_displayed INT64, prod_chosen INT64, survivors INT64
-- )
-- PARTITION BY report_date
-- CLUSTER BY route, airline, ipcc;

-- DELETE FROM `wego-cloud.analysis.ipcc_top5_selection` WHERE report_date = report_date;

INSERT INTO `wego-cloud.analysis.ipcc_top5_selection`
SELECT report_date, 30, route, airline, rank, ipcc, status,
       coverage_pct, win_rate_pct, wins, priced, avg_competitors, checker_net_min,
       cand_rows, displayed, chosen, survivors
FROM (
  SELECT *,
    COUNT(*)     OVER (PARTITION BY route, airline) AS survivors,
    ROW_NUMBER() OVER (PARTITION BY route, airline
                       ORDER BY win_rate_pct DESC, wins DESC, ipcc) AS rank
  FROM `wego-cloud.analysis.ipcc_scorecard_30d`
  WHERE win_rate_pct >= 10
    AND coverage_pct >= CASE status WHEN 'displayed, not chosen' THEN 30 ELSE 10 END
    AND report_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
WHERE rank <= 10;
{% endraw %}
