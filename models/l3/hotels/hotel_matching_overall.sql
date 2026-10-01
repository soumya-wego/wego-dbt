{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotel_matching_overall_daily_run
-- Destination: analysis.hotel_matching_overall  (unchanged)
-- Schedule   : every day 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
INSERT INTO `wego-cloud.analysis.hotel_matching_overall`
  (snapshot_date, total_wego_hotel_count, active_wego_hotel_count,
   total_provider_hotel_count, active_provider_hotel_count,
   total_matched_hotel_count, active_matched_hotel_count,
   overall_llm_matched_hotel_count, active_llm_matched_hotel_count)
SELECT
  CURRENT_DATE() AS snapshot_date,
  w.wego_hotel_count          AS total_wego_hotel_count,
  w.active_wego_hotel_count,
  p.provider_hotel_count      AS total_provider_hotel_count,
  p.active_provider_hotel_count,
  p.overall_matched_hotel_count AS total_matched_hotel_count,
  p.active_matched_hotel_count,
  p.llm_matched_hotel_count   AS overall_llm_matched_hotel_count,
  p.active_llm_matched_hotel_count
FROM (
  SELECT
    COUNT(*) AS wego_hotel_count,
    COUNTIF(disabled = FALSE) AS active_wego_hotel_count
  FROM `wego-cloud.hotel_services.hotels`
) AS w
CROSS JOIN (
  SELECT
    COUNT(*) AS provider_hotel_count,
    COUNTIF(active_by_import = 1 AND disabled_by_human = 0) AS active_provider_hotel_count,
    COUNTIF(hotel_id IS NOT NULL AND match_score >= 0.9) AS overall_matched_hotel_count,
    COUNTIF(hotel_id IS NOT NULL AND match_score >= 0.9 AND active_by_import = 1 AND disabled_by_human = 0) AS active_matched_hotel_count,
    COUNTIF(hotel_id IS NOT NULL AND match_score >= 0.9 AND matched_by = 'llm_matching') AS llm_matched_hotel_count,
    COUNTIF(hotel_id IS NOT NULL AND match_score >= 0.9 AND active_by_import = 1 AND disabled_by_human = 0 AND matched_by = 'llm_matching') AS active_llm_matched_hotel_count
  FROM `wego-cloud.hotels.provider_hotels`
) AS p;
{% endraw %}
