{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : ipcc_checker_reference
-- Destination: analysis.ipcc_checker_reference  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
DELETE FROM `wego-cloud.analysis.ipcc_checker_reference` 
WHERE probe_date = DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

INSERT INTO `wego-cloud.analysis.ipcc_checker_reference`
WITH s AS (
  SELECT search_id,
         CONCAT(origin.code, '-', destination.code) AS route,
         PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)        AS probe_date
  FROM `wego-cloud.services_content_checker.content_checker_searches_log*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND provider IN ('sabre','travelport')
    AND inbound_date IS NULL
)
SELECT
  s.probe_date, s.route,
  k.validating_airline_code AS airline,
  k.flight_id               AS leg,
  k.fare_ipcc               AS ipcc,
  COUNT(*)                  AS obs,
  ROUND(MIN(COALESCE(( k.original_total + IFNULL(k.vendor_fee,0)
      - ( IFNULL(k.adult_total_iata,0)+IFNULL(k.child_total_iata,0)
        + IFNULL(k.adult_total_plb,0)+IFNULL(k.child_total_plb,0)
        + IFNULL(k.adult_total_cat35_commission,0)+IFNULL(k.child_total_cat35_commission,0)
        + IFNULL(k.infant_total_cat35_commission,0) )
    ) * SAFE_DIVIDE(k.final_total_usd, k.final_total), k.final_total_usd)), 2) AS net_usd_min,
  ROUND(AVG(COALESCE(( k.original_total + IFNULL(k.vendor_fee,0)
      - ( IFNULL(k.adult_total_iata,0)+IFNULL(k.child_total_iata,0)
        + IFNULL(k.adult_total_plb,0)+IFNULL(k.child_total_plb,0)
        + IFNULL(k.adult_total_cat35_commission,0)+IFNULL(k.child_total_cat35_commission,0)
        + IFNULL(k.infant_total_cat35_commission,0) )
    ) * SAFE_DIVIDE(k.final_total_usd, k.final_total), k.final_total_usd)), 2) AS net_usd_avg,
  LOGICAL_OR(( IFNULL(k.adult_total_iata,0)+IFNULL(k.child_total_iata,0)
             + IFNULL(k.adult_total_plb,0)+IFNULL(k.child_total_plb,0)
             + IFNULL(k.adult_total_cat35_commission,0)+IFNULL(k.child_total_cat35_commission,0)
             + IFNULL(k.infant_total_cat35_commission,0) ) != 0) AS has_commission_data
FROM `wego-cloud.services_content_checker.content_checker_provider_fare_calculations_log*` k
JOIN s USING (search_id)
WHERE k._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  AND k.validating_airline_code IS NOT NULL
  AND k.final_total_usd IS NOT NULL
  AND k.final_total > 0
GROUP BY 1,2,3,4,5;
{% endraw %}
