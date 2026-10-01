{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : ipcc_candidate_vs_chosen
-- Destination: analysis.ipcc_candidate_vs_chosen  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
/* STEP 1b. Table A over a 7-day span, in ONE job.

   WHY ONE JOB: seven separate daily runs read 21 shards of each large table because
   the +/-1 day bridge windows overlap. One 7-day job reads 9. That is 7.46 TB instead
   of 20.9 TB, and roughly 235-320 slot-hours instead of ~660.

   MEASURED BASIS: the 1-day build ran 2.981 TB / 93.8 slot-hours / 7.6 min, i.e.
   31.5 slot-hours per TB and ~2.6 min per TB. Scaling that to 7.46 TB predicts
   ~19-28 min. Shuffle cost grows faster than linearly, so treat the top of that
   range as the realistic one and watch the 30-minute rule.

   d is the FIRST day of the window. report_date is taken per row, not from d.
   DELETE covers the whole span, so a re-run replaces all seven partitions.

   If step 0 proved D+1 unnecessary, change INTERVAL 7 DAY to INTERVAL 6 DAY in the
   three _TABLE_SUFFIX ranges: 9 shards becomes 8. */

-- DECLARE d      DATE DEFAULT DATE '2026-09-20';
-- DECLARE d_end  DATE DEFAULT DATE_ADD(d, INTERVAL 6 DAY);

-- CREATE TABLE IF NOT EXISTS `wego-cloud.analysis.ipcc_candidate_vs_chosen` (
--   report_date            DATE,
--   fare_id                STRING,
--   search_id              STRING,
--   route                  STRING,
--   airport_route          STRING,
--   airline                STRING,
--   integration_type       STRING,
--   leg                    STRING,
--   n_seg                  INT64,
--   chosen_ipcc            STRING,
--   srp_ipcc               STRING,
--   candidate_ipcc         STRING,
--   was_displayed          BOOL,
--   is_chosen              BOOL,
--   candidate_net_usd      FLOAT64,
--   has_commission_data    BOOL,
--   src_rows               INT64
-- )
-- PARTITION BY report_date
-- CLUSTER BY route, airline, candidate_ipcc;

-- DELETE FROM `wego-cloud.analysis.ipcc_candidate_vs_chosen`
-- WHERE report_date BETWEEN d AND d_end;

INSERT INTO `wego-cloud.analysis.ipcc_candidate_vs_chosen`
WITH checker_routes AS (            -- the probe covers ~82 routes, not ~9,000.
                              -- Keep Table A on the same routes as Table B.
  SELECT DISTINCT CONCAT(origin.code, '-', destination.code) AS route
  FROM `wego-cloud.services_content_checker.content_checker_searches_log*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND provider IN ('sabre','travelport')
    AND inbound_date IS NULL
),
oneway AS (
  SELECT id AS search_id
  FROM `wego-cloud.services_curiosity.searches*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND trip_type = 'ONEWAY'
),
bow AS (
  SELECT DISTINCT
         DATE(b.created_at)        AS report_date,
         b.fare_id, b.search_id,
         b.city_route              AS route,
         b.airport_route,
         b.validating_airline_code AS airline,
         b.integration_type,
         (SELECT STRING_AGG(REGEXP_EXTRACT(seg, r'^([A-Z0-9]+~[0-9]+)'), '-' ORDER BY idx)
          FROM UNNEST(SPLIT(SPLIT(b.flight_id, ':')[SAFE_OFFSET(1)], '-')) AS seg WITH OFFSET idx
         ) AS leg,
         ARRAY_LENGTH(SPLIT(SPLIT(b.flight_id, ':')[SAFE_OFFSET(1)], '-')) AS n_seg,
         b.booking_ipcc            AS chosen_ipcc,
         b.fare_ipcc               AS srp_ipcc
  FROM `wego-cloud.analysis.bow_branded_fares_selected` b
  JOIN oneway o ON o.search_id = b.search_id
  WHERE DATE(b.created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND b.city_route IS NOT NULL
    AND b.integration_type IN ('SABRE','TRAVELPORT')
    AND b.city_route IN (SELECT route FROM checker_routes)
),
bridge AS (
  SELECT DISTINCT fare_id, provider_fare_id
  FROM `wego-cloud.services_curiosity.fare_calculations*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),
cand AS (
  SELECT provider_fare_id, fare_ipcc, selected,
         COALESCE(
           ( original_total + IFNULL(vendor_fee,0)
             - ( IFNULL(adult_total_iata,0) + IFNULL(child_total_iata,0)
               + IFNULL(adult_total_plb,0)  + IFNULL(child_total_plb,0)
               + IFNULL(adult_total_cat35_commission,0)
               + IFNULL(child_total_cat35_commission,0)
               + IFNULL(infant_total_cat35_commission,0) )
           ) * SAFE_DIVIDE(final_total_usd, final_total),
           final_total_usd
         ) AS net_usd,
         ( IFNULL(adult_total_iata,0) + IFNULL(child_total_iata,0)
           + IFNULL(adult_total_plb,0)  + IFNULL(child_total_plb,0)
           + IFNULL(adult_total_cat35_commission,0)
           + IFNULL(child_total_cat35_commission,0)
           + IFNULL(infant_total_cat35_commission,0) ) != 0 AS has_comm
  FROM `wego-cloud.services_curiosity.provider_fare_calculations*`
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
SELECT
  b.report_date,
  b.fare_id, b.search_id, b.route, b.airport_route, b.airline, b.integration_type,
  b.leg, b.n_seg, b.chosen_ipcc, b.srp_ipcc,
  c.fare_ipcc                             AS candidate_ipcc,
  LOGICAL_OR(c.selected)                  AS was_displayed,
  LOGICAL_OR(c.fare_ipcc = b.chosen_ipcc) AS is_chosen,
  ROUND(MIN(c.net_usd), 2)                AS candidate_net_usd,
  LOGICAL_OR(c.has_comm)                  AS has_commission_data,
  COUNT(*)                                AS src_rows
FROM bow b
JOIN bridge br USING (fare_id)
JOIN cand   c  USING (provider_fare_id)
GROUP BY report_date, fare_id, search_id, route, airport_route, airline,
         integration_type, leg, n_seg, chosen_ipcc, srp_ipcc, candidate_ipcc;
{% endraw %}
