{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : ipcc_scorecard_30d
-- Destination: analysis.ipcc_scorecard_30d  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
/* ROLLING 28-DAY scorecard. Identical to ipcc_scorecard but windowed.
   Window chosen by sweep: 7/14/21/28/34 days against a held-out 14 days gave a flat
   hit rate (26.4-27.2%) but set convergence rising to 4.8/5 at 28 days, where the
   itinerary count also plateaus. Shorter is noisier for no gain, longer adds nothing.

   Scorecard: checker coverage and win rate joined to production status,
   one row per (route, airline, ipcc), with a priority for ordering.

   STATUS PRIORITY, for "which IPCC deserves a slot":
     1 chosen                  - proven winner, keep it
     2 never shopped           - untested opportunity, the whole point of the probe
     3 shopped, not displayed  - asked for, lost the slot
     4 displayed, not chosen   - got the slot and still lost, weakest case

   COVERAGE is conditional: the denominator is the itineraries in the probe days
   this IPCC actually appeared in, not all itineraries on the route. Only 40-51 of
   123 IPCCs appear in any one probe, so the unconditional version would measure
   probe scheduling rather than content.

   Status is derived from aggregates, not collapsed from per-leg statuses, so an
   IPCC chosen on one flight and absent on another reads as 'chosen'.  */
-- CREATE OR REPLACE TABLE `wego-cloud.analysis.ipcc_scorecard_30d` AS
INSERT INTO `wego-cloud.analysis.ipcc_scorecard_30d`
WITH itin AS (
  SELECT route, airline, probe_date, leg, MIN(net_usd_min) AS best
  FROM `wego-cloud.analysis.ipcc_checker_reference`
  WHERE net_usd_min IS NOT NULL
    AND probe_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY route, airline, probe_date, leg
),
day_n AS (
  SELECT route, airline, probe_date, COUNT(*) AS n FROM itin
  GROUP BY route, airline, probe_date
),
scope AS (                        -- conditional denominator
  SELECT r.route, r.airline, r.ipcc, SUM(d.n) AS in_scope
  FROM (SELECT DISTINCT route, airline, ipcc, probe_date
        FROM `wego-cloud.analysis.ipcc_checker_reference`
        WHERE net_usd_min IS NOT NULL
          AND probe_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) r
  JOIN day_n d USING (route, airline, probe_date)
  GROUP BY r.route, r.airline, r.ipcc
),
ranked AS (
  SELECT k.route, k.airline, k.ipcc, k.net_usd_min, i.best,
         RANK() OVER (PARTITION BY k.route,k.airline,k.probe_date,k.leg
                      ORDER BY k.net_usd_min) AS price_rank,
         COUNT(*) OVER (PARTITION BY k.route,k.airline,k.probe_date,k.leg) AS competitors
  FROM `wego-cloud.analysis.ipcc_checker_reference` k
  JOIN itin i USING (route, airline, probe_date, leg)
  WHERE k.net_usd_min IS NOT NULL
    AND k.probe_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
chk AS (
  SELECT r.route, r.airline, r.ipcc,
         COUNT(*)                                            AS priced,
         ROUND(100*COUNT(*)/s.in_scope, 1)                   AS coverage_pct,
         COUNTIF(r.price_rank = 1)                           AS wins,
         ROUND(100*COUNTIF(r.price_rank = 1)/COUNT(*), 1)    AS win_rate_pct,
         ROUND(100*COUNTIF(r.price_rank <= 5)/COUNT(*), 1)   AS top5_rate_pct,
         ROUND(AVG(r.competitors), 1)                        AS avg_competitors,
         ROUND(MIN(r.net_usd_min), 2)                        AS checker_net_min
  FROM ranked r JOIN scope s USING (route, airline, ipcc)
  GROUP BY r.route, r.airline, r.ipcc, s.in_scope
),
prod AS (
  SELECT route, airline, candidate_ipcc AS ipcc,
         COUNT(*)               AS cand_rows,
         COUNTIF(was_displayed) AS displayed,
         COUNTIF(is_chosen)     AS chosen,
         ROUND(MIN(candidate_net_usd), 2) AS prod_net_min
  FROM `wego-cloud.analysis.ipcc_candidate_vs_chosen`
  WHERE report_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY route, airline, candidate_ipcc
)
SELECT
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS report_date, c.route, c.airline, c.ipcc,
  CASE
    WHEN p.ipcc IS NULL     THEN 'never shopped'
    WHEN p.chosen    > 0    THEN 'chosen'
    WHEN p.displayed > 0    THEN 'displayed, not chosen'
    ELSE                         'shopped, not displayed'
  END AS status,
  CASE
    WHEN p.ipcc IS NULL     THEN 2
    WHEN p.chosen    > 0    THEN 1
    WHEN p.displayed > 0    THEN 4
    ELSE                         3
  END AS status_priority,
  c.coverage_pct, c.win_rate_pct, c.top5_rate_pct,
  c.priced, c.wins, c.avg_competitors, c.checker_net_min,
  p.cand_rows, p.displayed, p.chosen, p.prod_net_min,
  c.coverage_pct >= 10 AND c.win_rate_pct >= 10 AS passes_gates
FROM chk c
LEFT JOIN prod p USING (route, airline, ipcc)
{% endraw %}
