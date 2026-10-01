{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : goreward_hotel_list_v1
-- Destination: analysis.goreward_comparable_v1  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('analysis', 'goreward_hotel_list_v1') }}
-- depends_on: {{ source('analysis', 'goreward_hotel_stats_v1') }}
-- depends_on: {{ source('analysis', 'hotels_detail_list') }}
-- depends_on: {{ source('analysis', 'supplier_channel_mapping') }}
-- depends_on: {{ source('analysis', 'v__hotel_supplier_details') }}
-- depends_on: {{ source('ib_hotels', 'wegorates') }}
{% raw %}
-- GoReward daily pipeline — single scheduled query
-- Paste this into BQ console > Scheduled Queries, run daily (e.g. 02:00 UTC)
--
-- Step 1: append yesterday to base table
-- Step 2: rebuild hotel stats from last 14 days
-- Step 3: rebuild final hotel list (CI qualification + tiers + enrichment)

BEGIN

-- ─────────────────────────────────────────────────────────────
-- STEP 1: Append yesterday's data to goreward_comparable_v1
-- ─────────────────────────────────────────────────────────────

DELETE FROM `wego-cloud.analysis.goreward_comparable_v1`
WHERE search_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

INSERT INTO `wego-cloud.analysis.goreward_comparable_v1`

WITH

supplier_lookup AS (
  SELECT supplier_code, supplier_name, wego_channel,
    CASE WHEN rate_type = 'B2CNEW' THEN 'B2C' ELSE rate_type END AS rate_type
  FROM `wego-cloud.analysis.v__hotel_supplier_details`
  LEFT JOIN `wego-cloud.analysis.supplier_channel_mapping`
    ON supplier_name = supplier
),

rates AS (
  SELECT
    w.hotel_id,
    w.search_id,
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)                                                                    AS search_date,
    s.supplier_name,
    s.supplier_code,
    s.rate_type,
    MIN(w.supplier_original_price + COALESCE(w.tax_amount, 0) - COALESCE(w.marketing_fee, 0))             AS min_price
  FROM `wego-cloud.ib_hotels.wegorates*` w
  JOIN supplier_lookup s
    ON w.supplier_code = s.supplier_code
    AND LOWER(w.supplier_channel) = LOWER(s.wego_channel)
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND w.flow_type = 'CREATE_SEARCH'
    AND s.rate_type IN ('B2C', 'CUG', 'PACKAGE')
    AND w.supplier_original_price > 0
  GROUP BY 1, 2, 3, 4, 5, 6
),

cheapest AS (
  SELECT hotel_id, search_id, search_date, rate_type, MIN(min_price) AS min_price
  FROM rates
  GROUP BY 1, 2, 3, 4
),

cheapest_supplier AS (
  SELECT r.hotel_id, r.search_id, r.search_date, r.rate_type, r.supplier_name
  FROM rates r
  INNER JOIN cheapest c
    ON  r.hotel_id    = c.hotel_id
    AND r.search_id   = c.search_id
    AND r.search_date = c.search_date
    AND r.rate_type   = c.rate_type
    AND r.min_price   = c.min_price
  WHERE r.rate_type IN ('CUG', 'PACKAGE')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.hotel_id, r.search_id, r.search_date, r.rate_type
    ORDER BY r.supplier_name
  ) = 1
),

pivoted AS (
  SELECT
    hotel_id,
    search_id,
    MAX(search_date)                                AS search_date,
    MAX(IF(rate_type = 'B2C',     min_price, NULL)) AS b2c_price,
    MAX(IF(rate_type = 'CUG',     min_price, NULL)) AS cug_price,
    MAX(IF(rate_type = 'PACKAGE', min_price, NULL)) AS package_price
  FROM cheapest
  GROUP BY 1, 2
)

SELECT
  p.hotel_id,
  p.search_id,
  p.search_date,
  p.b2c_price,
  p.cug_price,
  p.package_price,
  cs_cug.supplier_name AS cug_supplier,
  cs_pkg.supplier_name AS package_supplier
FROM pivoted p
LEFT JOIN cheapest_supplier cs_cug
  ON  p.hotel_id  = cs_cug.hotel_id
  AND p.search_id = cs_cug.search_id
  AND cs_cug.rate_type = 'CUG'
LEFT JOIN cheapest_supplier cs_pkg
  ON  p.hotel_id  = cs_pkg.hotel_id
  AND p.search_id = cs_pkg.search_id
  AND cs_pkg.rate_type = 'PACKAGE'
WHERE p.b2c_price IS NOT NULL
  AND (p.cug_price IS NOT NULL OR p.package_price IS NOT NULL);


-- ─────────────────────────────────────────────────────────────
-- STEP 2: Rebuild hotel stats from last 14 days
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE `wego-cloud.analysis.goreward_hotel_stats_v1` AS

SELECT
  hotel_id,
  COUNT(*)                                                                              AS total_comparisons,
  COUNT(DISTINCT search_date)                                                           AS active_days,

  COUNTIF(cug_price IS NOT NULL)                                                        AS cug_searches,
  COUNTIF(cug_price < b2c_price)                                                        AS cug_cheaper_count,
  ROUND(SAFE_DIVIDE(COUNTIF(cug_price < b2c_price),
                    COUNTIF(cug_price IS NOT NULL)) * 100, 1)                           AS cug_cheaper_pct,
  ROUND(AVG(IF(cug_price IS NOT NULL,
               SAFE_DIVIDE(b2c_price - cug_price, b2c_price) * 100, NULL)), 2)         AS cug_avg_discount_pct,
  ROUND(STDDEV(IF(cug_price IS NOT NULL,
                  SAFE_DIVIDE(b2c_price - cug_price, b2c_price) * 100, NULL)), 4)      AS cug_discount_stddev,

  COUNTIF(package_price IS NOT NULL)                                                    AS package_searches,
  COUNTIF(package_price < b2c_price)                                                    AS package_cheaper_count,
  ROUND(SAFE_DIVIDE(COUNTIF(package_price < b2c_price),
                    COUNTIF(package_price IS NOT NULL)) * 100, 1)                       AS package_cheaper_pct,
  ROUND(AVG(IF(package_price IS NOT NULL,
               SAFE_DIVIDE(b2c_price - package_price, b2c_price) * 100, NULL)), 2)     AS package_avg_discount_pct,
  ROUND(STDDEV(IF(package_price IS NOT NULL,
                  SAFE_DIVIDE(b2c_price - package_price, b2c_price) * 100, NULL)), 4)  AS package_discount_stddev,

  -- B2C baseline stats
  ROUND(AVG(b2c_price), 2)                                                              AS b2c_avg_price,
  ROUND(MIN(b2c_price), 2)                                                              AS b2c_min_price,
  ROUND(MAX(b2c_price), 2)                                                              AS b2c_max_price,
  ROUND(STDDEV(b2c_price), 4)                                                           AS b2c_price_stddev

FROM `wego-cloud.analysis.goreward_comparable_v1`
WHERE search_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
GROUP BY 1;


-- ─────────────────────────────────────────────────────────────
-- STEP 3: Rebuild today's snapshot in goreward_hotel_list_v1
-- ─────────────────────────────────────────────────────────────

DELETE FROM `wego-cloud.analysis.goreward_hotel_list_v1`
WHERE snapshot_date = CURRENT_DATE();

INSERT INTO `wego-cloud.analysis.goreward_hotel_list_v1`
  (snapshot_date, hotel_id, hotel_name, city_code, country_code,
   total_comparisons, active_days,
   b2c_avg_price, b2c_min_price, b2c_max_price,
   cug_searches, cug_cheaper_pct, cug_avg_discount_pct, cug_ci_cheaper_lb, cug_ci_discount_lb,
   package_searches, package_cheaper_pct, package_avg_discount_pct, pkg_ci_cheaper_lb, pkg_ci_discount_lb,
   cug_qualifies, pkg_qualifies, best_ci_discount_lb, loyalty_tier)

WITH

ci_stats AS (
  SELECT
    hotel_id,
    cug_searches, cug_cheaper_pct, cug_avg_discount_pct, cug_discount_stddev,
    package_searches, package_cheaper_pct, package_avg_discount_pct, package_discount_stddev,
    total_comparisons, active_days,
    b2c_avg_price, b2c_min_price, b2c_max_price, b2c_price_stddev,

    ROUND((cug_cheaper_pct / 100
      - 1.96 * SQRT(cug_cheaper_pct / 100 * (1 - cug_cheaper_pct / 100)
                    / NULLIF(cug_searches, 0))) * 100, 2)            AS cug_ci_cheaper_lb,

    ROUND(cug_avg_discount_pct
      - 1.96 * SAFE_DIVIDE(cug_discount_stddev,
                           SQRT(NULLIF(cug_searches, 0))), 2)         AS cug_ci_discount_lb,

    ROUND((package_cheaper_pct / 100
      - 1.96 * SQRT(package_cheaper_pct / 100 * (1 - package_cheaper_pct / 100)
                    / NULLIF(package_searches, 0))) * 100, 2)         AS pkg_ci_cheaper_lb,

    ROUND(package_avg_discount_pct
      - 1.96 * SAFE_DIVIDE(package_discount_stddev,
                           SQRT(NULLIF(package_searches, 0))), 2)     AS pkg_ci_discount_lb

  FROM `wego-cloud.analysis.goreward_hotel_stats_v1`
),

tiered AS (
  SELECT
    *,
    -- Package only counts if it appears in >=90% of the hotel's searches
    SAFE_DIVIDE(package_searches, total_comparisons) >= 0.9 AS pkg_coverage_sufficient,

    (cug_ci_cheaper_lb >= 70)                                                                          AS cug_qualifies,
    (SAFE_DIVIDE(package_searches, total_comparisons) >= 0.9 AND pkg_ci_cheaper_lb >= 70)              AS pkg_qualifies,
    (cug_ci_cheaper_lb >= 70
      OR (SAFE_DIVIDE(package_searches, total_comparisons) >= 0.9 AND pkg_ci_cheaper_lb >= 70))        AS qualifies,

    -- Best discount CI: only include package if coverage is sufficient
    GREATEST(
      COALESCE(cug_ci_discount_lb, -999),
      IF(SAFE_DIVIDE(package_searches, total_comparisons) >= 0.9, COALESCE(pkg_ci_discount_lb, -999), -999)
    ) AS best_ci_discount_lb
  FROM ci_stats
),

hotel_geo AS (
  SELECT CAST(hotel_id AS STRING) AS hotel_id, hotel_name, city_code, country_code
  FROM `wego-cloud.analysis.hotels_detail_list`
)

SELECT
  CURRENT_DATE()                        AS snapshot_date,
  t.hotel_id,
  g.hotel_name,
  g.city_code,
  g.country_code,
  t.total_comparisons,
  t.active_days,

  -- B2C baseline (context for discount depth)
  t.b2c_avg_price,
  t.b2c_min_price,
  t.b2c_max_price,

  t.cug_searches,
  ROUND(t.cug_cheaper_pct, 1)          AS cug_cheaper_pct,
  ROUND(t.cug_avg_discount_pct, 2)     AS cug_avg_discount_pct,
  t.cug_ci_cheaper_lb,
  t.cug_ci_discount_lb,
  t.package_searches,
  ROUND(t.package_cheaper_pct, 1)      AS package_cheaper_pct,
  ROUND(t.package_avg_discount_pct, 2) AS package_avg_discount_pct,
  t.pkg_ci_cheaper_lb,
  t.pkg_ci_discount_lb,
  t.cug_qualifies,
  t.pkg_qualifies,
  t.best_ci_discount_lb,
  CASE
    WHEN t.best_ci_discount_lb >= 15 THEN 'Tier 1 (>=15%)'
    WHEN t.best_ci_discount_lb >= 10 THEN 'Tier 2 (>=10%)'
    WHEN t.best_ci_discount_lb >=  5 THEN 'Tier 3 (>=5%)'
    ELSE                                   'Cheaper only (<5%)'
  END AS loyalty_tier

FROM tiered t
LEFT JOIN hotel_geo g ON t.hotel_id = g.hotel_id
WHERE t.qualifies;


END
{% endraw %}
