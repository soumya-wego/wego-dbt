{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : saba_cross_supplier_netcost
-- Destination: analysis.saba_cross_supplier_netcost  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('integrated_bookings_flights', 'branded_fares') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'fare_calculations') }}
{% raw %}
INSERT INTO `wego-cloud.analysis.saba_cross_supplier_netcost`
WITH saba AS (
  SELECT
    PARSE_DATE('%Y%m%d', b._TABLE_SUFFIX) AS report_date,
    b.id           AS branded_fare_id,
    b.ms_fare_id   AS original_ms_fare_id,
    b.brand.name   AS brand_name,
    b.saba_details.original_provider_code AS original_provider,
    b.saba_details.provider_code          AS selected_provider,
    b.saba_details.original_price_usd     AS original_price_usd,
    b.saba_details.selected_price_usd     AS selected_price_usd,
    b.saba_details.final_price_usd        AS final_price_usd,
    (SELECT c.ms_fare_id
     FROM UNNEST(b.saba_details.compared_fares) c
     WHERE c.selected LIMIT 1)            AS selected_ms_fare_id,
    (SELECT SAFE_DIVIDE(c.price_usd, NULLIF(c.price, 0))
     FROM UNNEST(b.saba_details.compared_fares) c
     WHERE c.selected LIMIT 1)            AS fx_selected,
    (SELECT SAFE_DIVIDE(c.price_usd, NULLIF(c.price, 0))
     FROM UNNEST(b.saba_details.compared_fares) c
     WHERE NOT c.selected
       AND c.provider_code = b.saba_details.original_provider_code
     ORDER BY ABS(c.price_usd - b.saba_details.original_price_usd)
     LIMIT 1)                             AS fx_original
  FROM `wego-cloud.integrated_bookings_flights.branded_fares*` b
  WHERE b._TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND b.saba_details.provider_code IS NOT NULL
    AND STARTS_WITH(b.saba_details.reason, 'search_fare')
    AND b.saba_details.provider_changed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.id
                             ORDER BY IF(b.endpoint = 'COMPARE', 0, 1)) = 1
),
cmp AS (                                            -- COMPARE stage
  SELECT branded_fare_id,
         ANY_VALUE(fare_id)         AS fare_id,
         ANY_VALUE(original_total)  AS original_total,
         ANY_VALUE(net_margin)      AS net_margin,
         ANY_VALUE(final_total)     AS final_total,
         ANY_VALUE(final_total_usd) AS final_total_usd,
         ANY_VALUE(SAFE_DIVIDE(final_total, final_total_usd)) AS fx_compare
  FROM `wego-cloud.services_curiosity.branded_fare_calculations*`
  WHERE branded_fare_id IS NOT NULL
    AND _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  GROUP BY branded_fare_id
),
fc AS (                                             -- SEARCH stage, 3 columns only
  SELECT fare_id, original_total, net_margin, SAFE_DIVIDE(final_total, final_total_usd) AS fx_final
  FROM `wego-cloud.services_curiosity.fare_calculations*`
  WHERE fare_id IS NOT NULL
    AND _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)
SELECT
  s.report_date,
  s.branded_fare_id, s.brand_name,
  s.original_provider, s.selected_provider,
  s.selected_ms_fare_id,
  -- self-checks, both free
  c.fare_id = s.selected_ms_fare_id                              AS compare_key_agrees,
  s.fx_selected,
  SAFE_DIVIDE(c.final_total_usd, NULLIF(c.final_total, 0))       AS fx_from_calc,
  (fo.original_total - fo.net_margin) / fo.fx_final            AS search_net_cost_original_usd,
  (fs.original_total - fs.net_margin) / fs.fx_final            AS search_net_cost_selected_usd,
  (c.original_total  - c.net_margin)
      * SAFE_DIVIDE(c.final_total_usd, NULLIF(c.final_total, 0)) AS compare_net_cost_selected_usd,
  s.original_price_usd, s.selected_price_usd, s.final_price_usd,
  fo.net_margin / fo.fx_final net_margin_usd_original, 
  fs.net_margin / fs.fx_final net_margin_usd_selected,
  c.net_margin / c.fx_compare net_margin_usd_compare
FROM saba s
LEFT JOIN cmp c  ON c.branded_fare_id = s.branded_fare_id
LEFT JOIN fc  fo ON fo.fare_id = s.original_ms_fare_id
LEFT JOIN fc  fs ON fs.fare_id = s.selected_ms_fare_id
;
{% endraw %}
