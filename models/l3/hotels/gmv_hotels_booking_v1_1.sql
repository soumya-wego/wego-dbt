{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_hotels_bookings_netsuite_details_full
-- Destination: aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1  (unchanged)
-- Schedule   : every day 07:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1` AS (
-- 	WITH
-- exchange_rates AS (
-- 	SELECT
-- 		base,
-- 	  amount,
-- 	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
--   FROM
--     `wego-cloud.analytics.exchange_rates*`
--   WHERE TRUE
--     AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2021-01-14')
--     AND quote = 'USD'
-- )

-- SELECT
--   hb.booking_id,
--   cm.ns_market AS market,
--   hb.wego_base_price_usd,
--   hb.wego_taxes_usd,
--   hb.wego_markup_amount_usd,
--   hb.vendor_marketing_fee_usd,
--   hb.supplier_name,
--   /*Newly added fields*/
--   hb.check_in,
--   IF(
--     hb_c.created_time IS NOT NULL,
--     hb_c.created_time,
--     hb.check_in
--   ) AS check_in_recalculated, /*See CHANGELOG.md*/
--   hb.vendor_base_price_usd,
--   hb.wego_discount_amount_usd,
--   hb.promo_discount_amount_usd,
--   (CASE
--     WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
-- 		ELSE 'All Others'
--   END) AS Site_Code,
--   hb.exchange_rate_to_usd_from_other_currency_code,
--   hb.other_currency_code,
--   STRUCT(
--     hb.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount AS invoice_gov_tax_amount_sar,
--     hb.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount AS invoice_gov_tax_amount_egp,
--     -- hb.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount AS invoice_gov_tax_amount_pkr,
--     hb.invoice_gov_tax_details.vat_amount AS invoice_gov_tax_amount_usd /*Default in USD*/
--   ) AS invoice_gov_tax_amount
-- FROM (
--   SELECT
--     *
--   FROM
--     `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_confirmed_logs_audit`
--   WHERE TRUE
--     AND subproduct = 'bow'
-- ) hb
-- LEFT JOIN
--   `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit` hb_c
--   USING(client_booking_id) /*To get cancellation date (created_time)*/
-- LEFT JOIN
--   exchange_rates AS exchange_rates_sar
--   ON 'SAR' = exchange_rates_sar.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_sar.effective)
-- LEFT JOIN
--   exchange_rates AS exchange_rates_egp
--   ON 'EGP' = exchange_rates_egp.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_egp.effective)
-- -- LEFT JOIN
-- --   exchange_rates AS exchange_rates_pkr
-- --   ON 'PKR' = exchange_rates_pkr.base
-- --     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_pkr.effective)
-- LEFT JOIN
--   `wego-cloud.analytics.countries_misc` cm
--   ON hb.site_code = cm.country_code
-- );
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_cancellation_v1_1` AS (
-- 	WITH
-- exchange_rates AS (
-- 	SELECT
-- 		base,
-- 	  amount,
-- 	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
--   FROM
--     `wego-cloud.analytics.exchange_rates*`
--   WHERE TRUE
--     AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11')
--     AND quote = 'USD'
-- )

-- SELECT
--   hb.booking_id,
--   cm.ns_market AS market,
--   hb.wego_total_price_usd,
--   hb.user_refund_amount_usd,
--   hb.vendor_refund_amount_usd,
--   /*Newly added fields*/
--   hb.vendor_marketing_fee_usd,
--   hb.supplier_name,
--   hb.check_in,
--   hb.created_time,
--   (CASE
--     WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
-- 		ELSE 'All Others'
--   END) AS Site_Code,
--   hb.exchange_rate_to_usd_from_other_currency_code,
--   hb.other_currency_code,
--   STRUCT(
--     hb.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount AS invoice_gov_tax_amount_sar,
--     hb.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount AS invoice_gov_tax_amount_egp,
--     -- hb.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount AS invoice_gov_tax_amount_pkr,
--     hb.invoice_gov_tax_details.vat_amount AS invoice_gov_tax_amount_usd /*Default in USD*/
--   ) AS invoice_gov_tax_amount
-- FROM (
--   SELECT
--     *
--   FROM
--     `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit`
--   WHERE TRUE
--     AND subproduct = 'bow'
-- ) hb
-- LEFT JOIN
--   exchange_rates AS exchange_rates_sar
--   ON 'SAR' = exchange_rates_sar.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_sar.effective)
-- LEFT JOIN
--   exchange_rates AS exchange_rates_egp
--   ON 'EGP' = exchange_rates_egp.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_egp.effective)
-- -- LEFT JOIN
-- --   exchange_rates AS exchange_rates_pkr
-- --   ON 'PKR' = exchange_rates_pkr.base
-- --     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_pkr.effective)
-- LEFT JOIN
--   `wego-cloud.analytics.countries_misc` cm
--   ON hb.site_code = cm.country_code
-- );
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_booking_v1_1` AS (
-- 	SELECT
--   *,
--   IFNULL(vendor_base_price_usd, 0) AS cogs_value
-- FROM (
--   SELECT
--     hb.booking_id,
--     cm.ns_market as market,
--     hb.vendor_base_price_usd,
--     /*Newly added fields*/
--     hb.check_in,
--     IF(hb_c.created_time IS NOT NULL, hb_c.created_time, hb.check_in) AS check_in_recalculated, /*See CHANGELOG.md*/
--     (CASE
--       WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
--       ELSE 'All Others'
--     END) AS Site_Code,
--     hb.supplier_name,
--     hb.exchange_rate_to_usd_from_other_currency_code,
--     hb.other_currency_code,
--     hb.optimized_cost_usd
--   FROM (
--     SELECT
--       *
--     FROM
--       `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_confirmed_logs_audit`
--     WHERE TRUE
--       AND subproduct = 'bow'
--   ) hb
--   LEFT JOIN
--     `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit` hb_c
--     USING(client_booking_id) /*To get cancellation date (created_time)*/
--   LEFT JOIN
--     `wego-cloud.analytics.countries_misc` cm on hb.site_code = cm.country_code
-- )
-- );
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_cancellation_v1_1` AS (
-- 	SELECT
--   *,
--   IFNULL(vendor_base_price_usd, 0) - IFNULL(vendor_refund_amount_usd, 0) AS cogs_value
-- FROM (
--   SELECT
--     cm.ns_market AS market,
--     hb.vendor_base_price_usd,
--     hb.vendor_refund_amount_usd,
--     hb.user_refund_amount_usd,
--     /*Newly added fields*/
--     hb.check_in,
--     hb.created_time,
--     (CASE
--       WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
--       ELSE 'All Others'
--     END) AS Site_Code,
--     supplier_name,
--     hb.exchange_rate_to_usd_from_other_currency_code,
--     hb.other_currency_code
--   FROM (
--     SELECT
--       *
--     FROM
--       `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit`
--     WHERE TRUE
--       AND subproduct = 'bow'
--   ) hb
--   LEFT JOIN
--     `wego-cloud.analytics.countries_misc` cm
--     ON hb.site_code = cm.country_code
-- )
-- );
-- -- ============================================================
-- -- GMV Booking Details
-- -- Source: gmv_hotels_booking_v1_1
-- -- ============================================================
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_booking_details` AS (
--     SELECT
--         booking_id                                              AS `Booking ID`,
--         Site_Code                                               AS `Site Code`,
--         market                                                  AS `Market`,
--         other_currency_code                                     AS `Other Currency Code`,
--         exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
--         check_in                                                AS `Check In`,
--         check_in_recalculated                                   AS `Check In Recalculated`,
--         supplier_name                                           AS `Supplier Name`,

--         -- Sales_GMV_OTA_Hotels components
--         wego_base_price_usd                                     AS `Wego Base Price USD`,
--         wego_markup_amount_usd                                  AS `Wego Markup Amount USD`,
--         wego_taxes_usd                                          AS `Wego Taxes USD`,

--         -- Discount_GMV_OTA_Hotels components
--         wego_discount_amount_usd                                AS `Wego Discount Amount USD`,
--         promo_discount_amount_usd                               AS `Promo Discount Amount USD`,

--         -- Marketing_Fee component
--         vendor_marketing_fee_usd                                AS `Vendor Marketing Fee USD`,

--         -- VAT_Output components
--         invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
--         invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
--         invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

--     FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1`
-- );

-- -- ============================================================
-- -- GMV Cancellation Details
-- -- Source: gmv_hotels_cancellation_v1_1
-- -- ============================================================
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details` AS (
--     SELECT
--         booking_id                                              AS `Booking ID`,
--         Site_Code                                               AS `Site Code`,
--         market                                                  AS `Market`,
--         other_currency_code                                     AS `Other Currency Code`,
--         exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
--         created_time                                            AS `Created Time`,
--         supplier_name                                           AS `Supplier Name`,

--         -- Sales_GMV_OTA_Hotels component
--         user_refund_amount_usd                                  AS `User Refund Amount USD`,

--         -- Marketing_Fee_Cancelled component
--         vendor_marketing_fee_usd                                AS `Vendor Marketing Fee USD`,

--         -- VAT_Output components
--         invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
--         invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
--         invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

--     FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_v1_1`
-- );

-- -- ============================================================
-- -- COGS Booking Details
-- -- Source: cogs_hotels_booking_v1_1
-- -- ============================================================
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_booking_details` AS (
--     SELECT
--         booking_id                                              AS `Booking ID`,
--         Site_Code                                               AS `Site Code`,
--         market                                                  AS `Market`,
--         supplier_name                                           AS `Supplier Name`,
--         other_currency_code                                     AS `Other Currency Code`,
--         exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
--         check_in  																						  AS `Check In`,
--         check_in_recalculated                                   AS `Check In Recalculated`,

--         -- Hotels_Supplier_Costs_COGS_OTA_Hotels components
--         vendor_base_price_usd                                   AS `Vendor Base Price USD`,
--         optimized_cost_usd                                      AS `Optimized Cost USD`

--     FROM `aaaaa_temporary_export_folder.cogs_hotels_booking_v1_1`
-- );

-- -- ============================================================
-- -- COGS Cancellation Details
-- -- Source: cogs_hotels_cancellation_v1_1
-- -- ============================================================
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_cancellation_details` AS (
--     SELECT
--         Site_Code                                               AS `Site Code`,
--         market                                                  AS `Market`,
--         supplier_name                                           AS `Supplier Name`,
--         other_currency_code                                     AS `Other Currency Code`,
--         exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
--         created_time                                            AS `Created Time`,

--         -- Hotels_Supplier_Costs_COGS_OTA_Hotels component
--         vendor_refund_amount_usd                                AS `Vendor Refund Amount USD`

--     FROM `aaaaa_temporary_export_folder.cogs_hotels_cancellation_v1_1`
-- );

-- -- ============================================================
-- -- VCC / UATP Incentive Details
-- -- Source: wego_analytics.hotels_bookings (conversions_tracked = 1)
-- -- Covers both VCC partners (Apiso / Nium via vcc_provider) and UATP (vc_partner_account LIKE '%UATP%')
-- -- ============================================================
-- CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.vcc_uatp_hotels_details` AS (
--     SELECT
--         hb.booking_id                                           AS `Booking ID`,
--         hb.site_code                                            AS `Site Code`,
--         hb.market                                               AS `Market`,
--         hb.created_at                                           AS `Created Time`,
--         hb.check_in                                             AS `Check In`,

--         -- Partner identifiers
--         hb.vc_partner_account                                   AS `VC Partner Account`,
--         hb.vc_partner_card_pool_id                              AS `VC Partner Card Pool ID`,
--         CASE
--             WHEN vcc_provider.Partner IN ('Apiso', 'Nium') THEN 'Apiso_Nium'
--             WHEN vcc_provider.Partner IS NOT NULL           THEN vcc_provider.Partner
--             WHEN hb.vc_partner_account LIKE '%UATP%'         THEN 'UATP'
--             ELSE NULL
--         END                                                     AS `VC Partner`,
--         hb.vc_rebate_currency_code                              AS `VC Rebate Currency Code`,

--         -- VCC Rebate components (USD at source; multi-currency split lives in source script)
--         hb.vc_rebate_value_usd                                  AS `VC Rebate Value USD`,
--         hb.vc_rebate_fee_usd                                    AS `VC Rebate Fee USD`,
--         IFNULL(hb.vc_rebate_value_usd, 0)
--           + IFNULL(hb.vc_rebate_fee_usd, 0)                     AS `VCC Rebate Accrual USD`

--     FROM `wego_analytics.hotels_bookings` AS hb
--     LEFT JOIN `wego-cloud.ota_data_upload_template_working_30Oct24.vcc_provider` AS vcc_provider
--         ON hb.vc_partner_card_pool_id = vcc_provider.ClientID
--     WHERE hb.conversions_tracked = 1
-- );

DECLARE cutoff_KFH100 TIMESTAMP DEFAULT NULL;
DECLARE cutoff_ref_KFH100 STRING DEFAULT NULL;
DECLARE cutoff_partial_KFH100 NUMERIC DEFAULT NULL;

-- Compute each sponsored promo's budget-exhaustion timestamp once, hold it in a script
-- variable so the GMV CASE references it as a constant (no per-row join).
CREATE OR REPLACE TEMP TABLE promo_cutoffs AS (
	WITH
    budget AS (
      SELECT 'KFH100' AS promo_code, 550000 AS budget_usd
    )
    , x AS (
      -- Only scan table if at least one promo actually applies to that product
      -- Flights: promos that apply to flights
      SELECT
        promo_code,
        SAFE_CAST(promo_discount_amount_usd AS NUMERIC) AS promo_discount_amount_usd,
        booking_ref,
        created_at - INTERVAL 8 HOUR AS created_at
      FROM
        `wego-cloud.wego_analytics.flights_bookings`
      WHERE TRUE
        -- Longest campaign so far (as of 2026-07-24) is 3 months, so 4 months covers it
        AND _PARTITIONTIME >= TIMESTAMP(DATE_SUB(DATE_TRUNC(CURRENT_DATE('+08:00') - INTERVAL 1 DAY, MONTH), INTERVAL 4 MONTH))
        AND _PARTITIONTIME <=  TIMESTAMP(CURRENT_DATE('+08:00') - INTERVAL 1 DAY)
        AND conversions_tracked = 1
        AND promo_code IN ('KFH100')
      UNION ALL
      -- Hotels: promos that apply to hotels
      SELECT
        promo_code,
        SAFE_CAST(promo_discount_amount_usd AS NUMERIC) AS promo_discount_amount_usd,
        client_booking_id AS booking_ref,
        created_at - INTERVAL 8 HOUR AS created_at
      FROM
        `wego-cloud.wego_analytics.hotels_bookings`
      WHERE TRUE
        -- Longest campaign so far (as of 2026-07-24) is 3 months, so 4 months covers it
        AND DATE(created_at) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('+08:00') - INTERVAL 1 DAY, MONTH), INTERVAL 4 MONTH)
        AND DATE(created_at) <= CURRENT_DATE('+08:00') - INTERVAL 1 DAY
        AND conversions_tracked = 1
        AND promo_code IN ('KFH100')
    )
    , per_booking AS (
      SELECT
        x.promo_code,
        x.booking_ref,
        x.created_at,
        x.promo_discount_amount_usd,
        SUM(x.promo_discount_amount_usd) OVER (
          PARTITION BY x.promo_code
          ORDER BY
            x.created_at,
            x.booking_ref
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_promo_cost_usd,
        b.budget_usd
      FROM
        x
      LEFT JOIN
        budget b
        ON b.promo_code = x.promo_code
    )

    -- One row per promo = the booking at which cumulative spend first reaches the budget (the cutoff).
    --   cutoff_timestamp                : its created_at. Bookings before it are within budget (Wego bears
    --                                     0); bookings after it are over budget (Wego bears the full discount).
    --   cutoff_booking_ref              : this cutoff booking is SPLIT across the budget boundary.
    --   cutoff_partial_promo_amount_usd : Wego's overflow share of that split booking
    --                                     = cumulative_promo_cost_usd - budget_usd (partner covers the rest).
    --                                     Always in [0, discount).
    -- A promo absent here = budget never exhausted in data's timeframe; all its bookings within budget.
    SELECT
      promo_code,
      created_at AS cutoff_timestamp,
      booking_ref AS cutoff_booking_ref,
      SAFE_CAST(cumulative_promo_cost_usd - budget_usd AS NUMERIC) AS cutoff_partial_promo_amount_usd
    FROM
      per_booking
    WHERE TRUE
      AND cumulative_promo_cost_usd >= budget_usd
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY created_at, booking_ref) = 1
);
SET cutoff_KFH100 = (SELECT cutoff_timestamp FROM promo_cutoffs WHERE promo_code = 'KFH100');
SET cutoff_ref_KFH100 = (SELECT cutoff_booking_ref FROM promo_cutoffs WHERE promo_code = 'KFH100');
SET cutoff_partial_KFH100 = (SELECT cutoff_partial_promo_amount_usd FROM promo_cutoffs WHERE promo_code = 'KFH100');

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1` AS (
	WITH
exchange_rates AS (
	SELECT
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  FROM
    `wego-cloud.analytics.exchange_rates*`
  WHERE TRUE
    AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2021-01-14')
    AND quote = 'USD'
)

SELECT
  hb.booking_id,
  hb.booking_type,
  cm.ns_market AS market,
  -- Pay at Property: the room base price is paid at the property, so it is not Wego GMV. Markup and tax
  -- are kept (Wego earns the markup and charges the tax) and post via the GMV-sales transform.
  SAFE_CAST(
    IF(
      hb.booking_type = 'PAY_AT_PROPERTY',
      0.0,
      hb.wego_base_price_usd
    )
    AS NUMERIC
  ) AS wego_base_price_usd,
  SAFE_CAST(hb.wego_taxes_usd AS NUMERIC) AS wego_taxes_usd,
  SAFE_CAST(hb.wego_markup_amount_usd AS NUMERIC) AS wego_markup_amount_usd,
  SAFE_CAST(hb.vendor_marketing_fee_usd AS NUMERIC) AS vendor_marketing_fee_usd,
  hb.supplier_name,
  /*Newly added fields*/
  hb.check_in,
  IF(
    hb_c.created_time IS NOT NULL,
    hb_c.created_time,
    hb.check_in
  ) AS check_in_recalculated, /*See CHANGELOG.md*/
  SAFE_CAST(hb.vendor_base_price_usd AS NUMERIC) AS vendor_base_price_usd,
  SAFE_CAST(hb.wego_discount_amount_usd AS NUMERIC) AS wego_discount_amount_usd,
  SAFE_CAST(hb.promo_discount_amount_usd AS NUMERIC) AS promo_discount_amount_usd,
  (CASE
    -- Ongoing, still within budget
    WHEN TRIM(hb.promo_code) = 'KFH100' AND cutoff_KFH100 IS NULL THEN SAFE_CAST(0 AS NUMERIC)
    
    -- Eventually overbudget but it's in the future
    WHEN TRIM(hb.promo_code) = 'KFH100' AND hb.created_time < cutoff_KFH100 THEN SAFE_CAST(0 AS NUMERIC)
    
    -- Partially beared by Wego at exact cutoff booking
    WHEN TRIM(hb.promo_code) = 'KFH100' AND hb.client_booking_id = cutoff_ref_KFH100 THEN cutoff_partial_KFH100

    -- All overbudget bookings, and also normal promo (non-partnership)
    ELSE SAFE_CAST(hb.promo_discount_amount_usd AS NUMERIC)
  END) AS wego_beared_promo_discount_amount_usd,
  (CASE
    WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
		ELSE 'All Others'
  END) AS Site_Code,
  SAFE_CAST(
    hb.exchange_rate_to_usd_from_other_currency_code
    AS NUMERIC
  ) AS exchange_rate_to_usd_from_other_currency_code,
  hb.other_currency_code,
  STRUCT(
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_sar,
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_egp,
    -- SAFE_CAST(
    --   hb.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount
    --   AS NUMERIC
    -- ) AS invoice_gov_tax_amount_pkr,
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_usd /*Default in USD*/
  ) AS invoice_gov_tax_amount
FROM (
  SELECT
    *
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_confirmed_logs_audit`
  WHERE TRUE
    AND source_product = 'bow'
) hb
LEFT JOIN
  `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit` hb_c
  USING(client_booking_id) /*To get cancellation date (created_time)*/
LEFT JOIN
  exchange_rates AS exchange_rates_sar
  ON 'SAR' = exchange_rates_sar.base
    AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_sar.effective)
LEFT JOIN
  exchange_rates AS exchange_rates_egp
  ON 'EGP' = exchange_rates_egp.base
    AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_egp.effective)
-- LEFT JOIN
--   exchange_rates AS exchange_rates_pkr
--   ON 'PKR' = exchange_rates_pkr.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_pkr.effective)
LEFT JOIN
  `wego-cloud.analytics.countries_misc` cm
  ON hb.site_code = cm.country_code
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_cancellation_v1_1` AS (
	WITH
exchange_rates AS (
	SELECT
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  FROM
    `wego-cloud.analytics.exchange_rates*`
  WHERE TRUE
    AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11')
    AND quote = 'USD'
)

SELECT
  hb.booking_id,
  cm.ns_market AS market,
  hb.booking_type, /*Pay at Property is excluded from the GMV/COGS transforms; it posts only the marketing fee*/
  SAFE_CAST(hb.wego_total_price_usd AS NUMERIC) AS wego_total_price_usd,
  SAFE_CAST(hb.user_refund_amount_usd AS NUMERIC) AS user_refund_amount_usd,
  SAFE_CAST(hb.vendor_refund_amount_usd AS NUMERIC) AS vendor_refund_amount_usd,
  /*Newly added fields*/
  SAFE_CAST(hb.vendor_marketing_fee_usd AS NUMERIC) AS vendor_marketing_fee_usd,
  hb.supplier_name,
  hb.check_in,
  hb.created_time,
  (CASE
    WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
		ELSE 'All Others'
  END) AS Site_Code,
  SAFE_CAST(
    hb.exchange_rate_to_usd_from_other_currency_code
    AS NUMERIC
  ) AS exchange_rate_to_usd_from_other_currency_code,
  hb.other_currency_code,
  STRUCT(
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_sar,
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_egp,
    -- SAFE_CAST(
    --   hb.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount
    --   AS NUMERIC
    -- ) AS invoice_gov_tax_amount_pkr,
    SAFE_CAST(
      hb.invoice_gov_tax_details.vat_amount
      AS NUMERIC
    ) AS invoice_gov_tax_amount_usd /*Default in USD*/
  ) AS invoice_gov_tax_amount
FROM (
  SELECT
    *
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit`
  WHERE TRUE
    AND source_product = 'bow'
) hb
LEFT JOIN
  exchange_rates AS exchange_rates_sar
  ON 'SAR' = exchange_rates_sar.base
    AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_sar.effective)
LEFT JOIN
  exchange_rates AS exchange_rates_egp
  ON 'EGP' = exchange_rates_egp.base
    AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_egp.effective)
-- LEFT JOIN
--   exchange_rates AS exchange_rates_pkr
--   ON 'PKR' = exchange_rates_pkr.base
--     AND DATE(hb.invoice_gov_tax_details.created_at) = DATE(exchange_rates_pkr.effective)
LEFT JOIN
  `wego-cloud.analytics.countries_misc` cm
  ON hb.site_code = cm.country_code
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_booking_v1_1` AS (
	SELECT
  *,
  IFNULL(vendor_base_price_usd, 0) AS cogs_value
FROM (
  SELECT
    hb.booking_id,
    cm.ns_market as market,
    hb.booking_type, /*Pay at Property is excluded from the GMV/COGS transforms; it posts only the marketing fee*/
    SAFE_CAST(hb.vendor_base_price_usd AS NUMERIC) AS vendor_base_price_usd,
    /*Newly added fields*/
    hb.check_in,
    IF(
      hb_c.created_time IS NOT NULL,
      hb_c.created_time,
      hb.check_in
    ) AS check_in_recalculated, /*See CHANGELOG.md*/
    (CASE
      WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
      ELSE 'All Others'
    END) AS Site_Code,
    hb.supplier_name,
    SAFE_CAST(
      hb.exchange_rate_to_usd_from_other_currency_code
      AS NUMERIC
    ) AS exchange_rate_to_usd_from_other_currency_code,
    hb.other_currency_code,
    SAFE_CAST(hb.optimized_cost_usd AS NUMERIC) AS optimized_cost_usd
  FROM (
    SELECT
      *
    FROM
      `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_confirmed_logs_audit`
    WHERE TRUE
      AND source_product = 'bow'
  ) hb
  LEFT JOIN
    `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit` hb_c
    USING(client_booking_id) /*To get cancellation date (created_time)*/
  LEFT JOIN
    `wego-cloud.analytics.countries_misc` cm on hb.site_code = cm.country_code
)
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_cancellation_v1_1` AS (
	SELECT
  *,
  IFNULL(vendor_base_price_usd, 0) - IFNULL(vendor_refund_amount_usd, 0) AS cogs_value
FROM (
  SELECT
    hb.booking_id,
    cm.ns_market AS market,
    hb.booking_type, /*Pay at Property is excluded from the GMV/COGS transforms; it posts only the marketing fee*/
    SAFE_CAST(hb.vendor_base_price_usd AS NUMERIC) AS vendor_base_price_usd,
    SAFE_CAST(hb.vendor_refund_amount_usd AS NUMERIC) AS vendor_refund_amount_usd,
    SAFE_CAST(hb.user_refund_amount_usd AS NUMERIC) AS user_refund_amount_usd,
    /*Newly added fields*/
    hb.check_in,
    hb.created_time,
    (CASE
      WHEN hb.site_code IN ('SA', 'EG') THEN hb.site_code
      ELSE 'All Others'
    END) AS Site_Code,
    supplier_name,
    SAFE_CAST(
      hb.exchange_rate_to_usd_from_other_currency_code
      AS NUMERIC
    ) AS exchange_rate_to_usd_from_other_currency_code,
    hb.other_currency_code
  FROM (
    SELECT
      *
    FROM
      `wego-cloud.aaaaa_temporary_export_folder.hotels_bookings_cancelled_logs_audit`
    WHERE TRUE
      AND source_product = 'bow'
  ) hb
  LEFT JOIN
    `wego-cloud.analytics.countries_misc` cm
    ON hb.site_code = cm.country_code
)
);
-- ============================================================
-- GMV Booking Details
-- Source: gmv_hotels_booking_v1_1
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_booking_details` AS (
    SELECT
        booking_id                                              AS `Booking ID`,
        booking_type                                            AS `Booking Type`, -- v1.1 GMV/COGS exclude PAY_AT_PROPERTY; marketing fee keeps it
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        check_in                                                AS `Check In`,
        check_in_recalculated                                   AS `Check In Recalculated`,
        supplier_name                                           AS `Supplier Name`,

        -- Sales_GMV_OTA_Hotels components
        wego_base_price_usd                                     AS `Wego Base Price USD`,
        wego_markup_amount_usd                                  AS `Wego Markup Amount USD`,
        wego_taxes_usd                                          AS `Wego Taxes USD`,

        -- Discount_GMV_OTA_Hotels components
        wego_discount_amount_usd                                AS `Wego Discount Amount USD`,
        promo_discount_amount_usd                               AS `Promo Discount Amount USD`,
        wego_beared_promo_discount_amount_usd                   AS `Wego Beared Promo Discount Amount USD`,

        -- Sponsored_Discount_GMV_OTA_Hotels component (campaign discount / rebate lines)
        IFNULL(promo_discount_amount_usd, 0)
          - IFNULL(wego_beared_promo_discount_amount_usd, 0)    AS `Sponsored Promo Discount Amount USD`,

        -- Marketing_Fee component
        vendor_marketing_fee_usd                                AS `Vendor Marketing Fee USD`,

        -- VAT_Output components
        invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

    FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_v1_1`
);

-- ============================================================
-- GMV Cancellation Details
-- Source: gmv_hotels_cancellation_v1_1
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details` AS (
    SELECT
        booking_id                                              AS `Booking ID`,
        booking_type                                            AS `Booking Type`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,
        supplier_name                                           AS `Supplier Name`,

        -- Sales_GMV_OTA_Hotels component
        user_refund_amount_usd                                  AS `User Refund Amount USD`,

        -- Marketing_Fee_Cancelled component
        vendor_marketing_fee_usd                                AS `Vendor Marketing Fee USD`,

        -- VAT_Output components
        invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

    FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_v1_1`
);

-- ============================================================
-- COGS Booking Details
-- Source: cogs_hotels_booking_v1_1
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_booking_details` AS (
    SELECT
        booking_id                                              AS `Booking ID`,
        booking_type                                            AS `Booking Type`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        supplier_name                                           AS `Supplier Name`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        check_in                                                AS `Check In`,
        check_in_recalculated                                   AS `Check In Recalculated`,

        -- Hotels_Supplier_Costs_COGS_OTA_Hotels components
        vendor_base_price_usd                                   AS `Vendor Base Price USD`,
        optimized_cost_usd                                      AS `Optimized Cost USD`

    FROM `aaaaa_temporary_export_folder.cogs_hotels_booking_v1_1`
);

-- ============================================================
-- COGS Cancellation Details
-- Source: cogs_hotels_cancellation_v1_1
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_hotels_cancellation_details` AS (
    SELECT
        booking_id                                              AS `Booking ID`,
        booking_type                                            AS `Booking Type`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        supplier_name                                           AS `Supplier Name`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Hotels_Supplier_Costs_COGS_OTA_Hotels component
        vendor_refund_amount_usd                                AS `Vendor Refund Amount USD`

    FROM `aaaaa_temporary_export_folder.cogs_hotels_cancellation_v1_1`
);

-- ============================================================
-- VCC / UATP Incentive Details
-- Source: wego_analytics.hotels_bookings (conversions_tracked = 1)
-- Covers both VCC partners (Apiso / Nium via vcc_provider) and UATP (vc_partner_account LIKE '%UATP%')
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.vcc_uatp_hotels_details` AS (
    SELECT
        hb.booking_id                                           AS `Booking ID`,
        hb.site_code                                            AS `Site Code`,
        cm.ns_market                                            AS `Market`, -- same market mapping as GMV/COGS details and HOTEL_VCC_INCENTIVE
        hb.created_at                                           AS `Created Time`,
        hb.check_in                                             AS `Check In`,

        -- Partner identifiers
        hb.vc_partner_account                                   AS `VC Partner Account`,
        hb.vc_partner_card_pool_id                              AS `VC Partner Card Pool ID`,
        CASE
            WHEN vcc_provider.Partner IN ('Apiso', 'Nium') THEN 'Apiso_Nium'
            WHEN vcc_provider.Partner IS NOT NULL           THEN vcc_provider.Partner
            WHEN hb.vc_partner_account LIKE '%UATP%'         THEN 'UATP'
            ELSE NULL
        END                                                     AS `VC Partner`,
        hb.vc_rebate_currency_code                              AS `VC Rebate Currency Code`,

        -- VCC Rebate components (USD at source; multi-currency split lives in source script)
        hb.vc_rebate_value_usd                                  AS `VC Rebate Value USD`,
        hb.vc_rebate_fee_usd                                    AS `VC Rebate Fee USD`,
        IFNULL(hb.vc_rebate_value_usd, 0)
          + IFNULL(hb.vc_rebate_fee_usd, 0)                     AS `VCC Rebate Accrual USD`

    FROM `wego_analytics.hotels_bookings` AS hb
    LEFT JOIN `wego-cloud.ota_data_upload_template_working_30Oct24.vcc_provider` AS vcc_provider
        ON hb.vc_partner_card_pool_id = vcc_provider.ClientID
    LEFT JOIN `wego-cloud.analytics.countries_misc` AS cm
        ON hb.site_code = cm.country_code
    WHERE hb.conversions_tracked = 1
);
{% endraw %}
