{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : bow_gmv_cogs_flights_hotels_pivoted
-- Destination: aaaaa_temporary_export_folder.gmv_cogs_flights_hotels_unpivoted  (unchanged)
-- Schedule   : every day 07:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_flights_booking_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_flights_cancellation_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_flights_exchange_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_flights_insurance_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_hotels_booking_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'cogs_hotels_cancellation_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'gmv_flights_booking_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'gmv_flights_cancellation_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'gmv_flights_exchange_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'gmv_hotels_booking_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'gmv_hotels_cancellation_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'vcc_uatp_flights_details') }}
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'vcc_uatp_hotels_details') }}
{% raw %}
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_cogs_flights_hotels_unpivoted` AS
(
	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'GMV_SALES_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_flights_booking_details`,
	UNNEST([
	  STRUCT('Markup' AS type, `Markup` AS value),
	  STRUCT('Markdown', `Markdown`),
	  STRUCT('Vendor Total Amount USD', `Vendor Total Amount USD`),
	  STRUCT('Ancillary Vendor Total Amount USD', `Ancillary Vendor Total Amount USD`),
	  STRUCT('Ancillary Margin Amount USD', `Ancillary Margin Amount USD`),
	  STRUCT('Payment Fee USD', `Payment Fee USD`),
	  STRUCT('Booking Fee USD', `Booking Fee USD`),
	  STRUCT('Insurance Total Amount USD', `Insurance Total Amount USD`),
	  STRUCT('Promo Discount Amount USD', `Promo Discount Amount USD`),
	  STRUCT('Invoice Gov Tax Amount USD', `Invoice Gov Tax Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'GMV_SALES_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_flights_cancellation_details`,
	UNNEST([
	  STRUCT('User Refund Amount USD' AS type, `User Refund Amount USD` AS value),
	  STRUCT('Refund Fee USD', `Refund Fee USD`),
	  STRUCT('Invoice Gov Tax Amount USD', `Invoice Gov Tax Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'GMV_SALES_EXCHANGE' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_flights_exchange_details`,
	UNNEST([
	  STRUCT('Vendor Total Amount USD' AS type, `Vendor Total Amount USD` AS value),
	  STRUCT('Change Fee USD', `Change Fee USD`),
	  STRUCT('Exchange Admin Fee USD', `Exchange Admin Fee USD`),
	  STRUCT('Payment Fee Amount USD', `Payment Fee Amount USD`),
	  STRUCT('Invoice Gov Tax Amount USD', `Invoice Gov Tax Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'COGS_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_flights_booking_details`,
	UNNEST([
	  STRUCT('Vendor Total Amount USD' AS type, `Vendor Total Amount USD` AS value),
	  STRUCT('Optimize Cost USD', `Optimize Cost USD`),
	  STRUCT('Est Vendor Issuance Fee USD', `Est Vendor Issuance Fee USD`),
	  STRUCT('Ancillary Vendor Total Amount USD', `Ancillary Vendor Total Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created At`) AS created_date,
	  'Flight' AS product,
	  'COGS_INSURANCE' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_flights_insurance_details`,
	UNNEST([
	  STRUCT('Insurance Total Amount USD' AS type, `Insurance Total Amount USD` AS value),
	  STRUCT('Wego Commission USD', `Wego Commission USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'COGS_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_flights_cancellation_details`,
	UNNEST([
	  STRUCT('GDS Refund Amount USD' AS type, `GDS Refund Amount USD` AS value),
	  STRUCT('Est Vendor Issuance Fee USD', `Est Vendor Issuance Fee USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'COGS_EXCHANGE' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_flights_exchange_details`,
	UNNEST([
	  STRUCT('Vendor Total Amount USD' AS type, `Vendor Total Amount USD` AS value),
	  STRUCT('Est Vendor Issuance Fee USD', `Est Vendor Issuance Fee USD`),
	  STRUCT('Optimize Cost USD', `Optimize Cost USD`),
	  STRUCT('Change Fee USD', `Change Fee USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	/*============================================================*/

	UNION ALL

	SELECT
	  DATE(`Check In Recalculated`) AS created_date,
	  'Hotel' AS product,
	  'GMV_SALES_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_details`,
	UNNEST([
	  STRUCT('Wego Base Price USD' AS type, `Wego Base Price USD` AS value),
	  STRUCT('Wego Markup Amount USD', `Wego Markup Amount USD`),
	  STRUCT('Wego Taxes USD', `Wego Taxes USD`),
	  STRUCT('Wego Discount Amount USD', `Wego Discount Amount USD`),
	  STRUCT('Promo Discount Amount USD', `Promo Discount Amount USD`),
	  STRUCT('Invoice Gov Tax Amount USD', `Invoice Gov Tax Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Hotel' AS product,
	  'GMV_SALES_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details`,
	UNNEST([
	  STRUCT('User Refund Amount USD' AS type, `User Refund Amount USD` AS value),
	  STRUCT('Invoice Gov Tax Amount USD', `Invoice Gov Tax Amount USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Check In Recalculated`) AS created_date,
	  'Hotel' AS product,
	  'COGS_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_hotels_booking_details`,
	UNNEST([
	  STRUCT('Vendor Base Price USD' AS type, `Vendor Base Price USD` AS value),
	  STRUCT('Optimized Cost USD', `Optimized Cost USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Hotel' AS product,
	  'COGS_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.cogs_hotels_cancellation_details`,
	UNNEST([
	  STRUCT('Vendor Refund Amount USD' AS type, `Vendor Refund Amount USD` AS value)
	]) AS m
	GROUP BY 1, 2, 3, 4
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_cogs_flights_hotels_pivoted` AS
(
	WITH
	-- FLIGHTS
	gmv_sales_booking AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`Markup`,0)
	      + IFNULL(`Markdown`,0)
	      + IFNULL(`Vendor Total Amount USD`,0)
	      + IFNULL(`Ancillary Vendor Total Amount USD`,0)
	      + IFNULL(`Ancillary Margin Amount USD`,0)
	      + IFNULL(`Payment Fee USD`,0)
	      + IFNULL(`Booking Fee USD`,0)
	      + IFNULL(`Insurance Total Amount USD`,0)
	      - IFNULL(`Promo Discount Amount USD`,0)
	    ) AS GMV_SALES_BOOKING
	  FROM `aaaaa_temporary_export_folder.gmv_flights_booking_details`
	  GROUP BY 1
	),

	cogs_bookings AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`Vendor Total Amount USD`,0)
	      + IFNULL(`Optimize Cost USD`,0)
	      + IFNULL(`Est Vendor Issuance Fee USD`,0)
	      + IFNULL(`Ancillary Vendor Total Amount USD`,0)
	    ) AS COGS_BOOKING
	  FROM `aaaaa_temporary_export_folder.cogs_flights_booking_details`
	  GROUP BY 1
	),

	cogs_insurance AS (
	  SELECT
	    DATE(`Created At`) AS `Created Date`,
	    SUM(
	      IFNULL(`Insurance Total Amount USD`,0)
	      - IFNULL(`Wego Commission USD`,0)
	    ) AS COGS_INSURANCE
	  FROM `aaaaa_temporary_export_folder.cogs_flights_insurance_details`
	  GROUP BY 1
	),

	gmv_sales_exchange AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`Vendor Total Amount USD`,0)
	      + IFNULL(`Change Fee USD`,0)
	      + IFNULL(`Exchange Admin Fee USD`,0)
	      + IFNULL(`Payment Fee Amount USD`,0)
	    ) AS GMV_SALES_EXCHANGE
	  FROM `aaaaa_temporary_export_folder.gmv_flights_exchange_details`
	  GROUP BY 1
	),

	cogs_exchange AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`Vendor Total Amount USD`,0)
	      + IFNULL(`Est Vendor Issuance Fee USD`,0)
	      + IFNULL(`Optimize Cost USD`,0)
	      + IFNULL(`Change Fee USD`,0)
	    ) AS COGS_EXCHANGE
	  FROM `aaaaa_temporary_export_folder.cogs_flights_exchange_details`
	  GROUP BY 1
	),

	gmv_sales_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`User Refund Amount USD`,0)
	      - IFNULL(`Refund Fee USD`,0)
	    ) AS GMV_SALES_CANCELLED
	  FROM `aaaaa_temporary_export_folder.gmv_flights_cancellation_details`
	  GROUP BY 1
	),

	cogs_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`GDS Refund Amount USD`,0)
	      + IFNULL(`Est Vendor Issuance Fee USD`,0)
	    ) AS COGS_CANCELLATION
	  FROM `aaaaa_temporary_export_folder.cogs_flights_cancellation_details`
	  GROUP BY 1
	),

	-- HOTELS
	hotel_gmv_sales_booking AS (
	  SELECT
	    DATE(`Check In Recalculated`) AS `Created Date`,
	    SUM(
	      IFNULL(`Wego Base Price USD`,0)
	      + IFNULL(`Wego Markup Amount USD`,0)
	      + IFNULL(`Wego Taxes USD`,0)
	      - IFNULL(`Wego Discount Amount USD`,0)
	      - IFNULL(`Promo Discount Amount USD`,0)
	    ) AS GMV_SALES_BOOKING
	  FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_details`
	  GROUP BY 1
	),

	hotel_gmv_sales_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(IFNULL(`User Refund Amount USD`,0)) AS GMV_SALES_CANCELLED
	  FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details`
	  GROUP BY 1
	),

	hotel_cogs_bookings AS (
	  SELECT
	    DATE(`Check In Recalculated`) AS `Created Date`,
	    SUM(
	      IFNULL(`Vendor Base Price USD`,0)
	      + IFNULL(`Optimized Cost USD`,0)
	    ) AS COGS_BOOKING
	  FROM `aaaaa_temporary_export_folder.cogs_hotels_booking_details`
	  GROUP BY 1
	),

	hotel_cogs_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(
	      IFNULL(`Vendor Refund Amount USD`,0)
	    ) AS COGS_CANCELLATION
	  FROM `aaaaa_temporary_export_folder.cogs_hotels_cancellation_details`
	  GROUP BY 1
	)

	-- FINAL OUTPUT
	SELECT
	  'Flight' AS PRODUCT,
	  b.`Created Date`,
	  b.GMV_SALES_BOOKING,
	  COALESCE(e.GMV_SALES_EXCHANGE,0) AS GMV_SALES_EXCHANGE,
	  COALESCE(c.GMV_SALES_CANCELLED,0) AS GMV_SALES_CANCELLED,
	  b.GMV_SALES_BOOKING
	    + COALESCE(e.GMV_SALES_EXCHANGE,0)
	    - COALESCE(c.GMV_SALES_CANCELLED,0) AS NET_GMV,

	  COALESCE(cb.COGS_BOOKING,0) AS COGS_BOOKING,
	  COALESCE(ci.COGS_INSURANCE,0) AS COGS_INSURANCE,
	  COALESCE(ce.COGS_EXCHANGE,0) AS COGS_EXCHANGE,
	  COALESCE(cc.COGS_CANCELLATION,0) AS COGS_CANCELLATION,

	  COALESCE(cb.COGS_BOOKING,0)
	    + COALESCE(ci.COGS_INSURANCE,0)
	    + COALESCE(ce.COGS_EXCHANGE,0)
	    - COALESCE(cc.COGS_CANCELLATION,0) AS NET_COGS,

	  (
	    b.GMV_SALES_BOOKING
	    + COALESCE(e.GMV_SALES_EXCHANGE,0)
	    - COALESCE(c.GMV_SALES_CANCELLED,0)
	  )
	  -
	  (
	    COALESCE(cb.COGS_BOOKING,0)
	    + COALESCE(ci.COGS_INSURANCE,0)
	    + COALESCE(ce.COGS_EXCHANGE,0)
	    - COALESCE(cc.COGS_CANCELLATION,0)
	  ) AS GROSS_PROFIT

	FROM gmv_sales_booking b
	LEFT JOIN gmv_sales_exchange e USING (`Created Date`)
	LEFT JOIN gmv_sales_cancelled c USING (`Created Date`)
	LEFT JOIN cogs_bookings cb USING (`Created Date`)
	LEFT JOIN cogs_insurance ci USING (`Created Date`)
	LEFT JOIN cogs_exchange ce USING (`Created Date`)
	LEFT JOIN cogs_cancelled cc USING (`Created Date`)

	UNION ALL

	SELECT
	  'Hotel' AS PRODUCT,
	  b.`Created Date`,
	  b.GMV_SALES_BOOKING,
	  NULL AS GMV_SALES_EXCHANGE,
	  COALESCE(c.GMV_SALES_CANCELLED,0) AS GMV_SALES_CANCELLED,

	  b.GMV_SALES_BOOKING
	    - COALESCE(c.GMV_SALES_CANCELLED,0) AS NET_GMV,

	  COALESCE(cb.COGS_BOOKING,0) AS COGS_BOOKING,
	  NULL AS COGS_INSURANCE,
	  NULL AS COGS_EXCHANGE,
	  COALESCE(cc.COGS_CANCELLATION,0) AS COGS_CANCELLATION,

	  COALESCE(cb.COGS_BOOKING,0)
	    - COALESCE(cc.COGS_CANCELLATION,0) AS NET_COGS,

	  (
	    b.GMV_SALES_BOOKING
	    - COALESCE(c.GMV_SALES_CANCELLED,0)
	  )
	  -
	  (
	    COALESCE(cb.COGS_BOOKING,0)
	    - COALESCE(cc.COGS_CANCELLATION,0)
	  ) AS GROSS_PROFIT

	FROM hotel_gmv_sales_booking b
	LEFT JOIN hotel_gmv_sales_cancelled c
	  USING (`Created Date`)
	LEFT JOIN hotel_cogs_bookings cb
	  USING (`Created Date`)
	LEFT JOIN hotel_cogs_cancelled cc
	  USING (`Created Date`)
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.commission_incentive_flights_hotels_unpivoted` AS
(
	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'COMMISSION_INCENTIVE_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_flights_booking_details`,
	UNNEST([
	  STRUCT('Vendor Commissions PLB USD' AS type, `Vendor Commissions PLB USD` AS value),
	  STRUCT('Vendor Commissions IATA USD', `Vendor Commissions IATA USD`),
	  STRUCT('Vendor Commissions Upfront Backend USD', `Vendor Commissions Upfront Backend USD`),
	  STRUCT('GDS Segment Fee USD', `GDS Segment Fee USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'COMMISSION_INCENTIVE_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_flights_cancellation_details`,
	UNNEST([
	  STRUCT('Vendor Commissions PLB USD' AS type, `Vendor Commissions PLB USD` AS value),
	  STRUCT('Vendor Commissions IATA USD', `Vendor Commissions IATA USD`),
	  STRUCT('Vendor Commissions Upfront Backend USD', `Vendor Commissions Upfront Backend USD`),
	  STRUCT('GDS Segment Fee USD', `GDS Segment Fee USD`)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Check In Recalculated`) AS created_date,
	  'Hotel' AS product,
	  'COMMISSION_INCENTIVE_BOOKING' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_details`,
	UNNEST([
	  STRUCT('Vendor Marketing Fee USD' AS type, `Vendor Marketing Fee USD` AS value)
	]) AS m
	GROUP BY 1, 2, 3, 4

	UNION ALL

	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Hotel' AS product,
	  'COMMISSION_INCENTIVE_CANCELLATION' AS stage,
	  m.type,
	  SUM(m.value) AS value
	FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details`,
	UNNEST([
	  STRUCT('Vendor Marketing Fee USD' AS type, `Vendor Marketing Fee USD` AS value)
	]) AS m
	GROUP BY 1, 2, 3, 4
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.commission_incentive_flights_hotels_pivoted` AS
(
	WITH
	-- FLIGHTS
	flights_booking AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(IFNULL(`Vendor Commissions PLB USD`,0))              AS COMMISSION_PLB,
	    SUM(IFNULL(`Vendor Commissions IATA USD`,0))             AS COMMISSION_IATA,
	    SUM(IFNULL(`Vendor Commissions Upfront Backend USD`,0))  AS COMMISSION_UPFRONT_BACKEND,
	    SUM(IFNULL(`GDS Segment Fee USD`,0))                     AS INCENTIVE
	  FROM `aaaaa_temporary_export_folder.gmv_flights_booking_details`
	  GROUP BY 1
	),

	flights_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(IFNULL(`Vendor Commissions PLB USD`,0))              AS COMMISSION_PLB,
	    SUM(IFNULL(`Vendor Commissions IATA USD`,0))             AS COMMISSION_IATA,
	    SUM(IFNULL(`Vendor Commissions Upfront Backend USD`,0))  AS COMMISSION_UPFRONT_BACKEND,
	    SUM(IFNULL(`GDS Segment Fee USD`,0))                     AS INCENTIVE
	  FROM `aaaaa_temporary_export_folder.gmv_flights_cancellation_details`
	  GROUP BY 1
	),

	-- HOTELS (no commission/incentive in source; marketing fee instead)
	hotels_booking AS (
	  SELECT
	    DATE(`Check In Recalculated`) AS `Created Date`,
	    SUM(IFNULL(`Vendor Marketing Fee USD`,0))   AS MARKETING_FEE
	  FROM `aaaaa_temporary_export_folder.gmv_hotels_booking_details`
	  GROUP BY 1
	),

	hotels_cancelled AS (
	  SELECT
	    DATE(`Created Time`) AS `Created Date`,
	    SUM(IFNULL(`Vendor Marketing Fee USD`,0))   AS MARKETING_FEE
	  FROM `aaaaa_temporary_export_folder.gmv_hotels_cancellation_details`
	  GROUP BY 1
	)

	-- FINAL OUTPUT (always: ticketed/confirmed − cancelled)
	SELECT
	  'Flight' AS PRODUCT,
	  b.`Created Date`,
	  b.COMMISSION_PLB                                                        AS COMMISSION_PLB_BOOKED,
	  COALESCE(c.COMMISSION_PLB,0)                                            AS COMMISSION_PLB_CANCELLED,
	  b.COMMISSION_PLB - COALESCE(c.COMMISSION_PLB,0)                         AS NET_COMMISSION_PLB,
	  b.COMMISSION_IATA                                                       AS COMMISSION_IATA_BOOKED,
	  COALESCE(c.COMMISSION_IATA,0)                                           AS COMMISSION_IATA_CANCELLED,
	  b.COMMISSION_IATA - COALESCE(c.COMMISSION_IATA,0)                       AS NET_COMMISSION_IATA,
	  b.COMMISSION_UPFRONT_BACKEND                                            AS COMMISSION_UPFRONT_BACKEND_BOOKED,
	  COALESCE(c.COMMISSION_UPFRONT_BACKEND,0)                                AS COMMISSION_UPFRONT_BACKEND_CANCELLED,
	  b.COMMISSION_UPFRONT_BACKEND - COALESCE(c.COMMISSION_UPFRONT_BACKEND,0) AS NET_COMMISSION_UPFRONT_BACKEND,
	  b.INCENTIVE                                                             AS INCENTIVE_BOOKED,
	  COALESCE(c.INCENTIVE,0)                                                 AS INCENTIVE_CANCELLED,
	  b.INCENTIVE  - COALESCE(c.INCENTIVE,0)                                  AS NET_INCENTIVE,
	  NULL                                                                    AS MARKETING_FEE_BOOKED,
	  NULL                                                                    AS MARKETING_FEE_CANCELLED,
	  NULL                                                                    AS NET_MARKETING_FEE
	FROM flights_booking b
	LEFT JOIN flights_cancelled c USING (`Created Date`)

	UNION ALL

	SELECT
	  'Hotel' AS PRODUCT,
	  b.`Created Date`,
	  NULL                                            AS COMMISSION_PLB_BOOKED,
	  NULL                                            AS COMMISSION_PLB_CANCELLED,
	  NULL                                            AS NET_COMMISSION_PLB,
	  NULL                                            AS COMMISSION_IATA_BOOKED,
	  NULL                                            AS COMMISSION_IATA_CANCELLED,
	  NULL                                            AS NET_COMMISSION_IATA,
	  NULL                                            AS COMMISSION_UPFRONT_BACKEND_BOOKED,
	  NULL                                            AS COMMISSION_UPFRONT_BACKEND_CANCELLED,
	  NULL                                            AS NET_COMMISSION_UPFRONT_BACKEND,
	  NULL                                            AS INCENTIVE_BOOKED,
	  NULL                                            AS INCENTIVE_CANCELLED,
	  NULL                                            AS NET_INCENTIVE,
	  b.MARKETING_FEE                                 AS MARKETING_FEE_BOOKED,
	  COALESCE(c.MARKETING_FEE,0)                     AS MARKETING_FEE_CANCELLED,
	  b.MARKETING_FEE - COALESCE(c.MARKETING_FEE,0)   AS NET_MARKETING_FEE
	FROM hotels_booking b
	LEFT JOIN hotels_cancelled c USING (`Created Date`)
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.vcc_flights_hotels_pivoted` AS
(
	-- FLIGHTS (Created Time), non-UATP partners only
	SELECT
	  'Flight' AS PRODUCT,
	  DATE(`Created Time`) AS `Created Date`,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS VCC_REBATE_ACCRUAL_USD
	FROM `aaaaa_temporary_export_folder.vcc_uatp_flights_details`
	WHERE IFNULL(`VC Partner`,'') != 'UATP'
	GROUP BY 1, 2

	UNION ALL

	-- HOTELS (Check In), non-UATP partners only
	SELECT
	  'Hotel' AS PRODUCT,
	  DATE(`Check In`) AS `Created Date`,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS VCC_REBATE_ACCRUAL_USD
	FROM `aaaaa_temporary_export_folder.vcc_uatp_hotels_details`
	WHERE IFNULL(`VC Partner`,'') != 'UATP'
	GROUP BY 1, 2
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.uatp_flights_hotels_pivoted` AS
(
	-- FLIGHTS (Created Time), UATP partner only
	SELECT
	  'Flight' AS PRODUCT,
	  DATE(`Created Time`) AS `Created Date`,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS VCC_REBATE_ACCRUAL_USD
	FROM `aaaaa_temporary_export_folder.vcc_uatp_flights_details`
	WHERE IFNULL(`VC Partner`,'') = 'UATP'
	GROUP BY 1, 2

	UNION ALL

	-- HOTELS (Check In), UATP partner only
	SELECT
	  'Hotel' AS PRODUCT,
	  DATE(`Check In`) AS `Created Date`,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS VCC_REBATE_ACCRUAL_USD
	FROM `aaaaa_temporary_export_folder.vcc_uatp_hotels_details`
	WHERE IFNULL(`VC Partner`,'') = 'UATP'
	GROUP BY 1, 2
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.vcc_flights_hotels_unpivoted` AS
(
	-- FLIGHTS (Created Time), non-UATP partners only
	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'VCC_REBATE_ACCRUAL' AS stage,
	  'VCC Rebate Accrual USD' AS type,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS value
	FROM `aaaaa_temporary_export_folder.vcc_uatp_flights_details`
	WHERE IFNULL(`VC Partner`,'') != 'UATP'
	GROUP BY 1, 2, 3, 4

	UNION ALL

	-- HOTELS (Check In), non-UATP partners only
	SELECT
	  DATE(`Check In`) AS created_date,
	  'Hotel' AS product,
	  'VCC_REBATE_ACCRUAL' AS stage,
	  'VCC Rebate Accrual USD' AS type,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS value
	FROM `aaaaa_temporary_export_folder.vcc_uatp_hotels_details`
	WHERE IFNULL(`VC Partner`,'') != 'UATP'
	GROUP BY 1, 2, 3, 4
);

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.uatp_flights_hotels_unpivoted` AS
(
	-- FLIGHTS (Created Time), UATP partner only
	SELECT
	  DATE(`Created Time`) AS created_date,
	  'Flight' AS product,
	  'UATP_REBATE_ACCRUAL' AS stage,
	  'VCC Rebate Accrual USD' AS type,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS value
	FROM `aaaaa_temporary_export_folder.vcc_uatp_flights_details`
	WHERE IFNULL(`VC Partner`,'') = 'UATP'
	GROUP BY 1, 2, 3, 4

	UNION ALL

	-- HOTELS (Check In), UATP partner only
	SELECT
	  DATE(`Check In`) AS created_date,
	  'Hotel' AS product,
	  'UATP_REBATE_ACCRUAL' AS stage,
	  'VCC Rebate Accrual USD' AS type,
	  SUM(IFNULL(`VCC Rebate Accrual USD`,0)) AS value
	FROM `aaaaa_temporary_export_folder.vcc_uatp_hotels_details`
	WHERE IFNULL(`VC Partner`,'') = 'UATP'
	GROUP BY 1, 2, 3, 4
);
{% endraw %}
