{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Flights Bookings Offline
-- Destination: wego_analytics.flights_bookings_offline  (unchanged)
-- Schedule   : 1 of month 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
SELECT * EXCEPT (
		gateway_authorisation_fee,
		gateway_authorisation_fee_usd,
		gateway_blended_fee,
		gateway_blended_fee_usd,
		gateway_refund_fee,
		gateway_refund_fee_usd,
		gateway_void_fee,
		gateway_void_fee_usd,
		gateway_scheme_ic,
		gateway_scheme_ic_usd,
		est_vendor_refund_fee,
		est_vendor_refund_fee_usd,
		est_vendor_exchange_fee,
		est_vendor_exchange_fee_usd,
		est_gateway_void_fee,
		est_gateway_void_fee_usd,
		base,
		amount,
		effective
	),
	IFNULL(markup_amount_usd, 0) + IFNULL(total_commissions_usd, 0) AS gross_revenue_usd,
	IFNULL(markup_amount_usd, 0) + IFNULL(total_commissions_usd, 0) - IFNULL(cost_of_sales_usd,0) AS revenue_in_usd,
	IFNULL(markup_amount_usd, 0) + IFNULL(total_commissions_usd, 0) - IFNULL(cost_of_sales_usd,0) AS finance_revenue_usd,
	/*Start of generating a currency conversion using the payment currency code for Netsuite integration*/
	/* Netsuite entities supported: KSA (SAR), EG (EGP), PK (PKR), SG (everything else incl. AED & USD). */
  CASE
    WHEN temp_event2.payment_currency_code = 'SAR' THEN erSAR.amount
		WHEN temp_event2.payment_currency_code = 'EGP' THEN erEGP.amount
		WHEN temp_event2.payment_currency_code = 'PKR' THEN erPKR.amount
		ELSE 1.0
  END AS exchange_rate_to_usd_from_payment_currency_code_netsuite,

  CASE
    WHEN temp_event2.payment_currency_code = 'SAR' THEN 'SAR'
		WHEN temp_event2.payment_currency_code = 'EGP' THEN 'EGP'
		WHEN temp_event2.payment_currency_code = 'PKR' THEN 'PKR'
		ELSE 'USD'
  END AS payment_currency_code_netsuite,
  /*End of generating a currency conversion using the payment currency code for Netsuite integration*/
FROM
	/* -- temp_event2 --
	session_id
	booking_id
	site_code
	trip_category
	booking_type
	branded_fare_id
	gds_ref
	booking_ref
	ipcc
	payment_ref
	ticket_numbers
	currency_code
	payment_currency_code
	charged_currency_code
	vendor_currency_code
	created_at
	vendor_base_amount
	vendor_tax_amount
	vendor_total_amount
	vendor_total_amount_usd
	total_amount
	total_price
	total_price_usd
	adults_count
	children_count
	infants_count
	exchange_rate_from_currency_code_to_usd
	total_price_usd_from_vendor_currency_code
	markup_amount
	markup_amount_usd
	gds_segment_fee_usd
	vendor_commissions_upfront_backend_usd
	vendor_commissions_iata_usd
	vendor_commissions_plb_usd
	gds_segment_fee
	vendor_commissions_upfront_backend
	vendor_commissions_iata
	vendor_commissions_plb
	total_commissions
  ipcc_currency_code
  pcc
  vendor_id
  vendor
	total_commissions_usd
	ticket_issuance_fee_usd
	ticket_refund_fee_usd
	ticket_exchange_fee_usd
	ticket_exchanged_count
	ticket_refunded_count
	ticket_historical_count
	est_gateway_blended_fee
	est_gateway_blended_fee_usd
	est_gateway_refund_fee
	add_on_correction_ratio
	est_gateway_refund_fee_usd
	gateway_scheme_ic_usd
	gateway_scheme_ic
	est_vendor_issuance_fee
	est_vendor_issuance_fee_usd
	est_vendor_refund_fee
	est_vendor_refund_fee_usd
	est_vendor_exchange_fee
	est_vendor_exchange_fee_usd
	est_gateway_void_fee
	est_gateway_void_fee_usd
	est_gateway_authorisation_fee
	est_gateway_authorisation_fee_usd
	gateway_authorisation_fee
	gateway_authorisation_fee_usd
	gateway_blended_fee
	gateway_blended_fee_usd
	gateway_refund_fee
	gateway_refund_fee_usd
	gateway_void_fee
	gateway_void_fee_usd
	gateway_scheme_fixed_fee
	gateway_scheme_fixed_fee_usd
	gateway_scheme_variable_fee
	gateway_scheme_variable_fee_usd
	gateway_premium_fee
	gateway_premium_fee_usd
	est_total_gateway_fees
	total_gateway_fees
	est_total_gateway_fees_usd
	total_gateway_fees_usd
	est_total_vendor_fees
	est_total_vendor_fees_usd
	blended_total_gateway_fees
	blended_total_vendor_fees
	blended_total_gateway_fees_usd
	blended_total_vendor_fees_usd
	cost_of_sales
	cost_of_sales_usd
	*/
	(SELECT * EXCEPT (
		exchange_rate_from_currency_code_to_usd,
		total_price_usd_from_vendor_currency_code
	),
	----- COST OF SALES -----
	blended_total_gateway_fees + blended_total_vendor_fees AS cost_of_sales,
	blended_total_gateway_fees_usd + blended_total_vendor_fees_usd AS cost_of_sales_usd
FROM
(
	SELECT *,
		----- BLENDED FEES -----
		IF(created_at > '2022-08-18' AND charged_currency_code = 'SAR', est_total_gateway_fees, IF(total_gateway_fees IS NULL OR total_gateway_fees = 0, est_total_gateway_fees, total_gateway_fees)) AS blended_total_gateway_fees,
		est_total_vendor_fees AS blended_total_vendor_fees,

		IF(created_at > '2022-08-18' AND charged_currency_code = 'SAR', est_total_gateway_fees_usd, IF(total_gateway_fees_usd IS NULL OR total_gateway_fees_usd = 0, est_total_gateway_fees_usd, total_gateway_fees_usd)) AS blended_total_gateway_fees_usd,
		est_total_vendor_fees_usd AS blended_total_vendor_fees_usd,
	FROM
	(
		SELECT *,
      ----- GATEWAY FEES (Estimated, Actual) -----
      IFNULL(est_gateway_authorisation_fee,0) + IFNULL(est_gateway_blended_fee,0) + IFNULL(est_gateway_void_fee,0) AS est_total_gateway_fees,
      IFNULL(gateway_authorisation_fee,0) + IFNULL(gateway_blended_fee,0) + IFNULL(gateway_refund_fee,0) + IFNULL(gateway_void_fee,0) + IFNULL(gateway_scheme_fixed_fee,0) + IFNULL(gateway_scheme_variable_fee,0) + IFNULL(gateway_premium_fee,0) + IFNULL(gateway_scheme_ic,0) AS total_gateway_fees,

      IFNULL(est_gateway_authorisation_fee_usd,0) + IFNULL(est_gateway_blended_fee_usd,0) + IFNULL(est_gateway_void_fee_usd,0) AS est_total_gateway_fees_usd,
      IFNULL(gateway_authorisation_fee_usd,0) + IFNULL(gateway_blended_fee_usd,0) + IFNULL(gateway_refund_fee_usd,0) + IFNULL(gateway_void_fee_usd,0) + IFNULL(gateway_scheme_fixed_fee_usd,0) + IFNULL(gateway_scheme_variable_fee_usd,0) + IFNULL(gateway_premium_fee_usd,0) + IFNULL(gateway_scheme_ic_usd,0) AS total_gateway_fees_usd,

      ----- VENDOR FEES (Estimated, Actual) -----
      IFNULL(est_vendor_issuance_fee,0) + IFNULL(est_vendor_refund_fee,0) + IFNULL(est_vendor_exchange_fee,0) AS est_total_vendor_fees,

      IFNULL(est_vendor_issuance_fee_usd,0) + IFNULL(est_vendor_refund_fee_usd,0) + IFNULL(est_vendor_exchange_fee_usd,0) AS est_total_vendor_fees_usd,
    FROM
    (
      SELECT * EXCEPT (
          est_gateway_authorisation_fee,
          est_gateway_authorisation_fee_usd,
          gateway_authorisation_fee,
          gateway_authorisation_fee_usd,
          gateway_blended_fee,
					gateway_blended_fee_usd,
					gateway_refund_fee,
					gateway_refund_fee_usd,
					gateway_void_fee,
					gateway_void_fee_usd,
					gateway_scheme_fixed_fee,
					gateway_scheme_fixed_fee_usd,
					gateway_scheme_variable_fee,
					gateway_scheme_variable_fee_usd,
					gateway_premium_fee,
					gateway_premium_fee_usd
        ),
				0 AS est_gateway_void_fee,
				0 AS est_gateway_void_fee_usd,
        add_on_correction_ratio * est_gateway_authorisation_fee AS est_gateway_authorisation_fee,
        add_on_correction_ratio * est_gateway_authorisation_fee_usd AS est_gateway_authorisation_fee_usd,
				add_on_correction_ratio * gateway_authorisation_fee AS gateway_authorisation_fee,
				add_on_correction_ratio * gateway_authorisation_fee_usd AS gateway_authorisation_fee_usd,
				add_on_correction_ratio * gateway_blended_fee AS gateway_blended_fee,
				add_on_correction_ratio * gateway_blended_fee_usd AS gateway_blended_fee_usd,
				add_on_correction_ratio * gateway_refund_fee AS gateway_refund_fee,
				add_on_correction_ratio * gateway_refund_fee_usd AS gateway_refund_fee_usd,
				add_on_correction_ratio * gateway_void_fee AS gateway_void_fee,
				add_on_correction_ratio * gateway_void_fee_usd AS gateway_void_fee_usd,
				add_on_correction_ratio * gateway_scheme_fixed_fee AS gateway_scheme_fixed_fee,
				add_on_correction_ratio * gateway_scheme_fixed_fee_usd AS gateway_scheme_fixed_fee_usd,
				add_on_correction_ratio * gateway_scheme_variable_fee AS gateway_scheme_variable_fee,
				add_on_correction_ratio * gateway_scheme_variable_fee_usd AS gateway_scheme_variable_fee_usd,
				add_on_correction_ratio * gateway_premium_fee AS gateway_premium_fee,
				add_on_correction_ratio * gateway_premium_fee_usd AS gateway_premium_fee_usd
			FROM
			(
				SELECT event.*,
	        CASE
	          WHEN centralized_est_gateway_blended_fee IS NOT NULL AND centralized_est_gateway_blended_fee_type = 'Percentage'
	            THEN centralized_est_gateway_blended_fee * event.total_price_usd_from_vendor_currency_code / event.exchange_rate_from_currency_code_to_usd
	          ELSE gateway_blended_fees_percentage.est_gateway_blended_decimal * event.total_price_usd_from_vendor_currency_code / event.exchange_rate_from_currency_code_to_usd
	        END AS est_gateway_blended_fee,
					CASE
	          WHEN centralized_est_gateway_blended_fee IS NOT NULL AND centralized_est_gateway_blended_fee_type = 'Percentage'
	            THEN centralized_est_gateway_blended_fee * event.total_price_usd_from_vendor_currency_code
	          ELSE gateway_blended_fees_percentage.est_gateway_blended_decimal * event.total_price_usd_from_vendor_currency_code
	        END AS est_gateway_blended_fee_usd,
					IF(reprocessed.insurance_base_amount IS NULL, 1, 1 - SAFE_DIVIDE((reprocessed.insurance_base_amount + reprocessed.insurance_tax_amount),(reprocessed.insurance_base_amount + reprocessed.insurance_tax_amount + event.total_amount))) AS add_on_correction_ratio,
					CASE
            WHEN centralized_est_gateway_authorization_fee_usd IS NOT NULL THEN centralized_est_gateway_authorization_fee_usd / event.exchange_rate_from_currency_code_to_usd
            ELSE (est_gateway_authorisation_flat_value * exchange_rate_authorisation.amount) / event.exchange_rate_from_currency_code_to_usd
          END AS est_gateway_authorisation_fee,
          CASE
            WHEN centralized_est_gateway_authorization_fee_usd IS NOT NULL THEN centralized_est_gateway_authorization_fee_usd
            ELSE est_gateway_authorisation_flat_value * exchange_rate_authorisation.amount
          END AS est_gateway_authorisation_fee_usd,
          ------- PAYMENT FEE -------
					reprocessed.gateway_scheme_ic_usd,
					reprocessed.gateway_authorisation_fee_usd,
					reprocessed.gateway_blended_fee_usd,
					reprocessed.gateway_refund_fee_usd,
					reprocessed.gateway_void_fee_usd,
					reprocessed.gateway_scheme_fixed_fee_usd,
					reprocessed.gateway_scheme_variable_fee_usd,
					reprocessed.gateway_premium_fee_usd,
          IFNULL(SAFE_DIVIDE(gateway_scheme_ic_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_scheme_ic,
					(IFNULL(event.ticket_historical_count,0) * event.ticket_issuance_fee_usd) / event.exchange_rate_from_currency_code_to_usd  AS est_vendor_issuance_fee, -- 1 USD per ticket for now
					IFNULL(event.ticket_historical_count,0) * event.ticket_issuance_fee_usd AS est_vendor_issuance_fee_usd, -- 1 USD per ticket for now
					(IFNULL(event.ticket_refunded_count,0) * event.ticket_refund_fee_usd) / event.exchange_rate_from_currency_code_to_usd  AS est_vendor_refund_fee, -- 1 USD per ticket for now
		      IFNULL(event.ticket_refunded_count,0) * event.ticket_refund_fee_usd AS est_vendor_refund_fee_usd, -- 1 USD per ticket for now
		      (IFNULL(event.ticket_exchanged_count,0) * event.ticket_exchange_fee_usd) / event.exchange_rate_from_currency_code_to_usd  AS est_vendor_exchange_fee, -- 1 USD per ticket for now
		      IFNULL(event.ticket_exchanged_count,0) * event.ticket_exchange_fee_usd AS est_vendor_exchange_fee_usd, -- 1 USD per ticket for now
		      IFNULL(SAFE_DIVIDE(gateway_authorisation_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_authorisation_fee,
		      IFNULL(SAFE_DIVIDE(gateway_blended_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_blended_fee,
		      IFNULL(SAFE_DIVIDE(gateway_refund_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_refund_fee,
		      IFNULL(SAFE_DIVIDE(gateway_void_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_void_fee,
		      IFNULL(SAFE_DIVIDE(gateway_scheme_fixed_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_scheme_fixed_fee,
		      IFNULL(SAFE_DIVIDE(gateway_scheme_variable_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_scheme_variable_fee,
		      IFNULL(SAFE_DIVIDE(gateway_premium_fee_usd, event.exchange_rate_from_currency_code_to_usd),0) AS gateway_premium_fee,
				FROM
				/* -- temp_event1 --
				session_id
				booking_id
				site_code
				trip_category
				branded_fare_id
				gds_ref
				booking_ref
				ipcc
				payment_ref
				ticket_numbers
				currency_code
				payment_currency_code
				charged_currency_code
				vendor_currency_code
				created_at
				vendor_base_amount
				vendor_tax_amount
				vendor_total_amount
				vendor_total_amount_usd
				total_amount
				total_price
				total_price_usd
				adults_count
				children_count
				infants_count
				exchange_rate_from_currency_code_to_usd
				total_price_usd_from_vendor_currency_code
				markup_amount
				markup_amount_usd
				gds_segment_fee_usd
				vendor_commissions_upfront_backend_usd
				vendor_commissions_iata_usd
				vendor_commissions_plb_usd
        ipcc_currency_code
        pcc
        vendor_id
        vendor
				total_commissions_usd
				ticket_issuance_fee_usd
				ticket_refund_fee_usd
				ticket_exchange_fee_usd
				ticket_exchanged_count
				ticket_refunded_count
				ticket_historical_count
        */
        (/* -- temp_exchange_rates --
effective
base
amount
*/
WITH global_exchange_rates AS (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
)
SELECT
	session_id,
	booking_id,
	site_code,
	trip_category,
	booking_type,
	branded_fare_id,
	gds_ref,
	booking_ref,
	ipcc,
	payment_ref,
	ticket_numbers,
	currency_code,
	payment_currency_code,
	charged_currency_code,
	vendor_currency_code,
	created_at,
	vendor_base_amount,
	vendor_tax_amount,
	vendor_total_amount,
	vendor_total_amount_usd,
	total_price_in_vendor_currency_code as total_amount,
	total_price,
	total_price_usd,
	adults_count,
	children_count,
	infants_count,
	exchange_rate_from_currency_code_to_usd,
	total_price_in_vendor_currency_code * exchange_rate_from_vendor_currency_code_to_usd AS total_price_usd_from_vendor_currency_code,
	total_price_in_vendor_currency_code - vendor_total_amount AS markup_amount,
	(total_price_in_vendor_currency_code - vendor_total_amount) * exchange_rate_from_vendor_currency_code_to_usd AS markup_amount_usd,
	gds_segment_fee_usd,
	vendor_commissions_upfront_backend_usd,
	vendor_commissions_iata_usd,
	vendor_commissions_plb_usd,
  ipcc_currency_code,
  pcc,
  vendor_id,
  vendor,
	----- TOTAL COMMISSIONS -----
  total_commissions_usd,
  ticket_issuance_fee_usd,
	ticket_refund_fee_usd,
	ticket_exchange_fee_usd,
	ticket_exchanged_count,
	ticket_refunded_count,
	ticket_historical_count,
FROM
(
	SELECT
	  online.session_id,
	  online.booking_id,
	  online.site_code,
	  CASE
	    WHEN LOWER(offline.trip_category) = "i" THEN "international"
	    WHEN LOWER(offline.trip_category) = "d" THEN "domestic"
	    ELSE NULL
	  END AS trip_category,
	  offline.booking_type,
	  online.branded_fare_id,
	  offline.gds_ref,
	  offline.booking_ref,
	  offline.ipcc,
	  offline.payment_ref,
	  offline.ticket_numbers,
	  offline.currency_code,
	  offline.payment_currency_code,
	  offline.charged_currency_code,
	  offline.vendor_currency_code,
	  offline.created_at,
	  offline.vendor_base_amount,
	  offline.vendor_tax_amount,
	  offline.vendor_total_amount,
	  offline.vendor_total_amount_usd,
	  offline.total_price,
	  offline.total_price_usd,
	  offline.adults_count,
	  offline.children_count,
	  offline.infants_count,
	  SAFE_CAST(IFNULL(offline.total_price, 0) * IFNULL(SAFE_DIVIDE(exchange_rate.amount, exchange_rate_vendor.amount), 0) AS NUMERIC) AS total_price_in_vendor_currency_code,
		exchange_rate.amount AS exchange_rate_from_currency_code_to_usd,
		exchange_rate_vendor.amount AS exchange_rate_from_vendor_currency_code_to_usd,

		event0.ipcc_currency_code,
		event0.pcc,
		event0.vendor_id,
		event0.vendor,

    event0.total_commissions_usd,
    event0.gds_segment_fee_usd,
    event0.vendor_commissions_upfront_backend_usd,
    event0.vendor_commissions_iata_usd,
    event0.vendor_commissions_plb_usd,

	  event0.ticket_issuance_fee_usd,
		event0.ticket_refund_fee_usd,
		event0.ticket_exchange_fee_usd,
		event0.ticket_exchanged_count,
		event0.ticket_refunded_count,
		event0.ticket_historical_count,
	FROM
	/* -- temp_offline --
	booking_ref
	gds_ref
	ipcc
	payment_ref
	ticket_numbers
	payment_currency_code
	charged_currency_code
	currency_code
	vendor_currency_code
	created_at
	trip_category
	vendor_base_amount
	vendor_tax_amount
	vendor_total_amount
	vendor_total_amount_usd
	total_price
	total_price_usd
	adults_count
	children_count
	infants_count
	*/
	(

SELECT
	booking_ref,
	gds_ref,
	ipcc,
	payment_ref,
	ticket_numbers,
	payment_currency_code,
	charged_currency_code,
	currency_code,
	vendor_currency_code,
	created_at,
	trip_category,
	booking_type,
	vendor_base_amount,
	vendor_tax_amount,
	vendor_total_amount,
	vendor_total_amount_usd,
	total_price,
	total_price_usd,
	adults_count,
	children_count,
	infants_count
FROM
(
	SELECT
	  booking_ref,
	  /*
	  * SOO bookings duplicate rows across segments, so:
	  *   - gds_ref collapses to a comma-separated list of distinct values
	  *   - each passenger type is counted at most once (presence flag, not row sum)
	  * Non-SOO bookings keep the original ANY_VALUE / row-sum semantics.
	  */
	  IF(ANY_VALUE(booking_type) = 'SOO',
	     STRING_AGG(DISTINCT gds_ref, ','),
	     ANY_VALUE(gds_ref)) AS gds_ref,
	  ANY_VALUE(ipcc_integration) AS ipcc,
	  COALESCE(ANY_VALUE(payment_ref), 'N/A') AS payment_ref,
	  STRING_AGG(ticket_numbers, ',') AS ticket_numbers,
	  ANY_VALUE(payment_currency_code) AS payment_currency_code,
	  ANY_VALUE(payment_currency_code) AS charged_currency_code,
	  ANY_VALUE(payment_currency_code) AS currency_code,
	  ANY_VALUE(vendor_currency_code) AS vendor_currency_code,
	  ANY_VALUE(created_at) AS created_at,
	  ANY_VALUE(trip_category) AS trip_category,
	  ANY_VALUE(booking_type) AS booking_type,
	  SUM(IFNULL(vendor_base_amount,0)) AS vendor_base_amount,
	  SUM(IFNULL(vendor_tax_amount,0)) AS vendor_tax_amount,
	  SUM(IFNULL(vendor_total_amount,0)) AS vendor_total_amount,
	  SUM(IFNULL(vendor_total_amount_usd,0)) AS vendor_total_amount_usd,
	  SUM(IFNULL(total_payment_amount,0)) AS total_price, --Total price in payment_currency_code (aka currency_code)
	  SUM(IFNULL(total_price_usd,0)) AS total_price_usd,
	  IF(ANY_VALUE(booking_type) = 'SOO',
	     MAX(IF(PAX='ADT', 1, 0)),
	     SUM(IF(PAX='ADT', 1, 0))) AS adults_count,
	  IF(ANY_VALUE(booking_type) = 'SOO',
	     MAX(IF(PAX='CNN', 1, 0)),
	     SUM(IF(PAX='CNN', 1, 0))) AS children_count,
	  IF(ANY_VALUE(booking_type) = 'SOO',
	     MAX(IF(PAX='INF', 1, 0)),
	     SUM(IF(PAX='INF', 1, 0))) AS infants_count,
	FROM
  (
    /*
    * Converts vendor and payment amounts to USD using exchange rates joined by
    * date (created_at) and currency code.
    */
    SELECT *,
      IFNULL(vendor_total_amount, 0) * IFNULL(vcc_er.amount, 0) AS vendor_total_amount_usd,
      IFNULL(total_payment_amount, 0) * IFNULL(pcc_er.amount, 0) AS total_price_usd,
    FROM
    (
      /*
      * Fills NULL payment_currency_code / vendor_currency_code with the first
      * non-null value found for the same booking_ref (ordered by created_at)
      * so all rows of a booking share consistent currency codes.
      * booking_ref LIKE 'WF%' is pushed down so the window and downstream
      * joins only process WF bookings.
      */
      SELECT
        booking_ref,
        gds_ref,
        ipcc_integration,
        payment_ref,
        ticket_numbers,
        created_at,
        trip_category,
        booking_type,
        PAX,
        vendor_base_amount,
        vendor_tax_amount,
        vendor_total_amount,
        total_payment_amount,
        UPPER(COALESCE(payment_currency_code, FIRST_VALUE(payment_currency_code IGNORE NULLS) OVER (PARTITION BY booking_ref ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) AS payment_currency_code,
        UPPER(COALESCE(vendor_currency_code, FIRST_VALUE(vendor_currency_code IGNORE NULLS) OVER (PARTITION BY booking_ref ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) AS vendor_currency_code
      FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
      WHERE created_at IS NOT NULL
        AND booking_ref LIKE 'WF%'
    )
    LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS pcc_er ON DATE(created_at) = DATE(pcc_er.effective) AND payment_currency_code = pcc_er.base
    LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS vcc_er ON DATE(created_at) = DATE(vcc_er.effective) AND vendor_currency_code = vcc_er.base
  )
	GROUP BY booking_ref
) offline_spreadsheet) AS offline
	
  LEFT JOIN `wego_analytics.flights_bookings` AS online ON offline.booking_ref = online.booking_ref

	/* -- temp_event0 --
	booking_id
	ticket_issuance_fee_usd
	ticket_refund_fee_usd
	ticket_exchange_fee_usd
	ticket_historical_count
	total_commissions_usd
	gds_segment_fee_usd
	vendor_commissions_upfront_backend_usd
	vendor_commissions_iata_usd
	vendor_commissions_plb_usd
	ticket_refunded_count
	ticket_exchanged_count
	ipcc_currency_code
	pcc
	vendor_id
	vendor
	*/
	LEFT JOIN (SELECT
  backend.booking_id,
	backend.ticket_issuance_fee_usd,
	backend.ticket_refund_fee_usd,
	backend.ticket_exchange_fee_usd,
  backend.total_tickets AS ticket_historical_count,
  backend.total_commissions_usd,
  backend.gds_segment_fee_usd,
  backend.vendor_commissions_upfront_backend_usd,
  backend.vendor_commissions_iata_usd,
  backend.vendor_commissions_plb_usd,
  0 AS ticket_refunded_count,
  0 AS ticket_exchanged_count,
  ipcc_currency_code,
	pcc,
	vendor_id,
	vendor
FROM
  --  BACKEND TABLES AS MAIN SOURCE OF TRUTH
  /* -- raw_backend --
  booking_id
  branded_fare_id
  total_tickets
  total_commissions_usd
  gds_segment_fee_usd
  vendor_commissions_upfront_backend_usd
  vendor_commissions_iata_usd
  vendor_commissions_plb_usd
  ticket_issuance_fee_usd
	ticket_refund_fee_usd
	ticket_exchange_fee_usd
	ipcc_currency_code
	pcc
	vendor_id
	vendor
  */
  (-- BACKEND JOIN TO FRONTEND ON BRANDED_FARE_ID
SELECT
	offline_x_online_bookings.booking_id,
	offline_x_online_bookings.branded_fare_id,
	offline_x_online_bookings.total_tickets,
  offline_x_online_bookings.total_commissions_usd,
  offline_x_online_bookings.gds_segment_fee_usd,
  offline_x_online_bookings.vendor_commissions_upfront_backend_usd,
  offline_x_online_bookings.vendor_commissions_iata_usd,
  offline_x_online_bookings.vendor_commissions_plb_usd,
	IFNULL(vendor_fees.ticket_issuance_fee, 0) * exchange_rate_vendor_issuance_fee.amount as ticket_issuance_fee_usd,
	IFNULL(vendor_fees.ticket_refund_fee, 0) * exchange_rate_vendor_refund_fee.amount as ticket_refund_fee_usd,
	IFNULL(vendor_fees.ticket_exchange_fee, 0) * exchange_rate_vendor_exchange_fee.amount as ticket_exchange_fee_usd,
	ipcc_table.ipcc_currency_code,
	pcc_table.pcc,
	vendor_table.vendor_id,
	vendor_table.vendor,
FROM
	/* -- temp_minimum_offline_x_online_bookings --
	booking_id
	branded_fare_id
	itinerary_id
  total_commissions_usd
  gds_segment_fee_usd
  vendor_commissions_upfront_backend_usd
  vendor_commissions_iata_usd
  vendor_commissions_plb_usd
	total_tickets
	gds_ref
	booking_ref
	ipcc
	created_at
	*/
	(SELECT
	online.booking_id,
	online.branded_fare_id,
	online.itinerary_id,
  online.total_commissions_usd,
  online.gds_segment_fee_usd,
  online.vendor_commissions_upfront_backend_usd,
  online.vendor_commissions_iata_usd,
  online.vendor_commissions_plb_usd,
  offline.total_tickets,
	offline.gds_ref,
	offline.booking_ref,
	offline.ipcc,
	offline.created_at
FROM
(
  SELECT
    booking_ref,
    /* SOO bookings carry duplicate rows; collapse gds_ref to a comma-separated distinct list there. */
    IF(ANY_VALUE(booking_type) = 'SOO',
       STRING_AGG(DISTINCT gds_ref, ','),
       ANY_VALUE(gds_ref)) AS gds_ref,
    ANY_VALUE(ipcc_integration) AS ipcc,
    ANY_VALUE(created_at) AS created_at,
    COUNT(1) AS total_tickets
  FROM (
    SELECT booking_ref, gds_ref, ipcc_integration, created_at, booking_type
    FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
  )
  GROUP BY booking_ref
) AS offline
LEFT JOIN `wego_analytics.flights_bookings` AS online ON offline.booking_ref = online.booking_ref) as offline_x_online_bookings

	LEFT JOIN

	/* -- temp_ipccs --
	ipcc
	pcc_id
	vendor_code
	ipcc_currency_code
	*/
	(SELECT
    DISTINCT ipcc as ipcc,
    LAST_VALUE(pcc_id) OVER (PARTITION BY ipcc ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as pcc_id,
    LAST_VALUE(vendor_code) OVER (PARTITION BY ipcc ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as vendor_code,
    LAST_VALUE(currency_code) OVER (PARTITION BY ipcc ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ipcc_currency_code
FROM
    `integrated_bookings_flights.ipccs`) as ipcc_table

	ON ipcc_table.ipcc = offline_x_online_bookings.ipcc

	LEFT JOIN

	/* -- temp_pccs --
	pcc_id
	pcc
	*/
	(SELECT
    DISTINCT id as pcc_id,
    LAST_VALUE(pcc) OVER (
        PARTITION BY id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as pcc
FROM
    `integrated_bookings_flights.pccs`) as pcc_table

	ON ipcc_table.pcc_id = pcc_table.pcc_id

	LEFT JOIN

	/* -- temp_vendors --
	vendor_code
	vendor_id
	vendor
	*/
	(SELECT DISTINCT
		code as vendor_code,
    LAST_VALUE(id) OVER (PARTITION BY code ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as vendor_id,
    LAST_VALUE(name) OVER (PARTITION BY code ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as vendor
FROM
    `integrated_bookings_flights.vendors`) as vendor_table

	ON vendor_table.vendor_code = ipcc_table.vendor_code

	LEFT JOIN

  /* -- temp_vendor_fees --
  vendor_id
  ticket_issuance_currency
  ticket_refund_currency
  ticket_exchange_currency
  ticket_issuance_fee
  ticket_refund_fee
  ticket_exchange_fee
  */
  (SELECT
    vendor_id,
    MAX(
        IF(
            fee_type LIKE '%ticket_issuance%',
            flat_value_currency,
            NULL
        )
    ) as ticket_issuance_currency,
    MAX(
        IF(
            fee_type LIKE '%ticket_refund%',
            flat_value_currency,
            NULL
        )
    ) as ticket_refund_currency,
    MAX(
        IF(
            fee_type LIKE '%ticket_exchange%',
            flat_value_currency,
            NULL
        )
    ) as ticket_exchange_currency,
    MAX(
        IF(fee_type LIKE '%ticket_issuance%', flat_value, 0)
    ) as ticket_issuance_fee,
    MAX(
        IF(fee_type LIKE '%ticket_refund%', flat_value, 0)
    ) as ticket_refund_fee,
    MAX(
        IF(fee_type LIKE '%ticket_exchange%', flat_value, 0)
    ) as ticket_exchange_fee
FROM
    `wego-cloud.integrated_bookings_flights.vendor_fees`
GROUP BY
    1) as vendor_fees

  ON vendor_table.vendor_id = vendor_fees.vendor_id

	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_vendor_issuance_fee
	ON vendor_fees.ticket_issuance_currency = exchange_rate_vendor_issuance_fee.base AND DATE(offline_x_online_bookings.created_at) = DATE(exchange_rate_vendor_issuance_fee.effective)

	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_vendor_refund_fee
	ON vendor_fees.ticket_refund_currency = exchange_rate_vendor_refund_fee.base AND DATE(offline_x_online_bookings.created_at) = DATE(exchange_rate_vendor_refund_fee.effective)

	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_vendor_exchange_fee
	ON vendor_fees.ticket_exchange_currency = exchange_rate_vendor_exchange_fee.base AND DATE(offline_x_online_bookings.created_at) = DATE(exchange_rate_vendor_exchange_fee.effective)) AS backend) AS event0
		ON online.booking_id = event0.booking_id

	LEFT JOIN global_exchange_rates AS exchange_rate_vendor_commissions
		ON 'USD' = exchange_rate_vendor_commissions.base
		AND DATE(offline.created_at) = DATE(exchange_rate_vendor_commissions.effective)

	LEFT JOIN global_exchange_rates AS exchange_rate
		ON offline.currency_code = exchange_rate.base
		AND DATE(offline.created_at) = DATE(exchange_rate.effective)

	LEFT JOIN global_exchange_rates AS exchange_rate_vendor
		ON offline.vendor_currency_code = exchange_rate_vendor.base
		AND DATE(offline.created_at) = DATE(exchange_rate_vendor.effective)
)) AS event

        LEFT JOIN

        -- REPROCESSING OF STATUSES AS THEY MAY CHANGE
        /* -- temp_combined_order_level --
				payment_id
				reconciliation_created_at
				reference
				processing_country
				payout_country
				processing_currency
				payout_currency
				exchange_rate_gateway_processing_payout
				exchange_rate_gateway_usd
				gateway_profile_type_icplusplus
				gateway_profile_type_blended
				gateway_rolling_reserve_processing
				gateway_rolling_reserve_processing_outstanding
				gateway_authorisation_fee_processing
				gateway_blended_fee_processing
				gateway_refund_fee_processing
				gateway_void_fee_processing
				gateway_scheme_fixed_fee_processing
				gateway_scheme_variable_fee_processing
				gateway_premium_fee_processing
				gateway_scheme_ic_processing
				gateway_authorisation_fee_tax_processing
				gateway_blended_fee_tax_processing
				gateway_refund_fee_tax_processing
				gateway_void_fee_tax_processing
				gateway_scheme_fixed_fee_tax_processing
				gateway_scheme_variable_fee_tax_processing
				gateway_premium_fee_tax_processing
				gateway_scheme_ic_tax_processing
				gateway_rolling_reserve_payout
				gateway_rolling_reserve_payout_outstanding
				gateway_authorisation_fee_payout
				gateway_blended_fee_payout
				gateway_refund_fee_payout
				gateway_void_fee_payout
				gateway_scheme_fixed_fee_payout
				gateway_scheme_variable_fee_payout
				gateway_premium_fee_payout
				gateway_scheme_ic_payout
				gateway_authorisation_fee_tax_payout
				gateway_blended_fee_tax_payout
				gateway_refund_fee_tax_payout
				gateway_void_fee_tax_payout
				gateway_scheme_fixed_fee_tax_payout
				gateway_scheme_variable_fee_tax_payout
				gateway_premium_fee_tax_payout
				gateway_scheme_ic_tax_payout
				reconciliation_description
				gateway_rolling_reserve_usd
				gateway_rolling_reserve_outstanding_usd
				gateway_authorisation_fee_usd
				gateway_blended_fee_usd
				gateway_refund_fee_usd
				gateway_void_fee_usd
				gateway_scheme_fixed_fee_usd
				gateway_scheme_variable_fee_usd
				gateway_premium_fee_usd
				gateway_scheme_ic_usd
				est_gateway_currency
				centralized_est_gateway_blended_fee
				centralized_est_gateway_blended_fee_type
				insurance_base_amount
				insurance_tax_amount
        */
        (
SELECT
	reconciliation.payment_id,
	reconciliation.reconciliation_created_at,
	reconciliation.reference,
	reconciliation.processing_country,
	reconciliation.payout_country,
	reconciliation.processing_currency,
	reconciliation.payout_currency,
	reconciliation.exchange_rate_gateway_processing_payout,
	reconciliation.exchange_rate_gateway_usd,
	reconciliation.gateway_profile_type_icplusplus,
	reconciliation.gateway_profile_type_blended,
	reconciliation.gateway_rolling_reserve_processing,
	reconciliation.gateway_rolling_reserve_processing_outstanding,
	reconciliation.gateway_authorisation_fee_processing,
	reconciliation.gateway_blended_fee_processing,
	reconciliation.gateway_refund_fee_processing,
	reconciliation.gateway_void_fee_processing,
	reconciliation.gateway_scheme_fixed_fee_processing,
	reconciliation.gateway_scheme_variable_fee_processing,
	reconciliation.gateway_premium_fee_processing,
	reconciliation.gateway_scheme_ic_processing,
	reconciliation.gateway_authorisation_fee_tax_processing,
	reconciliation.gateway_blended_fee_tax_processing,
	reconciliation.gateway_refund_fee_tax_processing,
	reconciliation.gateway_void_fee_tax_processing,
	reconciliation.gateway_scheme_fixed_fee_tax_processing,
	reconciliation.gateway_scheme_variable_fee_tax_processing,
	reconciliation.gateway_premium_fee_tax_processing,
	reconciliation.gateway_scheme_ic_tax_processing,
	reconciliation.gateway_rolling_reserve_payout,
	reconciliation.gateway_rolling_reserve_payout_outstanding,
	reconciliation.gateway_authorisation_fee_payout,
	reconciliation.gateway_blended_fee_payout,
	reconciliation.gateway_refund_fee_payout,
	reconciliation.gateway_void_fee_payout,
	reconciliation.gateway_scheme_fixed_fee_payout,
	reconciliation.gateway_scheme_variable_fee_payout,
	reconciliation.gateway_premium_fee_payout,
	reconciliation.gateway_scheme_ic_payout,
	reconciliation.gateway_authorisation_fee_tax_payout,
	reconciliation.gateway_blended_fee_tax_payout,
	reconciliation.gateway_refund_fee_tax_payout,
	reconciliation.gateway_void_fee_tax_payout,
	reconciliation.gateway_scheme_fixed_fee_tax_payout,
	reconciliation.gateway_scheme_variable_fee_tax_payout,
	reconciliation.gateway_premium_fee_tax_payout,
	reconciliation.gateway_scheme_ic_tax_payout,
	reconciliation.reconciliation_description,
	reconciliation.gateway_rolling_reserve_usd,
	reconciliation.gateway_rolling_reserve_outstanding_usd,
	reconciliation.gateway_authorisation_fee_usd,
	reconciliation.gateway_blended_fee_usd,
	reconciliation.gateway_refund_fee_usd,
	reconciliation.gateway_void_fee_usd,
	reconciliation.gateway_scheme_fixed_fee_usd,
	reconciliation.gateway_scheme_variable_fee_usd,
	reconciliation.gateway_premium_fee_usd,
	reconciliation.gateway_scheme_ic_usd,
	'AED' as est_gateway_currency,
	COALESCE(
		centralized_payments_gateway_fees.centralized_est_gateway_authorization_fee_usd,
		orchestrator_centralized_payments_gateway_fees.final_partner_centralized_est_gateway_authorization_fee_usd
	) + IFNULL(orchestrator_centralized_payments_gateway_fees.orchestrator_centralized_est_gateway_authorization_fee, 0)  AS centralized_est_gateway_authorization_fee_usd,
	COALESCE(
		centralized_payments_gateway_fees.centralized_est_gateway_blended_fee,
		orchestrator_centralized_payments_gateway_fees.final_partner_centralized_est_gateway_blended_fee
	) AS centralized_est_gateway_blended_fee,
	COALESCE(
	  centralized_payments_gateway_fees.centralized_est_gateway_blended_fee_type,
	  orchestrator_centralized_payments_gateway_fees.final_partner_centralized_est_gateway_blended_fee_type
	) AS centralized_est_gateway_blended_fee_type,
	NULL AS insurance_base_amount,
	NULL AS insurance_tax_amount,
FROM
/* -- temp_reconciliation --
payment_uuid
payment_ref
reconciliation_created_at
reference
processing_country
payout_country
processing_currency
payout_currency
exchange_rate_gateway_processing_payout
exchange_rate_gateway_usd
gateway_profile_type_icplusplus
gateway_profile_type_blended
gateway_rolling_reserve_processing
gateway_rolling_reserve_processing_outstanding
gateway_authorisation_fee_processing
gateway_blended_fee_processing
gateway_refund_fee_processing
gateway_void_fee_processing
gateway_scheme_fixed_fee_processing
gateway_scheme_variable_fee_processing
gateway_premium_fee_processing
gateway_scheme_ic_processing
gateway_authorisation_fee_tax_processing
gateway_blended_fee_tax_processing
gateway_refund_fee_tax_processing
gateway_void_fee_tax_processing
gateway_scheme_fixed_fee_tax_processing
gateway_scheme_variable_fee_tax_processing
gateway_premium_fee_tax_processing
gateway_scheme_ic_tax_processing
gateway_rolling_reserve_payout
gateway_rolling_reserve_payout_outstanding
gateway_authorisation_fee_payout
gateway_blended_fee_payout
gateway_refund_fee_payout
gateway_void_fee_payout
gateway_scheme_fixed_fee_payout
gateway_scheme_variable_fee_payout
gateway_premium_fee_payout
gateway_scheme_ic_payout
gateway_authorisation_fee_tax_payout
gateway_blended_fee_tax_payout
gateway_refund_fee_tax_payout
gateway_void_fee_tax_payout
gateway_scheme_fixed_fee_tax_payout
gateway_scheme_variable_fee_tax_payout
gateway_premium_fee_tax_payout
gateway_scheme_ic_tax_payout
reconciliation_description
gateway_rolling_reserve_usd
gateway_rolling_reserve_outstanding_usd
gateway_authorisation_fee_usd
gateway_blended_fee_usd
gateway_refund_fee_usd
gateway_void_fee_usd
gateway_scheme_fixed_fee_usd
gateway_scheme_variable_fee_usd
gateway_premium_fee_usd
gateway_scheme_ic_usd
*/
(SELECT * FROM (SELECT DISTINCT
	payment_uuid,
	FIRST_VALUE(payment_id IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS payment_id,
	FIRST_VALUE(TIMESTAMP_TRUNC(reconciliation_created_at, SECOND) IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reconciliation_created_at,
	FIRST_VALUE(reference IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reference,
	FIRST_VALUE(processing_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_country,
	FIRST_VALUE(payout_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_country,
	FIRST_VALUE(processing_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_currency,
	FIRST_VALUE(payout_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_currency,
	FIRST_VALUE(exchange_rate_gateway_processing_payout IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as exchange_rate_gateway_processing_payout,
	MAX(exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as exchange_rate_gateway_usd,

	MAX(gateway_profile_type_icplusplus) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_profile_type_icplusplus,
	MAX(gateway_profile_type_blended) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_profile_type_blended,

	SUM(IF(gateway_rolling_reserve_processing<0,ABS(gateway_rolling_reserve_processing),0)) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_processing,
	SUM(-1*gateway_rolling_reserve_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_processing_outstanding,

	SUM(gateway_authorisation_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_authorisation_fee_processing,
	SUM(gateway_blended_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_blended_fee_processing,
	SUM(gateway_refund_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_refund_fee_processing,
	SUM(gateway_void_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_void_fee_processing,
	SUM(gateway_scheme_fixed_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_fixed_fee_processing,
	SUM(gateway_scheme_variable_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_variable_fee_processing,
	SUM(gateway_premium_fee_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_premium_fee_processing,
	SUM(gateway_scheme_ic_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_ic_processing,

	SUM(gateway_authorisation_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_authorisation_fee_tax_processing,
	SUM(gateway_blended_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_blended_fee_tax_processing,
	SUM(gateway_refund_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_refund_fee_tax_processing,
	SUM(gateway_void_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_void_fee_tax_processing,
	SUM(gateway_scheme_fixed_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_fixed_fee_tax_processing,
	SUM(gateway_scheme_variable_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_variable_fee_tax_processing,
	SUM(gateway_premium_fee_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_premium_fee_tax_processing,
	SUM(gateway_scheme_ic_tax_processing) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_ic_tax_processing,

	SUM(IF(gateway_rolling_reserve_payout<0,ABS(gateway_rolling_reserve_payout),0)) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_payout,
	SUM(-1*gateway_rolling_reserve_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_payout_outstanding,

	SUM(gateway_authorisation_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_authorisation_fee_payout,
	SUM(gateway_blended_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_blended_fee_payout,
	SUM(gateway_refund_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_refund_fee_payout,
	SUM(gateway_void_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_void_fee_payout,
	SUM(gateway_scheme_fixed_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_fixed_fee_payout,
	SUM(gateway_scheme_variable_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_variable_fee_payout,
	SUM(gateway_premium_fee_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_premium_fee_payout,
	SUM(gateway_scheme_ic_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_ic_payout,

	SUM(gateway_authorisation_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_authorisation_fee_tax_payout,
	SUM(gateway_blended_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_blended_fee_tax_payout,
	SUM(gateway_refund_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_refund_fee_tax_payout,
	SUM(gateway_void_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_void_fee_tax_payout,
	SUM(gateway_scheme_fixed_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_fixed_fee_tax_payout,
	SUM(gateway_scheme_variable_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_variable_fee_tax_payout,
	SUM(gateway_premium_fee_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_premium_fee_tax_payout,
	SUM(gateway_scheme_ic_tax_payout) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_ic_tax_payout,

	FIRST_VALUE(reconciliation_description IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reconciliation_description,

	SUM(IF(gateway_rolling_reserve_processing<0,ABS(gateway_rolling_reserve_processing),0) * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_usd,
	SUM((-1*gateway_rolling_reserve_processing) * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_rolling_reserve_outstanding_usd,
	SUM(gateway_authorisation_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_authorisation_fee_usd,
	SUM(gateway_blended_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_blended_fee_usd,
	SUM(gateway_refund_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_refund_fee_usd,
	SUM(gateway_void_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_void_fee_usd,
	SUM(gateway_scheme_fixed_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_fixed_fee_usd,
	SUM(gateway_scheme_variable_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_variable_fee_usd,
	SUM(gateway_premium_fee_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_premium_fee_usd,
	SUM(gateway_scheme_ic_processing * exchange_rate_gateway_reconciliation.amount) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as gateway_scheme_ic_usd
FROM
(
  SELECT
    CASE WHEN REGEXP_CONTAINS(reference,r'(^(WF)+)') THEN reference
    WHEN REGEXP_CONTAINS(reference, r'(^(F\.)+)') THEN REGEXP_EXTRACT(reference, r'\.(.*)')
    WHEN REGEXP_CONTAINS(reference,r'(^(p)+)') THEN reference
    ELSE payment_id END AS payment_uuid,
    payment_id,
    reference,
    TIMESTAMP(processed_on, '-8:00') as reconciliation_created_at, -- Need to leave it as -8:00 NOT +8:00
    issuer_country as processing_country,
    entity_country as payout_country,
    processing_currency as processing_currency,
    holding_currency as payout_currency,
    IF(holding_currency = processing_currency, 1, CAST(REGEXP_EXTRACT(breakdown_type, '@(.*)') AS NUMERIC)) as exchange_rate_gateway_processing_payout, -- If same currency, then exchange rate = 1, else extract
    IF(breakdown_type LIKE  '%Scheme%' OR breakdown_type LIKE '%Premium Variable%',1,0) as gateway_profile_type_icplusplus,
    IF(breakdown_type LIKE  '%Blended Fee%',1,0) as gateway_profile_type_blended,
    IF(breakdown_type LIKE '%RR%',processing_currency_amount,0) as gateway_rolling_reserve_processing,
    ABS(IF(breakdown_type LIKE '%Authorization Fix Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_authorisation_fee_processing,
    ABS(IF(breakdown_type LIKE '%Blended Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_blended_fee_processing,
    ABS(IF(breakdown_type LIKE '%Refund Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_refund_fee_processing,
    ABS(IF(breakdown_type LIKE '%Void Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_void_fee_processing,
    ABS(IF(breakdown_type LIKE '%Scheme Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_fixed_fee_processing,
    ABS(IF(breakdown_type LIKE '%Scheme Variable Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_variable_fee_processing,
    ABS(IF(breakdown_type LIKE '%Premium Variable Fee%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_premium_fee_processing,
    ABS(IF(breakdown_type LIKE '%SchemeIC%' AND breakdown_type NOT LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_ic_processing,
    ABS(IF(breakdown_type LIKE '%Authorization Fix Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_authorisation_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Blended Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_blended_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Refund Fixed Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_refund_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Void Fixed Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_void_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Scheme Fixed Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_fixed_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Scheme Variable Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_variable_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%Premium Variable Fee%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_premium_fee_tax_processing,
    ABS(IF(breakdown_type LIKE '%SchemeIC%' AND breakdown_type LIKE '%Tax%',processing_currency_amount,0)) as gateway_scheme_ic_tax_processing,
    ABS(IF(breakdown_type LIKE '%Capture%',processing_currency_amount,0)) as gateway_captured_processing,
    IF(breakdown_type LIKE '%RR%',holding_currency_amount,0) as gateway_rolling_reserve_payout,
    ABS(IF(breakdown_type LIKE '%Authorization Fix Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_authorisation_fee_payout,
    ABS(IF(breakdown_type LIKE '%Blended Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_blended_fee_payout,
    ABS(IF(breakdown_type LIKE '%Refund Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_refund_fee_payout,
    ABS(IF(breakdown_type LIKE '%Void Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_void_fee_payout,
    ABS(IF(breakdown_type LIKE '%Scheme Fixed Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_fixed_fee_payout,
    ABS(IF(breakdown_type LIKE '%Scheme Variable Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_variable_fee_payout,
    ABS(IF(breakdown_type LIKE '%Premium Variable Fee%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_premium_fee_payout,
    ABS(IF(breakdown_type LIKE '%SchemeIC%' AND breakdown_type NOT LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_ic_payout,
    ABS(IF(breakdown_type LIKE '%Authorization Fix Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_authorisation_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Blended Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_blended_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Refund Fixed Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_refund_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Void Fixed Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_void_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Scheme Fixed Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_fixed_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Scheme Variable Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_variable_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%Premium Variable Fee%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_premium_fee_tax_payout,
    ABS(IF(breakdown_type LIKE '%SchemeIC%' AND breakdown_type LIKE '%Tax%',holding_currency_amount,0)) as gateway_scheme_ic_tax_payout,
    ABS(IF(breakdown_type LIKE '%Capture%',holding_currency_amount,0)) as gateway_captured_payout,
    response_description as reconciliation_description
  FROM `wego-cloud.integrated_bookings_payments.checkout*`
) AS co
LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_gateway_reconciliation
ON co.processing_currency = exchange_rate_gateway_reconciliation.base AND DATE(co.reconciliation_created_at) = DATE(exchange_rate_gateway_reconciliation.effective))) as reconciliation

LEFT JOIN
/* -- temp_centralized_payments_gateway_fees --
payment_ref
centralized_payment_id
order_ref
payment_gateway
payment_gateway_ref
cashback_created_at
cashback_campaign
cashback_currency_code
cashback_amount
centralized_payment_created_at
centralized_payment_currency_code
fee_network
route_id
payment_method_id
payment_method
client_id
partner_id
start_datetime_routes
end_datetime_routes
partner_account
payment_method_centralized
centralized_est_gateway_authorization_fee_id
centralized_est_gateway_authorization_fee_currency
centralized_est_gateway_authorization_fee
centralized_est_gateway_authorization_fee_type
start_datetime_auth
end_datetime_auth
centralized_est_gateway_blended_fee_id
centralized_est_gateway_blended_fee
centralized_est_gateway_blended_fee_type
start_datetime_blended
end_datetime_blended
payment_currency
partner_account_id
partner_account_payment_method_id
centralized_est_gateway_authorization_fee_usd
card_payment_type
scheme
card_category
issuer
*/
(SELECT
	*
FROM
(
	SELECT
		centralized_payment.payment_ref,
		centralized_payment.source_id,
		centralized_payment.centralized_payment_id,
		centralized_payment.order_ref,
		centralized_payment.payment_gateway,
		centralized_payment.payment_gateway_ref,
		centralized_payment.cashback_created_at,
		centralized_payment.cashback_campaign,
		centralized_payment.cashback_currency_code,
		centralized_payment.cashback_amount,
		centralized_payment.centralized_payment_created_at,
		centralized_payment.centralized_payment_currency_code,
		centralized_payment.fee_network,
		centralized_payment.route_id,
		centralized_payment.payment_method_id,
		centralized_payment.payment_method,
		centralized_payment.client_id,
		centralized_payment.partner_id,
		centralized_gateway_fees.start_datetime_routes,
		centralized_gateway_fees.end_datetime_routes,
		centralized_gateway_fees.partner_account,
		centralized_gateway_fees.payment_method_centralized,
		centralized_gateway_fees.centralized_est_gateway_authorization_fee_id,
		centralized_gateway_fees.centralized_est_gateway_authorization_fee_currency,
		centralized_gateway_fees.centralized_est_gateway_authorization_fee,
		centralized_gateway_fees.centralized_est_gateway_authorization_fee_type,
		centralized_gateway_fees.start_datetime_auth,
		centralized_gateway_fees.end_datetime_auth,
		centralized_gateway_fees.centralized_est_gateway_blended_fee_id,
		centralized_gateway_fees.centralized_est_gateway_blended_fee,
		centralized_gateway_fees.centralized_est_gateway_blended_fee_type,
		centralized_gateway_fees.start_datetime_blended,
		centralized_gateway_fees.end_datetime_blended,
		centralized_gateway_fees.payment_currency,
		centralized_gateway_fees.partner_account_id,
		centralized_gateway_fees.partner_account_payment_method_id,
		centralized_gateway_fees.centralized_est_gateway_authorization_fee * exchange_rate_centralized_gateway_authorisation_fees.amount AS centralized_est_gateway_authorization_fee_usd,
	FROM
	/* -- temp_centralized_payment --
	amount
	approved
	payment_transaction_status
	source_id
	centralized_payment_id
	payment_ref
	order_ref
	payment_gateway
	payment_gateway_ref
	cashback_created_at       # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_campaign         # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_currency_code    # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_amount           # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	centralized_payment_created_at
	centralized_payment_currency_code
	fee_network
	route_id
	payment_method_id
	payment_method
	client_id
	partner_id
	status                    # Will not be available if using_orchestrator_payment_gateway is false
	final_partner_id          # Will not be available if using_orchestrator_payment_gateway is false
	issuer_country
	*/
	(SELECT * EXCEPT(payment_status_selection)
FROM
(
	SELECT
		payments.amount,
		payments.approved,
		payments.payment_transaction_status,
		payments.source_id,
		payments.centralized_payment_id,
		payments.payment_ref,
		payments.order_ref,
		partners.payment_gateway,
		CASE
			WHEN partners.payment_gateway = 'Checkout' THEN payments.order_ref
			WHEN partners.payment_gateway = 'MyFatoorah' THEN COALESCE(
				JSON_EXTRACT_SCALAR(payments.partner_meta, '$.invoice_reference')
			)
		END AS payment_gateway_ref,
		
			
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payment_actions.cashback_created_at, NULL) as cashback_created_at,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payments.cashback_ref, NULL) as cashback_campaign,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payments.centralized_payment_currency_code, NULL) as cashback_currency_code,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payment_actions.cashback_amount, NULL) as cashback_amount,
			
		
		payments.centralized_payment_created_at,
		payments.centralized_payment_currency_code,
		payments.fee_network,
		payments.route_id,
		payments.payment_method_id,
		payment_methods.payment_method,
		payments.client_id,
		payments.partner_id,
		
		ps.issuer_country,
		IF(DENSE_RANK() OVER (PARTITION BY order_ref ORDER BY max_created_at) = 1 AND centralized_payment_created_at = max_created_at AND payments.centralized_payment_id = max_centralized_payment_id,1,0) as payment_status_selection,
	FROM
		/* -- temp_payments_payments --
		amount
		approved
		payment_transaction_status
		source_id
		centralized_payment_id
		payment_ref
		partner_id
		order_ref
		partner_meta
		cashback_ref        	    # Will be NULL if post_process is true
		centralized_payment_currency_code
		route_id
		payment_method_id
		client_id
		centralized_payment_created_at
		fee_network
		status                    # Will not be available if using_orchestrator_payment_gateway is false
		final_partner_id          # For every Juspay transaction, there will be a final_partner_id which represents
															# the payment gateway to which the transaction was routed to via Juspay
															# Will not be available if using_orchestrator_payment_gateway is false
		max_created_at
		max_centralized_payment_id
		*/
		(SELECT
	amount,
	approved,
	status as payment_transaction_status,
	source_id,
	id as centralized_payment_id,
	payment_ref,
	partner_id,
	order_ref,
	partner_meta,
	
	    cashback_ref,
	
	currency_code as centralized_payment_currency_code,
	route_id,
	payment_method_id,
	client_id,
	created_at as centralized_payment_created_at,
	fee_network,
	MAX(created_at) OVER (PARTITION BY order_ref, amount) as max_created_at,
	MAX(id) OVER (PARTITION BY order_ref, amount) as max_centralized_payment_id,
FROM `wego-cloud.payments.payments`
WHERE final_partner_id IS NULL
) as payments
		LEFT JOIN
		/* -- wego-cloud.payments.payment_methods --
		payment_method_id
		payment_method_centralized
		payment_method
		*/
		(SELECT
  id as payment_method_id,
  name as payment_method_centralized,
  name as payment_method,
FROM `wego-cloud.payments.payment_methods`) as payment_methods
		ON payments.payment_method_id = payment_methods.payment_method_id
		LEFT JOIN
		/* -- wego-cloud.payments.partners --
		partner_id
		payment_gateway
		*/
		(
	SELECT
		id as partner_id,
		ANY_VALUE(name) as payment_gateway,
	FROM `wego-cloud.payments.partners`
	GROUP BY 1
) as partners
		ON payments.partner_id = partners.partner_id
		
			LEFT JOIN
			
			/* -- temp_payment_actions --
			centralized_payment_id
			cashback_created_at
			cashback_amount
			*/
			(SELECT
    payment_id as centralized_payment_id,
    TIMESTAMP_TRUNC(
        MAX(TIMESTAMP(DATETIME(created_at, '+8:00'))),
        SECOND
    ) as cashback_created_at,
    SUM(CAST(amount AS FLOAT64)) as cashback_amount
FROM
    `wego-cloud.payments.payment_actions`
WHERE
    approved = true
    AND type = 'Refund'
GROUP BY
    1) as payment_actions
			ON payments.centralized_payment_id = payment_actions.centralized_payment_id
		

		/* --payment_sources--
		id
		issuer_country
		*/
		LEFT JOIN `payments.payment_sources` ps ON payments.source_id = ps.id
)
WHERE (payment_status_selection = 1 AND order_ref LIKE '%WH%') OR order_ref NOT LIKE '%WH%') as centralized_payment

	LEFT JOIN

	-- Gateway Fees Estimates
	/* -- temp_centralized_gateway_fees --
	route_id
	client_id
	start_datetime_routes
	end_datetime_routes
	partner_account
	payment_method_centralized
	payment_method
	centralized_est_gateway_authorization_fee_id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	centralized_est_gateway_authorization_fee_type
	start_datetime_auth
	end_datetime_auth
	centralized_est_gateway_blended_fee_id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	start_datetime_blended
	end_datetime_blended
	payment_currency
	fee_network
	issuer_country
	partner_account_id
	payment_method_id
	partner_account_payment_method_id
	*/
	(SELECT
	routes.route_id,
	routes.client_id,
	routes.start_datetime_routes,
	routes.end_datetime_routes,
	partners.partner_account,
	payment_methods.payment_method_centralized,
	payment_methods.payment_method,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_id,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_currency,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_type,
	fees_partner_account_payment_method_fees.start_datetime_auth,
	fees_partner_account_payment_method_fees.end_datetime_auth,
	fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee_id,
	fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee,
	fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee_type,
	fees_partner_account_payment_method_fees.start_datetime_blended,
	fees_partner_account_payment_method_fees.end_datetime_blended,
	fees_partner_account_payment_method_fees.payment_currency,
	fees_partner_account_payment_method_fees.fee_network,
	fees_partner_account_payment_method_fees.issuer_country,
	partners.partner_account_id,
	routes.payment_method_id,
	papm.partner_account_payment_method_id,
FROM
	/* -- wego-cloud.payments.routes --
	route_id
	client_id
	payment_method_id
	partner_account_id
	start_datetime_routes
	end_datetime_routes
	*/
	(SELECT
  id as route_id,
  client_id,
  payment_method_id.element as payment_method_id,
  partner_account_id,
  start_datetime as start_datetime_routes,
  end_datetime as end_datetime_routes
FROM `wego-cloud.payments.routes`,
UNNEST(payment_method_ids.list) as payment_method_id) as routes

	LEFT JOIN
	/* -- wego-cloud.payments.partner_account_payment_methods --
	partner_account_payment_method_id
	payment_method_id
	partner_account_id
	*/
	(SELECT
  id as partner_account_payment_method_id,
  payment_method_id,
  partner_account_id
FROM `wego-cloud.payments.partner_account_payment_methods`) as papm
	ON routes.payment_method_id = papm.payment_method_id AND routes.partner_account_id = papm.partner_account_id

	LEFT JOIN
	/* -- wego-cloud.payments.partner_accounts --
	partner_account_id
	partner_account
	*/
	(
	SELECT
	  id as partner_account_id,
	  account as partner_account
	FROM `wego-cloud.payments.partner_accounts`
) as partners
	ON routes.partner_account_id = partners.partner_account_id

	LEFT JOIN
	/* -- wego-cloud.payments.payment_methods --
	payment_method_id
	payment_method_centralized
	payment_method
	*/
	(SELECT
  id as payment_method_id,
  name as payment_method_centralized,
  name as payment_method,
FROM `wego-cloud.payments.payment_methods`) as payment_methods
	ON routes.payment_method_id = payment_methods.payment_method_id

	LEFT JOIN
	/* -- temp_fees_partner_account_payment_method_fees_2 --
	partner_account_payment_method_id
	centralized_est_gateway_authorization_fee_id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	centralized_est_gateway_authorization_fee_type
	start_datetime_auth
	end_datetime_auth
	centralized_est_gateway_blended_fee_id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	start_datetime_blended
	end_datetime_blended
	payment_currency
	fee_network
	issuer_country
	partner_id 	                              # Will not be available if using_orchestrator_payment_gateway is false
	*/
	(/**/
SELECT
    a.partner_account_payment_method_id,
    a.centralized_est_gateway_authorization_fee_id,
    a.centralized_est_gateway_authorization_fee_currency,
    a.centralized_est_gateway_authorization_fee,
    a.centralized_est_gateway_authorization_fee_type,
    a.start_datetime_auth,
    a.end_datetime_auth,
    b.centralized_est_gateway_blended_fee_id,
    b.centralized_est_gateway_blended_fee,
    b.centralized_est_gateway_blended_fee_type,
    b.start_datetime_blended,
    b.end_datetime_blended,
    a.payment_currency,
    a.fee_network,
    a.issuer_country,
    
FROM
    /* -- temp_fees_partner_account_payment_method_fees_0 --
    centralized_est_gateway_authorization_fee_currency
    centralized_est_gateway_authorization_fee
    centralized_est_gateway_authorization_fee_type
    start_datetime_auth
    end_datetime_auth
    payment_currency
    fee_network
    issuer_country
		centralized_est_gateway_authorization_fee_id (aka fee_id)
		partner_account_payment_method_id
		partner_id     # Will not be available if using_orchestrator_payment_gateway is false
    */
    (SELECT
	x.* EXCEPT (id),
	x.id as centralized_est_gateway_authorization_fee_id,
	y.partner_account_payment_method_id,
	
FROM
	/* -- payments.fees --
	id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	start_datetime_auth
	end_datetime_auth
	payment_currency
	fee_network
	issuer_country
	*/
	(
	  SELECT
	    id,
	    currency as centralized_est_gateway_authorization_fee_currency,
	    value as centralized_est_gateway_authorization_fee,
	    value_type as centralized_est_gateway_authorization_fee_type,
	    start_datetime as start_datetime_auth,
	    end_datetime as end_datetime_auth,
	    payment_currency.element as payment_currency,
	    fee_network.element as fee_network,
	    issuer_country.element as issuer_country
	  FROM
	    `payments.fees`,
	    UNNEST(charge_currencies.list) as payment_currency, /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1739154393308349*/
	    UNNEST(fee_networks.list) as fee_network,
	    UNNEST(IFNULL(issuer_countries.list, [STRUCT("*" AS element)])) AS issuer_country /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1741256728108069?thread_ts=1739154393.308349&cid=C03276WBLNR*/
	  WHERE type = 'GatewayTransactionFee' AND deleted_at IS NULL
	) as x
LEFT JOIN `payments.partner_account_payment_method_fees` as y
ON x.id = y.fee_id
WHERE y.deleted_at IS NULL) as a

    CROSS JOIN
    /* -- temp_fees_partner_account_payment_method_fees_1 --
		centralized_est_gateway_blended_fee
		centralized_est_gateway_blended_fee_type
		start_datetime_blended
		end_datetime_blended
		payment_currency
		fee_network
		issuer_country
    centralized_est_gateway_blended_fee_id (aka fee_id)
		partner_account_payment_method_id
		partner_id    # Will not be available if using_orchestrator_payment_gateway is false
    */
    (SELECT
	x.* EXCEPT (id),
	x.id as centralized_est_gateway_blended_fee_id,
	y.partner_account_payment_method_id,
	
FROM
	/* -- payments.fees --
	id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	start_datetime_blended
	end_datetime_blended
	payment_currency
	fee_network
	issuer_country
	*/
	(
		SELECT
			id,
			value as centralized_est_gateway_blended_fee,
			value_type as centralized_est_gateway_blended_fee_type,
			start_datetime as start_datetime_blended,
			end_datetime as end_datetime_blended,
			payment_currency.element as payment_currency,
			fee_network.element as fee_network,
			issuer_country.element as issuer_country
		FROM
			`payments.fees`,
			UNNEST(charge_currencies.list) as payment_currency, /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1739154393308349*/
			UNNEST(fee_networks.list) as fee_network,
			UNNEST(IFNULL(issuer_countries.list, [STRUCT("*" AS element)])) AS issuer_country /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1741256728108069?thread_ts=1739154393.308349&cid=C03276WBLNR*/
		WHERE type = 'CardProcessingFee' AND deleted_at IS NULL
	) as x
LEFT JOIN `payments.partner_account_payment_method_fees` as y
ON x.id = y.fee_id
WHERE y.deleted_at IS NULL) as b

WHERE a.payment_currency = b.payment_currency
		AND a.fee_network = b.fee_network
			AND a.partner_account_payment_method_id = b.partner_account_payment_method_id
				AND a.issuer_country = b.issuer_country
			) as fees_partner_account_payment_method_fees
	ON papm.partner_account_payment_method_id = fees_partner_account_payment_method_fees.partner_account_payment_method_id) as centralized_gateway_fees

	ON centralized_payment.route_id = centralized_gateway_fees.route_id
	  AND centralized_payment.payment_method_id = centralized_gateway_fees.payment_method_id
	    AND centralized_payment.centralized_payment_created_at BETWEEN centralized_gateway_fees.start_datetime_auth AND centralized_gateway_fees.end_datetime_auth
	      AND centralized_payment.centralized_payment_created_at BETWEEN centralized_gateway_fees.start_datetime_blended AND centralized_gateway_fees.end_datetime_blended
	        AND centralized_payment.centralized_payment_created_at BETWEEN centralized_gateway_fees.start_datetime_routes AND centralized_gateway_fees.end_datetime_routes
	          AND (centralized_payment.centralized_payment_currency_code = centralized_gateway_fees.payment_currency OR centralized_gateway_fees.payment_currency = '*')
	            AND centralized_payment.fee_network = centralized_gateway_fees.fee_network
	              AND (centralized_payment.issuer_country = centralized_gateway_fees.issuer_country OR centralized_gateway_fees.issuer_country = '*')

	
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_centralized_gateway_authorisation_fees ON centralized_gateway_fees.centralized_est_gateway_authorization_fee_currency = exchange_rate_centralized_gateway_authorisation_fees.base AND DATE(centralized_payment.centralized_payment_created_at) = exchange_rate_centralized_gateway_authorisation_fees.effective
	
	QUALIFY ROW_NUMBER () OVER (PARTITION BY centralized_payment.centralized_payment_id ORDER BY end_datetime_blended DESC) = 1
) AS centralized_payment_gateway_fees
LEFT JOIN
/* -- temp_payments_payment_sources --
source_id
card_payment_type
scheme
card_category
issuer
*/
(SELECT
	id as source_id,
	card_type as card_payment_type,
	scheme,
	card_category,
	issuer
FROM `wego-cloud.payments.payment_sources`) AS payment_sources USING (source_id)
WHERE order_ref LIKE '%WF%'
QUALIFY ROW_NUMBER () OVER (PARTITION BY order_ref ORDER BY centralized_payment_created_at DESC) = 1) as centralized_payments_gateway_fees
/* temp_centralized_payments_gateway_fees will contain fees for payment records that have NULL final_partner_id */
ON centralized_payments_gateway_fees.payment_ref = reconciliation.payment_id

/**/
LEFT JOIN
/* -- temp_orchestrator_centralized_payments_gateway_fees --
payment_ref
order_ref
partner_account_id
partner_account
card_payment_type
scheme
card_category
issuer
cashback_created_at
cashback_campaign
cashback_currency_code
cashback_amount
centralized_payment_created_at
payment_gateway
payment_gateway_ref
payment_transaction_status
centralized_payment_currency_code
payment_method
fee_network
payment_method_id
route_id
client_id
partner_id
status
final_partner_id
orchestrator_centralized_est_gateway_authorization_fee_id
orchestrator_centralized_est_gateway_authorization_fee
orchestrator_centralized_est_gateway_authorization_fee_type
orchestrator_centralized_est_gateway_authorization_fee_currency
final_partner_centralized_est_gateway_authorization_fee_id
final_partner_centralized_est_gateway_authorization_fee
final_partner_centralized_est_gateway_authorization_fee_type
final_partner_centralized_est_gateway_authorization_fee_currency
final_partner_centralized_est_gateway_blended_fee_id
final_partner_centralized_est_gateway_blended_fee
final_partner_centralized_est_gateway_blended_fee_type
final_partner_centralized_est_gateway_authorization_fee_usd
*/
(/**/

SELECT
	centralized_payment.payment_ref,
	centralized_payment.order_ref,
	routes.partner_account_id,
	partners.partner_account,
	payment_sources.card_payment_type,
	payment_sources.scheme,
	payment_sources.card_category,
	payment_sources.issuer,
	centralized_payment.cashback_created_at,
	centralized_payment.cashback_campaign,
	centralized_payment.cashback_currency_code,
	centralized_payment.cashback_amount,
	centralized_payment.centralized_payment_created_at,
	centralized_payment.payment_gateway,
	centralized_payment.payment_gateway_ref,
	centralized_payment.payment_transaction_status,
	/**/
	centralized_payment.centralized_payment_currency_code,
	centralized_payment.payment_method,
	centralized_payment.fee_network,
	centralized_payment.payment_method_id,
	centralized_payment.route_id,
	centralized_payment.client_id,
	centralized_payment.partner_id,
	centralized_payment.status,
	centralized_payment.final_partner_id,

	fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee_id AS orchestrator_centralized_est_gateway_authorization_fee_id,

	CASE
		WHEN centralized_payment.status IN ('Declined', 'Voided') THEN 0.0 /*Declined, Voided: https://wego.slack.com/archives/C03276WBLNR/p1735627768563429?thread_ts=1733729196.514589&cid=C03276WBLNR*/
		ELSE fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee
	END AS orchestrator_centralized_est_gateway_authorization_fee,

	fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee_type AS orchestrator_centralized_est_gateway_authorization_fee_type,
	fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee_currency AS orchestrator_centralized_est_gateway_authorization_fee_currency,
	/**/

	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_id AS final_partner_centralized_est_gateway_authorization_fee_id,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee AS final_partner_centralized_est_gateway_authorization_fee,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_type AS final_partner_centralized_est_gateway_authorization_fee_type,
	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_currency AS final_partner_centralized_est_gateway_authorization_fee_currency,

	fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee_id AS final_partner_centralized_est_gateway_blended_fee_id,

	CASE
		WHEN centralized_payment.status IN ('Declined', 'Voided') THEN 0.0 /*Declined, Voided: https://wego.slack.com/archives/C03276WBLNR/p1735627768563429?thread_ts=1733729196.514589&cid=C03276WBLNR*/
		ELSE fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee
	END AS final_partner_centralized_est_gateway_blended_fee,

	fees_partner_account_payment_method_fees.centralized_est_gateway_blended_fee_type AS final_partner_centralized_est_gateway_blended_fee_type,

	/**/

	fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee * exchange_rate_centralized_gateway_authorisation_fees.amount AS final_partner_centralized_est_gateway_authorization_fee_usd,
FROM
	/* -- temp_centralized_payment --
	amount
	approved
	payment_transaction_status
	source_id
	centralized_payment_id
	payment_ref
	order_ref
	payment_gateway
	payment_gateway_ref
	cashback_created_at               # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_campaign                 # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_currency_code            # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	cashback_amount                   # Will be NULL if post_process is true or not be available if analytics_type is "wegopro_bow"
	centralized_payment_created_at
	centralized_payment_currency_code
	fee_network
	route_id
	payment_method_id
	payment_method
	client_id
	partner_id
	status                           # Will not be available if using_orchestrator_payment_gateway is false
	final_partner_id                 # Will not be available if using_orchestrator_payment_gateway is false
	issuer_country
	*/
	(SELECT * EXCEPT(payment_status_selection)
FROM
(
	SELECT
		payments.amount,
		payments.approved,
		payments.payment_transaction_status,
		payments.source_id,
		payments.centralized_payment_id,
		payments.payment_ref,
		payments.order_ref,
		partners.payment_gateway,
		CASE
			WHEN partners.payment_gateway = 'Checkout' THEN payments.order_ref
			WHEN partners.payment_gateway = 'MyFatoorah' THEN COALESCE(
				JSON_EXTRACT_SCALAR(payments.partner_meta, '$.invoice_reference')
			)
		END AS payment_gateway_ref,
		
			
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payment_actions.cashback_created_at, NULL) as cashback_created_at,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payments.cashback_ref, NULL) as cashback_campaign,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payments.centralized_payment_currency_code, NULL) as cashback_currency_code,
				IF(payments.cashback_ref != '' AND payments.cashback_ref IS NOT NULL, payment_actions.cashback_amount, NULL) as cashback_amount,
			
		
		payments.centralized_payment_created_at,
		payments.centralized_payment_currency_code,
		payments.fee_network,
		payments.route_id,
		payments.payment_method_id,
		payment_methods.payment_method,
		payments.client_id,
		payments.partner_id,
		
			payments.status,
			payments.final_partner_id,
		
		ps.issuer_country,
		IF(DENSE_RANK() OVER (PARTITION BY order_ref ORDER BY max_created_at) = 1 AND centralized_payment_created_at = max_created_at AND payments.centralized_payment_id = max_centralized_payment_id,1,0) as payment_status_selection,
	FROM
		/* -- temp_payments_payments --
		amount
		approved
		payment_transaction_status
		source_id
		centralized_payment_id
		payment_ref
		partner_id
		order_ref
		partner_meta
		cashback_ref        	    # Will be NULL if post_process is true
		centralized_payment_currency_code
		route_id
		payment_method_id
		client_id
		centralized_payment_created_at
		fee_network
		status                    # Will not be available if using_orchestrator_payment_gateway is false
		final_partner_id          # For every Juspay transaction, there will be a final_partner_id which represents
															# the payment gateway to which the transaction was routed to via Juspay
															# Will not be available if using_orchestrator_payment_gateway is false
		max_created_at
		max_centralized_payment_id
		*/
		(SELECT
	amount,
	approved,
	status as payment_transaction_status,
	source_id,
	id as centralized_payment_id,
	payment_ref,
	partner_id,
	order_ref,
	partner_meta,
	
	    cashback_ref,
	
	currency_code as centralized_payment_currency_code,
	route_id,
	payment_method_id,
	client_id,
	created_at as centralized_payment_created_at,
	fee_network,
	  status,
	  final_partner_id,
	
	MAX(created_at) OVER (PARTITION BY order_ref, amount) as max_created_at,
	MAX(id) OVER (PARTITION BY order_ref, amount) as max_centralized_payment_id,
FROM `wego-cloud.payments.payments`
/* Determines the payment gateway where "orchestrator" redirects the payment transaction */
WHERE final_partner_id IS NOT NULL
) as payments
		LEFT JOIN
		/* -- wego-cloud.payments.payment_methods --
		payment_method_id
		payment_method_centralized
		payment_method
		*/
		(SELECT
  id as payment_method_id,
  name as payment_method_centralized,
  name as payment_method,
FROM `wego-cloud.payments.payment_methods`) as payment_methods
		ON payments.payment_method_id = payment_methods.payment_method_id
		LEFT JOIN
		/* -- wego-cloud.payments.partners --
		partner_id
		payment_gateway
		*/
		(
	SELECT
		id as partner_id,
		ANY_VALUE(name) as payment_gateway,
	FROM `wego-cloud.payments.partners`
	GROUP BY 1
) as partners
		ON payments.partner_id = partners.partner_id
		
			LEFT JOIN
			
			/* -- temp_payment_actions --
			centralized_payment_id
			cashback_created_at
			cashback_amount
			*/
			(SELECT
    payment_id as centralized_payment_id,
    TIMESTAMP_TRUNC(
        MAX(TIMESTAMP(DATETIME(created_at, '+8:00'))),
        SECOND
    ) as cashback_created_at,
    SUM(CAST(amount AS FLOAT64)) as cashback_amount
FROM
    `wego-cloud.payments.payment_actions`
WHERE
    approved = true
    AND type = 'Refund'
GROUP BY
    1) as payment_actions
			ON payments.centralized_payment_id = payment_actions.centralized_payment_id
		

		/* --payment_sources--
		id
		issuer_country
		*/
		LEFT JOIN `payments.payment_sources` ps ON payments.source_id = ps.id
)
WHERE (payment_status_selection = 1 AND order_ref LIKE '%WH%') OR order_ref NOT LIKE '%WH%') as centralized_payment

	LEFT JOIN
	/* -- temp_fees_partner_account_payment_method_fees_0 --
  centralized_est_gateway_authorization_fee_currency
  centralized_est_gateway_authorization_fee
  centralized_est_gateway_authorization_fee_type
  start_datetime_auth
  end_datetime_auth
  payment_currency
  fee_network
	centralized_est_gateway_authorization_fee_id (aka fee_id)
	partner_account_payment_method_id
	partner_id
	*/
	(SELECT
	x.* EXCEPT (id),
	x.id as centralized_est_gateway_authorization_fee_id,
	y.partner_account_payment_method_id,
	
		y.partner_id,
	
FROM
	/* -- payments.fees --
	id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	start_datetime_auth
	end_datetime_auth
	payment_currency
	fee_network
	issuer_country
	*/
	(
	  SELECT
	    id,
	    currency as centralized_est_gateway_authorization_fee_currency,
	    value as centralized_est_gateway_authorization_fee,
	    value_type as centralized_est_gateway_authorization_fee_type,
	    start_datetime as start_datetime_auth,
	    end_datetime as end_datetime_auth,
	    payment_currency.element as payment_currency,
	    fee_network.element as fee_network,
	    issuer_country.element as issuer_country
	  FROM
	    `payments.fees`,
	    UNNEST(charge_currencies.list) as payment_currency, /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1739154393308349*/
	    UNNEST(fee_networks.list) as fee_network,
	    UNNEST(IFNULL(issuer_countries.list, [STRUCT("*" AS element)])) AS issuer_country /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1741256728108069?thread_ts=1739154393.308349&cid=C03276WBLNR*/
	  WHERE type = 'GatewayTransactionFee' AND deleted_at IS NULL
	) as x
LEFT JOIN `payments.partner_account_payment_method_fees` as y
ON x.id = y.fee_id
WHERE y.deleted_at IS NULL) as fees_partner_account_payment_method_fees_0
	ON centralized_payment.centralized_payment_created_at BETWEEN fees_partner_account_payment_method_fees_0.start_datetime_auth AND fees_partner_account_payment_method_fees_0.end_datetime_auth
		AND (centralized_payment.centralized_payment_currency_code = fees_partner_account_payment_method_fees_0.payment_currency OR fees_partner_account_payment_method_fees_0.payment_currency = "*")
		  AND (centralized_payment.fee_network = fees_partner_account_payment_method_fees_0.fee_network OR fees_partner_account_payment_method_fees_0.fee_network = "*")
		    AND centralized_payment.partner_id = fees_partner_account_payment_method_fees_0.partner_id
		      AND centralized_payment.status IN ('Authorized', 'Declined', 'Captured', 'Refunded', 'PartiallyRefunded', 'Voided') /*Declined, Voided: https://wego.slack.com/archives/C03276WBLNR/p1735627768563429?thread_ts=1733729196.514589&cid=C03276WBLNR*/

	/* THIS PART IS TO CALCULATE THE FINAL PARTNER FEES */
	LEFT JOIN
	/* -- wego-cloud.payments.routes --
	route_id
	client_id
	payment_method_id
	partner_account_id
	start_datetime_routes
	end_datetime_routes
	*/
	(SELECT
  id as route_id,
  client_id,
  payment_method_id.element as payment_method_id,
  partner_account_id,
  start_datetime as start_datetime_routes,
  end_datetime as end_datetime_routes
FROM `wego-cloud.payments.routes`,
UNNEST(payment_method_ids.list) as payment_method_id) as routes
	ON centralized_payment.route_id = routes.route_id
		AND centralized_payment.payment_method_id = routes.payment_method_id
			AND centralized_payment.centralized_payment_created_at BETWEEN routes.start_datetime_routes AND routes.end_datetime_routes

	LEFT JOIN
	/* -- wego-cloud.payments.partner_account_payment_methods --
	partner_account_payment_method_id
	payment_method_id
	partner_account_id
	*/
	(SELECT
  id as partner_account_payment_method_id,
  payment_method_id,
  partner_account_id
FROM `wego-cloud.payments.partner_account_payment_methods`) as papm
	ON routes.payment_method_id = papm.payment_method_id AND routes.partner_account_id = papm.partner_account_id

	LEFT JOIN
	/* -- wego-cloud.payments.partner_accounts --
	partner_account_id
	partner_account
	*/
	(
	SELECT
	  id as partner_account_id,
	  account as partner_account
	FROM `wego-cloud.payments.partner_accounts`
) as partners
	ON routes.partner_account_id = partners.partner_account_id

	LEFT JOIN
	/* -- temp_payments_payment_sources --
	source_id
	card_payment_type
	scheme
	card_category
	issuer
	*/
	(SELECT
	id as source_id,
	card_type as card_payment_type,
	scheme,
	card_category,
	issuer
FROM `wego-cloud.payments.payment_sources`) AS payment_sources USING (source_id)

	LEFT JOIN
	/* -- temp_fees_partner_account_payment_method_fees_2 --
	partner_account_payment_method_id
	centralized_est_gateway_authorization_fee_id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	centralized_est_gateway_authorization_fee_type
	start_datetime_auth
	end_datetime_auth
	centralized_est_gateway_blended_fee_id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	start_datetime_blended
	end_datetime_blended
	payment_currency
	fee_network
	issuer_country
	partner_id            # Will not be available if using_orchestrator_payment_gateway is false
	*/
	(/**/
SELECT
    a.partner_account_payment_method_id,
    a.centralized_est_gateway_authorization_fee_id,
    a.centralized_est_gateway_authorization_fee_currency,
    a.centralized_est_gateway_authorization_fee,
    a.centralized_est_gateway_authorization_fee_type,
    a.start_datetime_auth,
    a.end_datetime_auth,
    b.centralized_est_gateway_blended_fee_id,
    b.centralized_est_gateway_blended_fee,
    b.centralized_est_gateway_blended_fee_type,
    b.start_datetime_blended,
    b.end_datetime_blended,
    a.payment_currency,
    a.fee_network,
    a.issuer_country,
    
      a.partner_id,
    
FROM
    /* -- temp_fees_partner_account_payment_method_fees_0 --
    centralized_est_gateway_authorization_fee_currency
    centralized_est_gateway_authorization_fee
    centralized_est_gateway_authorization_fee_type
    start_datetime_auth
    end_datetime_auth
    payment_currency
    fee_network
    issuer_country
		centralized_est_gateway_authorization_fee_id (aka fee_id)
		partner_account_payment_method_id
		partner_id     # Will not be available if using_orchestrator_payment_gateway is false
    */
    (SELECT
	x.* EXCEPT (id),
	x.id as centralized_est_gateway_authorization_fee_id,
	y.partner_account_payment_method_id,
	
		y.partner_id,
	
FROM
	/* -- payments.fees --
	id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	start_datetime_auth
	end_datetime_auth
	payment_currency
	fee_network
	issuer_country
	*/
	(
	  SELECT
	    id,
	    currency as centralized_est_gateway_authorization_fee_currency,
	    value as centralized_est_gateway_authorization_fee,
	    value_type as centralized_est_gateway_authorization_fee_type,
	    start_datetime as start_datetime_auth,
	    end_datetime as end_datetime_auth,
	    payment_currency.element as payment_currency,
	    fee_network.element as fee_network,
	    issuer_country.element as issuer_country
	  FROM
	    `payments.fees`,
	    UNNEST(charge_currencies.list) as payment_currency, /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1739154393308349*/
	    UNNEST(fee_networks.list) as fee_network,
	    UNNEST(IFNULL(issuer_countries.list, [STRUCT("*" AS element)])) AS issuer_country /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1741256728108069?thread_ts=1739154393.308349&cid=C03276WBLNR*/
	  WHERE type = 'GatewayTransactionFee' AND deleted_at IS NULL
	) as x
LEFT JOIN `payments.partner_account_payment_method_fees` as y
ON x.id = y.fee_id
WHERE y.deleted_at IS NULL) as a

    CROSS JOIN
    /* -- temp_fees_partner_account_payment_method_fees_1 --
		centralized_est_gateway_blended_fee
		centralized_est_gateway_blended_fee_type
		start_datetime_blended
		end_datetime_blended
		payment_currency
		fee_network
		issuer_country
    centralized_est_gateway_blended_fee_id (aka fee_id)
		partner_account_payment_method_id
		partner_id    # Will not be available if using_orchestrator_payment_gateway is false
    */
    (SELECT
	x.* EXCEPT (id),
	x.id as centralized_est_gateway_blended_fee_id,
	y.partner_account_payment_method_id,
	
		y.partner_id,
	
FROM
	/* -- payments.fees --
	id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	start_datetime_blended
	end_datetime_blended
	payment_currency
	fee_network
	issuer_country
	*/
	(
		SELECT
			id,
			value as centralized_est_gateway_blended_fee,
			value_type as centralized_est_gateway_blended_fee_type,
			start_datetime as start_datetime_blended,
			end_datetime as end_datetime_blended,
			payment_currency.element as payment_currency,
			fee_network.element as fee_network,
			issuer_country.element as issuer_country
		FROM
			`payments.fees`,
			UNNEST(charge_currencies.list) as payment_currency, /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1739154393308349*/
			UNNEST(fee_networks.list) as fee_network,
			UNNEST(IFNULL(issuer_countries.list, [STRUCT("*" AS element)])) AS issuer_country /*Slack Ref: https://wego.slack.com/archives/C03276WBLNR/p1741256728108069?thread_ts=1739154393.308349&cid=C03276WBLNR*/
		WHERE type = 'CardProcessingFee' AND deleted_at IS NULL
	) as x
LEFT JOIN `payments.partner_account_payment_method_fees` as y
ON x.id = y.fee_id
WHERE y.deleted_at IS NULL) as b

WHERE a.payment_currency = b.payment_currency
		AND a.fee_network = b.fee_network
			AND a.partner_account_payment_method_id = b.partner_account_payment_method_id
				AND a.issuer_country = b.issuer_country
			
	      AND a.partner_id = b.partner_id
	    ) as fees_partner_account_payment_method_fees
	ON papm.partner_account_payment_method_id = fees_partner_account_payment_method_fees.partner_account_payment_method_id
		AND centralized_payment.centralized_payment_created_at BETWEEN fees_partner_account_payment_method_fees.start_datetime_auth AND fees_partner_account_payment_method_fees.end_datetime_auth
			AND centralized_payment.centralized_payment_created_at BETWEEN fees_partner_account_payment_method_fees.start_datetime_blended AND fees_partner_account_payment_method_fees.end_datetime_blended
				AND (centralized_payment.centralized_payment_currency_code = fees_partner_account_payment_method_fees.payment_currency OR fees_partner_account_payment_method_fees.payment_currency = "*")
				  AND centralized_payment.fee_network = fees_partner_account_payment_method_fees.fee_network
				    AND centralized_payment.final_partner_id = fees_partner_account_payment_method_fees.partner_id
				      AND (centralized_payment.issuer_country = fees_partner_account_payment_method_fees.issuer_country OR fees_partner_account_payment_method_fees.issuer_country = '*')

	
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_centralized_gateway_authorisation_fees ON fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_currency = exchange_rate_centralized_gateway_authorisation_fees.base AND DATE(centralized_payment.centralized_payment_created_at) = exchange_rate_centralized_gateway_authorisation_fees.effective
	

	WHERE fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee_id IS NOT NULL
) as orchestrator_centralized_payments_gateway_fees
/* temp_orchestrator_centralized_payments_gateway_fees will contain fees for payment records that have final_partner_id */
ON orchestrator_centralized_payments_gateway_fees.payment_ref = reconciliation.payment_id

WHERE FORMAT_DATE("%Y%m%d", DATE(reconciliation.reconciliation_created_at)) IN (SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)) AS reprocessed
          ON event.payment_ref = reprocessed.payment_id

        LEFT JOIN

        /* -- temp_payment_gateway_fees_0 --
        est_gateway_blended_currencies
        start_time
        end_time
        est_gateway_blended_decimal
        */
        (SELECT
    user_charged_currencies as est_gateway_blended_currencies,
    TIMESTAMP(DATETIME(start_time, '+8:00')) as start_time,
    TIMESTAMP(DATETIME(end_time, '+8:00')) as end_time,
    MAX(fee) as est_gateway_blended_decimal
FROM
    `wego-cloud.integrated_bookings_flights.payment_gateway_fees`,
    UNNEST(user_charged_currencies) as user_charged_currencies
WHERE
    service = 'checkout'
    AND fee_type = 'card_fee'
GROUP BY
    1,
    2,
    3) AS gateway_blended_fees_percentage
	        ON gateway_blended_fees_percentage.est_gateway_blended_currencies = event.charged_currency_code
	        AND TIMESTAMP(event.created_at) >= gateway_blended_fees_percentage.start_time
	        AND TIMESTAMP(event.created_at) <= gateway_blended_fees_percentage.end_time

        LEFT JOIN

        /* -- temp_payment_gateway_fees --
        est_gateway_authorisation_user_charged_currencies
        est_gateway_authorisation_currencies
        start_time
        end_time
        est_gateway_authorisation_flat_value
        */
        (SELECT
    user_charged_currencies as est_gateway_authorisation_user_charged_currencies,
    flat_value_currency as est_gateway_authorisation_currencies,
    TIMESTAMP(DATETIME(start_time, '+8:00')) as start_time,
    TIMESTAMP(DATETIME(end_time, '+8:00')) as end_time,
    MAX(flat_value) as est_gateway_authorisation_flat_value
FROM
    `wego-cloud.integrated_bookings_flights.payment_gateway_fees`,
    UNNEST(user_charged_currencies) as user_charged_currencies
WHERE
    service = 'checkout'
    AND fee_type = 'gateway_fee'
GROUP BY
    1,
    2,
    3,
    4) AS gateway_authorisation_fees_percentage
          ON gateway_authorisation_fees_percentage.est_gateway_authorisation_user_charged_currencies = event.charged_currency_code
          AND TIMESTAMP(event.created_at) >= gateway_authorisation_fees_percentage.start_time
          AND TIMESTAMP(event.created_at) <= gateway_authorisation_fees_percentage.end_time

        LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_gateway
          ON reprocessed.est_gateway_currency = exchange_rate_gateway.base
          AND DATE(reprocessed.reconciliation_created_at) = DATE(exchange_rate_gateway.effective)
        LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS exchange_rate_authorisation
          ON gateway_authorisation_fees_percentage.est_gateway_authorisation_currencies = exchange_rate_authorisation.base
          AND DATE(event.created_at) = DATE(exchange_rate_authorisation.effective)
			)
    )
	)
)) AS temp_event2
	
	/*Start of joining exchange rate for Netsuite integration*/
	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS erSAR
		ON erSAR.base = 'SAR' AND DATE(erSAR.effective) = DATE(temp_event2.created_at)

	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS erEGP
		ON erEGP.base = 'EGP' AND DATE(erEGP.effective) = DATE(temp_event2.created_at)

	LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE _TABLE_SUFFIX IN
/* --bow_flights_offline_bookings--
	formatted_created_date
*/

	(SELECT DISTINCT
  FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
FROM `wego-cloud.wego_analytics.flight_bookings_offline_sheet`
WHERE created_at IS NOT NULL)
) AS erPKR
		ON erPKR.base = 'PKR' AND DATE(erPKR.effective) = DATE(temp_event2.created_at)
	/*End of joining exchange rate for Netsuite integration*/
{% endraw %}
