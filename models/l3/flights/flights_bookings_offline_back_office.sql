{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Offline flights bookings - Back Office dataset
-- Destination: wego_analytics.flights_bookings_offline_back_office  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('analytics', 'exchange_rates') }}
-- depends_on: {{ source('back_office', 'bookings') }}
-- depends_on: {{ source('back_office', 'itineraries') }}
-- depends_on: {{ source('back_office', 'legs') }}
-- depends_on: {{ source('back_office', 'passengers') }}
-- depends_on: {{ source('back_office', 'payments') }}
-- depends_on: {{ source('back_office', 'prices') }}
-- depends_on: {{ source('integrated_bookings_flights', 'ipccs') }}
-- depends_on: {{ source('integrated_bookings_flights', 'payment_sources') }}
-- depends_on: {{ source('integrated_bookings_flights', 'pccs') }}
-- depends_on: {{ source('integrated_bookings_flights', 'vendors') }}
-- depends_on: {{ source('integrated_bookings_payments', 'checkout') }}
-- depends_on: {{ source('integrated_bookings_payments', 'payments') }}
-- depends_on: {{ source('payments', 'fees') }}
-- depends_on: {{ source('payments', 'partner_account_payment_method_fees') }}
-- depends_on: {{ source('payments', 'partner_account_payment_methods') }}
-- depends_on: {{ source('payments', 'partner_accounts') }}
-- depends_on: {{ source('payments', 'partners') }}
-- depends_on: {{ source('payments', 'payment_actions') }}
-- depends_on: {{ source('payments', 'payment_methods') }}
-- depends_on: {{ source('payments', 'payment_sources') }}
-- depends_on: {{ source('payments', 'payments') }}
-- depends_on: {{ source('payments', 'routes') }}
{% raw %}
SELECT
	*,
	total_amount_usd - vendor_total_amount_usd AS markup_amount_usd,
FROM
(
	SELECT
		b.id as booking_id,
		b.booking_ref,
		b.parent_booking_ref, /*This booking ref will match with booking_ref in the wego_analytics.flights_bookings table*/
		b.booking_mode, /*The booking mode is either directly with sabre or wego*/
		b.trip_type,
		b.cabin_type,
		b.adults_count,
		b.children_count,
		b.infants_count,
		b.adults_count + b.children_count + b.infants_count AS passengers_count,
		b.booking_status,
		b.site_code, /*Only SG for VAT*/
		b.created_at,
		i.id as itinerary_id,
		i.gds,
		i.ipcc,
		i.gds_ref,
		i.ticket_status,
		i.validating_carrier_code,
		i.itinerary_status,
		i.integration_type,
		tn.ticket_numbers,
		legs.id as leg_id,
		legs.scd_dep_date_time,
		legs.departure_airport_code,
		legs.arrival_airport_code,
		legs.scd_arr_date_time,
		legs.status as leg_status,
		p.vendor_base_amount,
		p.vendor_tax_amount,
		p.vendor_total_amount,
		p.vendor_total_amount * vendor_exchange_rates.amount AS vendor_total_amount_usd,
		p.base_amount,
		p.tax_amount,
		p.total_amount,
		p.total_amount * user_exchange_rates.amount AS total_amount_usd,
		p.currency_code as vendor_currency_code,
		p.user_currency_code as currency_code,
		combined_order_level.order_id,
		combined_order_level.payment_created_at,
		combined_order_level.payment_id,
		combined_order_level.payment_ref_id,
		combined_order_level.payment_status,
		combined_order_level.payment_method,
		combined_order_level.card_type,
		combined_order_level.card_category,
		combined_order_level.card_scheme,
		combined_order_level.card_issuer,
		combined_order_level.card_issuer_country_code,
		combined_order_level.reconciliation_created_at,
		combined_order_level.reference,
		combined_order_level.processing_country,
		combined_order_level.payout_country,
		combined_order_level.processing_currency,
		combined_order_level.payout_currency,
		combined_order_level.exchange_rate_gateway_processing_payout,
		combined_order_level.exchange_rate_gateway_usd,
		combined_order_level.gateway_profile_type,
		combined_order_level.gateway_rolling_reserve_processing,
		combined_order_level.gateway_rolling_reserve_processing_outstanding,
		combined_order_level.gateway_authorisation_fee_processing,
		combined_order_level.gateway_blended_fee_processing,
		combined_order_level.gateway_refund_fee_processing,
		combined_order_level.gateway_void_fee_processing,
		combined_order_level.gateway_scheme_fixed_fee_processing,
		combined_order_level.gateway_scheme_variable_fee_processing,
		combined_order_level.gateway_premium_fee_processing,
		combined_order_level.gateway_scheme_ic_processing,
		combined_order_level.gateway_authorisation_fee_tax_processing,
		combined_order_level.gateway_blended_fee_tax_processing,
		combined_order_level.gateway_refund_fee_tax_processing,
		combined_order_level.gateway_void_fee_tax_processing,
		combined_order_level.gateway_scheme_fixed_fee_tax_processing,
		combined_order_level.gateway_scheme_variable_fee_tax_processing,
		combined_order_level.gateway_premium_fee_tax_processing,
		combined_order_level.gateway_scheme_ic_tax_processing,
		combined_order_level.gateway_rolling_reserve_payout,
		combined_order_level.gateway_rolling_reserve_payout_outstanding,
		combined_order_level.gateway_authorisation_fee_payout,
		combined_order_level.gateway_blended_fee_payout,
		combined_order_level.gateway_refund_fee_payout,
		combined_order_level.gateway_void_fee_payout,
		combined_order_level.gateway_scheme_fixed_fee_payout,
		combined_order_level.gateway_scheme_variable_fee_payout,
		combined_order_level.gateway_premium_fee_payout,
		combined_order_level.gateway_scheme_ic_payout,
		combined_order_level.gateway_authorisation_fee_tax_payout,
		combined_order_level.gateway_blended_fee_tax_payout,
		combined_order_level.gateway_refund_fee_tax_payout,
		combined_order_level.gateway_void_fee_tax_payout,
		combined_order_level.gateway_scheme_fixed_fee_tax_payout,
		combined_order_level.gateway_scheme_variable_fee_tax_payout,
		combined_order_level.gateway_premium_fee_tax_payout,
		combined_order_level.gateway_scheme_ic_tax_payout,
		combined_order_level.gateway_rolling_reserve_usd,
		combined_order_level.gateway_rolling_reserve_outstanding_usd,
		combined_order_level.gateway_authorisation_fee_usd,
		combined_order_level.gateway_blended_fee_usd,
		combined_order_level.gateway_refund_fee_usd,
		combined_order_level.gateway_void_fee_usd,
		combined_order_level.gateway_scheme_fixed_fee_usd,
		combined_order_level.gateway_scheme_variable_fee_usd,
		combined_order_level.gateway_premium_fee_usd,
		combined_order_level.gateway_scheme_ic_usd,
		combined_order_level.reconciliation_description,
		combined_order_level.route_id,
		combined_order_level.partner_account_id,
		combined_order_level.partner_account,
		combined_order_level.payment_method_id,
		combined_order_level.payment_method_centralized,
		combined_order_level.fee_network,
		combined_order_level.centralized_payment_currency_code,
		combined_order_level.centralized_est_gateway_authorization_fee_id,
		combined_order_level.centralized_est_gateway_authorization_fee_currency,
		combined_order_level.centralized_est_gateway_authorization_fee,
		combined_order_level.centralized_est_gateway_authorization_fee_usd,
		combined_order_level.centralized_est_gateway_blended_fee_id,
		combined_order_level.centralized_est_gateway_blended_fee,
		combined_order_level.centralized_est_gateway_blended_fee_type,
		combined_order_level.orchestrator_centralized_est_gateway_authorization_fee_id,
		combined_order_level.orchestrator_centralized_est_gateway_authorization_fee,
		combined_order_level.orchestrator_centralized_est_gateway_authorization_fee_type,
		combined_order_level.orchestrator_centralized_est_gateway_authorization_fee_currency,
		combined_order_level.partner_id,
		combined_order_level.final_partner_id,
		combined_order_level.payment_gateway,
		combined_order_level.payment_gateway_breakdown_old,
		combined_order_level.payment_gateway_breakdown,
		combined_order_level.cashback_created_at,
		combined_order_level.cashback_campaign,
		combined_order_level.cashback_currency_code,
		combined_order_level.cashback_amount,
		combined_order_level.payment_currency_code,
		combined_order_level.payment_fee,
		combined_order_level.payment_fee_usd,
		ipcc_table.ipcc_currency_code,
		pcc_table.pcc,
		vendor_table.vendor_id,
		vendor_table.vendor,
		/*Start of generating a currency conversion using the payment currency code for Netsuite integration*/
		CASE
		  WHEN combined_order_level.payment_currency_code = 'SAR' THEN erSAR.amount
			WHEN combined_order_level.payment_currency_code = 'AED' THEN erAED.amount
			WHEN combined_order_level.payment_currency_code = 'EGP' THEN erEGP.amount
			ELSE 1.0
		END AS exchange_rate_to_usd_from_payment_currency_code_netsuite,

		CASE
		  WHEN combined_order_level.payment_currency_code = 'SAR' THEN 'SAR'
			WHEN combined_order_level.payment_currency_code = 'AED' THEN 'AED'
			WHEN combined_order_level.payment_currency_code = 'EGP' THEN 'EGP'
			ELSE 'USD'
		END AS payment_currency_code_netsuite,
		/*End of generating a currency conversion using the payment currency code for Netsuite integration*/
	FROM `wego-cloud.back_office.bookings` b
	/*-- temp_ticket_numbers --
	booking_id
	ticket_numbers
	*/
	LEFT JOIN (SELECT
  booking_id,
  STRING_AGG(ticket_number, ',') AS ticket_numbers
FROM
  wego-cloud.back_office.passengers
GROUP BY
  1) tn ON b.id = tn.booking_id
	LEFT JOIN `wego-cloud.back_office.itineraries` i ON b.id = i.booking_id
	LEFT JOIN `wego-cloud.back_office.legs` legs ON i.id = legs.itinerary_id
	LEFT JOIN `wego-cloud.back_office.prices` p ON i.id = p.itinerary_id

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

	ON ipcc_table.ipcc = i.ipcc

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

	/*-- temp_combined_order_level --
	order_id
	booking_id
	payment_created_at
	payment_id
	payment_ref_id
	payment_status
	payment_method
	card_type
	card_category
	card_scheme
	card_issuer
	card_issuer_country_code
	reconciliation_created_at
	reference
	processing_country
	payout_country
	processing_currency
	payout_currency
	exchange_rate_gateway_processing_payout
	exchange_rate_gateway_usd
	gateway_profile_type
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
	reconciliation_description
	route_id
	partner_account_id
	partner_account
	payment_method_id
	payment_method_centralized
	fee_network
	centralized_payment_currency_code
	centralized_est_gateway_authorization_fee_id
	centralized_est_gateway_authorization_fee_currency
	centralized_est_gateway_authorization_fee
	centralized_est_gateway_authorization_fee_usd
	centralized_est_gateway_blended_fee_id
	centralized_est_gateway_blended_fee
	centralized_est_gateway_blended_fee_type
	orchestrator_centralized_est_gateway_authorization_fee_id
	orchestrator_centralized_est_gateway_authorization_fee
	orchestrator_centralized_est_gateway_authorization_fee_type
	orchestrator_centralized_est_gateway_authorization_fee_currency
	partner_id
	final_partner_id
	payment_gateway
	payment_gateway_breakdown_old
	payment_gateway_breakdown
	cashback_created_at
	cashback_campaign
	cashback_currency_code
	cashback_amount
	payment_currency_code
	payment_fee
	payment_fee_usd
	*/
	LEFT JOIN (

SELECT
	order_id,
	MAX(booking_id) AS booking_id,
	-- Take status only of actual payment attempt, values are all summed across payment attempts.
	-- PAYMENTS
	MAX(payment_created_at)                                    AS payment_created_at,
	MAX(IF(payment_actual_attempt = 1, payment_id, NULL))      AS payment_id,

	MAX(IF(payment_actual_attempt = 1, payment_uuid, NULL))    AS payment_ref_id,
	MAX(IF(payment_actual_attempt = 1, payment_status, NULL))  AS payment_status,
	MAX(IF(payment_actual_attempt = 1, payment_method, NULL))  AS payment_method,
	-- CARD TYPE
	MAX(IF(payment_actual_attempt = 1, card_type, NULL))                AS card_type,
	MAX(IF(payment_actual_attempt = 1, card_category, NULL))            AS card_category,
	MAX(IF(payment_actual_attempt = 1, card_scheme, NULL))              AS card_scheme,
	MAX(IF(payment_actual_attempt = 1, card_issuer, NULL))              AS card_issuer,
	MAX(IF(payment_actual_attempt = 1, card_issuer_country_code, NULL)) AS card_issuer_country_code,

	-- RECONCILIATION
	MAX(reconciliation_created_at)               AS reconciliation_created_at,
	STRING_AGG(reference)                        AS reference,
	MAX(processing_country)                      AS processing_country,
	MAX(payout_country)                          AS payout_country,
	MAX(processing_currency)                     AS processing_currency,
	MAX(payout_currency)                         AS payout_currency,
	MAX(exchange_rate_gateway_processing_payout) AS exchange_rate_gateway_processing_payout,
	MAX(exchange_rate_gateway_usd)               AS exchange_rate_gateway_usd,


	CASE
	  WHEN MAX(gateway_profile_type_icplusplus) = 1 AND MAX(gateway_profile_type_blended) = 1 THEN 'Blended,IC++'
	  WHEN MAX(gateway_profile_type_icplusplus) = 1 AND MAX(gateway_profile_type_blended) = 0 THEN 'IC++'
	  WHEN MAX(gateway_profile_type_icplusplus) = 0 AND MAX(gateway_profile_type_blended) = 1 THEN 'Blended'
	  ELSE NULL
	END AS gateway_profile_type,


	SUM(gateway_rolling_reserve_processing)             AS gateway_rolling_reserve_processing,
	SUM(gateway_rolling_reserve_processing_outstanding) AS gateway_rolling_reserve_processing_outstanding,

	SUM(gateway_authorisation_fee_processing)           AS gateway_authorisation_fee_processing,
	SUM(gateway_blended_fee_processing)                 AS gateway_blended_fee_processing,
	SUM(gateway_refund_fee_processing)                  AS gateway_refund_fee_processing,
	SUM(gateway_void_fee_processing)                    AS gateway_void_fee_processing,
	SUM(gateway_scheme_fixed_fee_processing)            AS gateway_scheme_fixed_fee_processing,
	SUM(gateway_scheme_variable_fee_processing)         AS gateway_scheme_variable_fee_processing,
	SUM(gateway_premium_fee_processing)                 AS gateway_premium_fee_processing,
	SUM(gateway_scheme_ic_processing)                   AS gateway_scheme_ic_processing,

	SUM(gateway_authorisation_fee_tax_processing)       AS gateway_authorisation_fee_tax_processing,
	SUM(gateway_blended_fee_tax_processing)             AS gateway_blended_fee_tax_processing,
	SUM(gateway_refund_fee_tax_processing)              AS gateway_refund_fee_tax_processing,
	SUM(gateway_void_fee_tax_processing)                AS gateway_void_fee_tax_processing,
	SUM(gateway_scheme_fixed_fee_tax_processing)        AS gateway_scheme_fixed_fee_tax_processing,
	SUM(gateway_scheme_variable_fee_tax_processing)     AS gateway_scheme_variable_fee_tax_processing,
	SUM(gateway_premium_fee_tax_processing)             AS gateway_premium_fee_tax_processing,
	SUM(gateway_scheme_ic_tax_processing)               AS gateway_scheme_ic_tax_processing,

	SUM(gateway_rolling_reserve_payout)                 AS gateway_rolling_reserve_payout,
	SUM(gateway_rolling_reserve_payout_outstanding)     AS gateway_rolling_reserve_payout_outstanding,

	SUM(gateway_authorisation_fee_payout)               AS gateway_authorisation_fee_payout,
	SUM(gateway_blended_fee_payout)                     AS gateway_blended_fee_payout,
	SUM(gateway_refund_fee_payout)                      AS gateway_refund_fee_payout,
	SUM(gateway_void_fee_payout)                        AS gateway_void_fee_payout,
	SUM(gateway_scheme_fixed_fee_payout)                AS gateway_scheme_fixed_fee_payout,
	SUM(gateway_scheme_variable_fee_payout)             AS gateway_scheme_variable_fee_payout,
	SUM(gateway_premium_fee_payout)                     AS gateway_premium_fee_payout,
	SUM(gateway_scheme_ic_payout)                       AS gateway_scheme_ic_payout,

	SUM(gateway_authorisation_fee_tax_payout)     AS gateway_authorisation_fee_tax_payout,
	SUM(gateway_blended_fee_tax_payout)           AS gateway_blended_fee_tax_payout,
	SUM(gateway_refund_fee_tax_payout)            AS gateway_refund_fee_tax_payout,
	SUM(gateway_void_fee_tax_payout)              AS gateway_void_fee_tax_payout,
	SUM(gateway_scheme_fixed_fee_tax_payout)      AS gateway_scheme_fixed_fee_tax_payout,
	SUM(gateway_scheme_variable_fee_tax_payout)   AS gateway_scheme_variable_fee_tax_payout,
	SUM(gateway_premium_fee_tax_payout)           AS gateway_premium_fee_tax_payout,
	SUM(gateway_scheme_ic_tax_payout)             AS gateway_scheme_ic_tax_payout,

	SUM(gateway_rolling_reserve_usd)             AS gateway_rolling_reserve_usd,
	SUM(gateway_rolling_reserve_outstanding_usd) AS gateway_rolling_reserve_outstanding_usd,
	SUM(gateway_authorisation_fee_usd)           AS gateway_authorisation_fee_usd,
	SUM(gateway_blended_fee_usd)                 AS gateway_blended_fee_usd,
	SUM(gateway_refund_fee_usd)                  AS gateway_refund_fee_usd,
	SUM(gateway_void_fee_usd)                    AS gateway_void_fee_usd,
	SUM(gateway_scheme_fixed_fee_usd)            AS gateway_scheme_fixed_fee_usd,
	SUM(gateway_scheme_variable_fee_usd)         AS gateway_scheme_variable_fee_usd,
	SUM(gateway_premium_fee_usd)                 AS gateway_premium_fee_usd,
	SUM(gateway_scheme_ic_usd)                   AS gateway_scheme_ic_usd,

	MAX(reconciliation_description) as reconciliation_description,
	-- PAYMENT GATEWAY ESTIMATES
	MAX(IF(payment_actual_attempt = 1, route_id, NULL))                                           AS route_id,
	MAX(IF(payment_actual_attempt = 1, partner_account_id, NULL))                                 AS partner_account_id,
	MAX(IF(payment_actual_attempt = 1, partner_account, NULL))                                    AS partner_account,
	MAX(IF(payment_actual_attempt = 1, payment_method_id, NULL))                                  AS payment_method_id,
	MAX(IF(payment_actual_attempt = 1, payment_method_centralized, NULL))                         AS payment_method_centralized,
	MAX(IF(payment_actual_attempt = 1, fee_network, NULL))                                        AS fee_network,
	MAX(IF(payment_actual_attempt = 1, centralized_payment_currency_code, NULL))                  AS centralized_payment_currency_code,

	/**/
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_authorization_fee_id, final_partner_centralized_est_gateway_authorization_fee_id), NULL)) AS centralized_est_gateway_authorization_fee_id,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_authorization_fee_currency, final_partner_centralized_est_gateway_authorization_fee_currency), NULL)) AS centralized_est_gateway_authorization_fee_currency,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_authorization_fee, final_partner_centralized_est_gateway_authorization_fee), NULL))  AS centralized_est_gateway_authorization_fee,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_authorization_fee_usd, final_partner_centralized_est_gateway_authorization_fee_usd), NULL)) AS centralized_est_gateway_authorization_fee_usd,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_blended_fee_id, final_partner_centralized_est_gateway_blended_fee_id), NULL)) AS centralized_est_gateway_blended_fee_id,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_blended_fee, final_partner_centralized_est_gateway_blended_fee), NULL)) AS centralized_est_gateway_blended_fee,
	MAX(IF(payment_actual_attempt = 1, IFNULL(centralized_est_gateway_blended_fee_type, final_partner_centralized_est_gateway_blended_fee_type), NULL)) AS centralized_est_gateway_blended_fee_type,

	MAX(IF(payment_actual_attempt = 1, orchestrator_centralized_est_gateway_authorization_fee_id, NULL)) AS orchestrator_centralized_est_gateway_authorization_fee_id,
	MAX(IF(payment_actual_attempt = 1, orchestrator_centralized_est_gateway_authorization_fee, NULL)) AS orchestrator_centralized_est_gateway_authorization_fee,
	MAX(IF(payment_actual_attempt = 1, orchestrator_centralized_est_gateway_authorization_fee_type, NULL)) AS orchestrator_centralized_est_gateway_authorization_fee_type,
	MAX(IF(payment_actual_attempt = 1, orchestrator_centralized_est_gateway_authorization_fee_currency, NULL)) AS orchestrator_centralized_est_gateway_authorization_fee_currency,
	MAX(IF(payment_actual_attempt = 1, partner_id, NULL)) AS partner_id,
	MAX(IF(payment_actual_attempt = 1, final_partner_id, NULL)) AS final_partner_id,
	-- PAYMENT GATEWAY
	MAX(IF(payment_actual_attempt = 1, payment_gateway, NULL)) as payment_gateway,
	ARRAY_TO_STRING(ARRAY_AGG(CONCAT(IF(payment_block_rank = 1, 'Booking','Post-Booking'),'=',payment_created_at,'=',payment_status,'=','Checkout','=',COALESCE(payment_method_centralized, payment_method),'=',order_id,'=',payment_id,'=',payment_uuid,'=',payment_currency_code,'=',payment_amount,'=',IFNULL(payment_fee,0)) ORDER BY payment_created_at),',')                 AS payment_gateway_breakdown_old,
	ARRAY_TO_STRING(ARRAY_AGG(CONCAT(IF(payment_block_rank = 1, 'Booking','Post-Booking'),'=',payment_created_at,'=',payment_status,'=',payment_gateway,'=',COALESCE(payment_method_centralized, payment_method),'=',payment_gateway_ref,'=',payment_id,'=',payment_uuid,'=',payment_currency_code,'=',payment_amount,'=',IFNULL(payment_fee,0)) ORDER BY payment_created_at),',') AS payment_gateway_breakdown,
	-- CASHBACK
	MAX(IF(payment_actual_attempt = 1, cashback_created_at, NULL))    AS cashback_created_at,
	MAX(IF(payment_actual_attempt = 1, cashback_campaign, NULL))      AS cashback_campaign,
	MAX(IF(payment_actual_attempt = 1, cashback_currency_code, NULL)) AS cashback_currency_code,
	MAX(IF(payment_actual_attempt = 1, cashback_amount, NULL))        AS cashback_amount,
	-- PAYMENT FEE
	MAX(payment_currency_code) as payment_currency_code,
	/*
	  Note: payment_status = 'AUTHORIZED' AND booking_status = 'TICKETINPROCESS' state that a booking is currently in price optimization process
	  and payment_fee should be added for the booking
	*/
	SUM(IF(payment_status IN ('CAPTURED','REFUNDED') OR (payment_status = 'AUTHORIZED' AND booking_status = 'TICKETINPROCESS'),payment_fee,NULL)) as payment_fee,
	SUM(IF(payment_status IN ('CAPTURED','REFUNDED') OR (payment_status = 'AUTHORIZED' AND booking_status = 'TICKETINPROCESS'),payment_fee_usd,NULL)) as payment_fee_usd
FROM
(
	SELECT
		*,
		IF(payment_block_rank = 1 AND payment_created_at = payment_block_max_created_at,1,0) as payment_actual_attempt
	FROM
	(
		SELECT * EXCEPT (
				payment_id,
				payment_uuid,
				route_id,
				payment_method_id,
				client_id,
				fee_network,
				partner_id,
				partner_account_id,
				partner_account,
				cashback_created_at,
				cashback_campaign,
				cashback_currency_code,
				cashback_amount,
				payment_gateway,
				payment_gateway_ref,
				centralized_payment_created_at,
				booking_id,
				centralized_payment_currency_code,
				payment_method_centralized,
				payment_method,
				card_payment_type,
				scheme,
				card_category,
				issuer
			),
		  payments.payment_id,
		  payments.booking_id,
		  payments.payment_ref AS payment_uuid,
		  payment_sources.card_category,
		  COALESCE(centralized_payments_gateway_fees.centralized_payment_currency_code, orchestrator_centralized_payments_gateway_fees.centralized_payment_currency_code) AS centralized_payment_currency_code,
		  COALESCE(centralized_payments_gateway_fees.payment_method_centralized, orchestrator_centralized_payments_gateway_fees.payment_method) AS payment_method_centralized,
		  COALESCE(centralized_payments_gateway_fees.payment_method_centralized, orchestrator_centralized_payments_gateway_fees.payment_method) AS payment_method,
		  COALESCE(centralized_payments_gateway_fees.route_id, orchestrator_centralized_payments_gateway_fees.route_id) AS route_id,
		  COALESCE(centralized_payments_gateway_fees.payment_method_id, orchestrator_centralized_payments_gateway_fees.payment_method_id) AS payment_method_id,
		  COALESCE(centralized_payments_gateway_fees.client_id, orchestrator_centralized_payments_gateway_fees.client_id) AS client_id,
		  COALESCE(centralized_payments_gateway_fees.fee_network, orchestrator_centralized_payments_gateway_fees.fee_network) AS fee_network,
		  COALESCE(centralized_payments_gateway_fees.partner_id, orchestrator_centralized_payments_gateway_fees.partner_id) AS partner_id,
		  COALESCE(centralized_payments_gateway_fees.partner_account_id, orchestrator_centralized_payments_gateway_fees.partner_account_id) AS partner_account_id,
		  COALESCE(centralized_payments_gateway_fees.partner_account, orchestrator_centralized_payments_gateway_fees.partner_account) AS partner_account,
		  COALESCE(centralized_payments_gateway_fees.cashback_created_at, orchestrator_centralized_payments_gateway_fees.cashback_created_at) AS cashback_created_at,
		  COALESCE(centralized_payments_gateway_fees.cashback_campaign, orchestrator_centralized_payments_gateway_fees.cashback_campaign) AS cashback_campaign,
		  COALESCE(centralized_payments_gateway_fees.cashback_currency_code, orchestrator_centralized_payments_gateway_fees.cashback_currency_code) AS cashback_currency_code,
		  COALESCE(centralized_payments_gateway_fees.cashback_amount, orchestrator_centralized_payments_gateway_fees.cashback_amount) AS cashback_amount,
		  COALESCE(centralized_payments_gateway_fees.payment_gateway, orchestrator_centralized_payments_gateway_fees.payment_gateway) AS payment_gateway,
		  COALESCE(centralized_payments_gateway_fees.payment_gateway_ref, orchestrator_centralized_payments_gateway_fees.payment_gateway_ref) AS payment_gateway_ref,

		  DENSE_RANK() OVER (PARTITION BY order_id ORDER BY payment_block_max_created_at)                           AS payment_block_rank,
		  DENSE_RANK() OVER (PARTITION BY order_id, payment_block_max_created_at ORDER BY payment_created_at DESC)  AS payment_attempt_rank,

		  payment_fee * exchange_rate_payment.amount                                                                AS payment_fee_usd
		FROM
		/*-- temp_payments --
		booking_id
		payment_ref
		payment_status
		partner
		payment_currency_code
		payment_amount
		payment_created_at
		order_id
		source_id
		payment_fee
		payment_block_max_created_at
		*/
		(SELECT
		bop.id as payment_id,
    bop.booking_id,
    bop.payment_ref,
    bop.status AS payment_status,
    bop.partner,
    bop.currency_code AS payment_currency_code,
    bop.amount AS payment_amount,
    bop.created_at AS payment_created_at,
    bop.payment_order_id AS order_id,
    pp.source_id,
    pp.payment_fee_amount AS payment_fee,
	  MAX(bop.created_at) OVER (PARTITION BY bop.payment_order_id, bop.amount) AS payment_block_max_created_at
FROM `wego-cloud.back_office.payments` bop
LEFT JOIN `payments.payments` pp ON bop.payment_ref = pp.payment_ref
WHERE bop.status IS NOT NULL
AND bop.status != '') AS payments

		LEFT JOIN
		/* -- bookings --
		booking_id
		booking_status
		*/
		(
		  SELECT id as booking_id, booking_status
		  FROM `wego-cloud.back_office.bookings`
		) as bookings
		ON payments.booking_id = bookings.booking_id

		LEFT JOIN
		/* -- temp_reconciliation --
		payment_uuid
		reference
		processing_country
		payout_country
		processing_currency
		payout_currency
		exchange_rate_gateway_processing_payout
		exchange_rate_gateway_usd
		reconciliation_created_at                       # Will be NULL if post_process is true
		gateway_profile_type_icplusplus                 # Will be NULL if post_process is true
		gateway_profile_type_blended                    # Will be NULL if post_process is true
		gateway_rolling_reserve_processing              # Will be NULL if post_process is true
		gateway_rolling_reserve_processing_outstanding  # Will be NULL if post_process is true
		gateway_authorisation_fee_processing            # Will be NULL if post_process is true
		gateway_blended_fee_processing                  # Will be NULL if post_process is true
		gateway_refund_fee_processing                   # Will be NULL if post_process is true
		gateway_void_fee_processing                     # Will be NULL if post_process is true
		gateway_scheme_fixed_fee_processing             # Will be NULL if post_process is true
		gateway_scheme_variable_fee_processing          # Will be NULL if post_process is true
		gateway_premium_fee_processing                  # Will be NULL if post_process is true
		gateway_scheme_ic_processing                    # Will be NULL if post_process is true
		gateway_authorisation_fee_tax_processing        # Will be NULL if post_process is true
		gateway_blended_fee_tax_processing              # Will be NULL if post_process is true
		gateway_refund_fee_tax_processing               # Will be NULL if post_process is true
		gateway_void_fee_tax_processing                 # Will be NULL if post_process is true
		gateway_scheme_fixed_fee_tax_processing         # Will be NULL if post_process is true
		gateway_scheme_variable_fee_tax_processing      # Will be NULL if post_process is true
		gateway_premium_fee_tax_processing              # Will be NULL if post_process is true
		gateway_scheme_ic_tax_processing                # Will be NULL if post_process is true
		gateway_rolling_reserve_payout                  # Will be NULL if post_process is true
		gateway_rolling_reserve_payout_outstanding      # Will be NULL if post_process is true
		gateway_authorisation_fee_payout                # Will be NULL if post_process is true
		gateway_blended_fee_payout                      # Will be NULL if post_process is true
		gateway_refund_fee_payout                       # Will be NULL if post_process is true
		gateway_void_fee_payout                         # Will be NULL if post_process is true
		gateway_scheme_fixed_fee_payout                 # Will be NULL if post_process is true
		gateway_scheme_variable_fee_payout              # Will be NULL if post_process is true
		gateway_premium_fee_payout                      # Will be NULL if post_process is true
		gateway_scheme_ic_payout                        # Will be NULL if post_process is true
		gateway_authorisation_fee_tax_payout            # Will be NULL if post_process is true
		gateway_blended_fee_tax_payout                  # Will be NULL if post_process is true
		gateway_refund_fee_tax_payout                   # Will be NULL if post_process is true
		gateway_void_fee_tax_payout                     # Will be NULL if post_process is true
		gateway_scheme_fixed_fee_tax_payout             # Will be NULL if post_process is true
		gateway_scheme_variable_fee_tax_payout          # Will be NULL if post_process is true
		gateway_premium_fee_tax_payout                  # Will be NULL if post_process is true
		gateway_scheme_ic_tax_payout                    # Will be NULL if post_process is true
		reconciliation_description                      # Will be NULL if post_process is true
		gateway_rolling_reserve_usd                     # Will be NULL if post_process is true
		gateway_rolling_reserve_outstanding_usd         # Will be NULL if post_process is true
		gateway_authorisation_fee_usd                   # Will be NULL if post_process is true
		gateway_blended_fee_usd                         # Will be NULL if post_process is true
		gateway_refund_fee_usd                          # Will be NULL if post_process is true
		gateway_void_fee_usd                            # Will be NULL if post_process is true
		gateway_scheme_fixed_fee_usd                    # Will be NULL if post_process is true
		gateway_scheme_variable_fee_usd                 # Will be NULL if post_process is true
		gateway_premium_fee_usd                         # Will be NULL if post_process is true
		gateway_scheme_ic_usd                           # Will be NULL if post_process is true
		*/
		(SELECT * FROM (SELECT DISTINCT
	payment_uuid,
	FIRST_VALUE(reference IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reference,
	FIRST_VALUE(processing_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_country,
	FIRST_VALUE(payout_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_country,
	FIRST_VALUE(processing_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_currency,
	FIRST_VALUE(payout_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_currency,
	FIRST_VALUE(exchange_rate_gateway_processing_payout IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as exchange_rate_gateway_processing_payout,

	FIRST_VALUE(TIMESTAMP_TRUNC(reconciliation_created_at, SECOND) IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reconciliation_created_at,

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
    -- ID to join from integrated_bookings_payments.payments to integrated_bookings_flights.payments changed from
    -- pay_ to pxxx from 2021-07-29 onwards (Centralised Payments), pxxx to F.pxxx from 2022-04-11 onwards
    -- Have to take id for historical data, then jump to reference field to extract pxxx format.
    CASE WHEN REGEXP_CONTAINS(reference,r'(^(WF)+)') THEN reference
    WHEN REGEXP_CONTAINS(reference, r'(^(F\.)+)') THEN REGEXP_EXTRACT(reference, r'\.(.*)')
    WHEN REGEXP_CONTAINS(reference,r'(^(p)+)') THEN reference
    ELSE payments.id END AS payment_uuid,
    reference,
    issuer_country as processing_country,
    merchant_country as payout_country,
    processing_currency,
    payout_currency,
    IF(processing_currency = payout_currency, 1, CAST(REGEXP_EXTRACT(breakdowns.type, '@(.*)') AS NUMERIC)) as exchange_rate_gateway_processing_payout, -- If same currency, then exchange rate = 1, else extract
  
    TIMESTAMP(actions.processed_on, '-8:00') as reconciliation_created_at, -- Need to leave it as -8:00 NOT +8:00
    (SELECT MAX(IF(x.type LIKE  '%Scheme%' OR x.type LIKE '%Premium%',1,0)) FROM UNNEST(actions.breakdown) AS x ) as gateway_profile_type_icplusplus,
    (SELECT MAX(IF(x.type LIKE  '%Blended Fee%',1,0)) FROM UNNEST(actions.breakdown) AS x ) as gateway_profile_type_blended,
    IF(breakdowns.type LIKE '%RR%',breakdowns.processing_currency_amount,0) as gateway_rolling_reserve_processing,
    ABS(IF(breakdowns.type LIKE '%Authorization Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_authorisation_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Blended Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_blended_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Refund Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_refund_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Void Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_void_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Scheme Fixed Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_fixed_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Scheme Variable Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_variable_fee_processing,
    ABS(IF(breakdowns.type LIKE '%Premium Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_premium_fee_processing,
    ABS(IF(breakdowns.type LIKE '%SchemeIC%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_ic_processing,
    ABS(IF(breakdowns.type LIKE '%Authorization Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_authorisation_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Blended Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_blended_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Refund Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_refund_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Void Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_void_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Scheme Fixed Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_fixed_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Scheme Variable Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_variable_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Premium Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_premium_fee_tax_processing,
    ABS(IF(breakdowns.type LIKE '%SchemeIC%' AND breakdowns.type LIKE '%Tax%',breakdowns.processing_currency_amount,0)) as gateway_scheme_ic_tax_processing,
    ABS(IF(breakdowns.type LIKE '%Captured%',breakdowns.processing_currency_amount,0)) as gateway_captured_processing,
    IF(breakdowns.type LIKE '%RR%',breakdowns.payout_currency_amount,0) as gateway_rolling_reserve_payout,
    ABS(IF(breakdowns.type LIKE '%Authorization Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_authorisation_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Blended Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_blended_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Refund Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_refund_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Void Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_void_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Scheme Fixed Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_fixed_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Scheme Variable Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_variable_fee_payout,
    ABS(IF(breakdowns.type LIKE '%Premium Fee%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_premium_fee_payout,
    ABS(IF(breakdowns.type LIKE '%SchemeIC%' AND breakdowns.type NOT LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_ic_payout,
    ABS(IF(breakdowns.type LIKE '%Authorization Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_authorisation_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Blended Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_blended_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Refund Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_refund_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Void Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_void_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Scheme Fixed Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_fixed_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Scheme Variable Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_variable_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Premium Fee%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_premium_fee_tax_payout,
    ABS(IF(breakdowns.type LIKE '%SchemeIC%' AND breakdowns.type LIKE '%Tax%',breakdowns.payout_currency_amount,0)) as gateway_scheme_ic_tax_payout,
    ABS(IF(breakdowns.type LIKE '%Captured%',breakdowns.payout_currency_amount,0)) as gateway_captured_payout,
    actions.response_description as reconciliation_description,
  
  FROM `wego-cloud.integrated_bookings_payments.payments*` as payments,
  UNNEST(actions) as actions, UNNEST(actions.breakdown) as breakdowns
) as a


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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS exchange_rate_gateway_reconciliation

ON a.processing_currency = exchange_rate_gateway_reconciliation.base AND DATE(a.reconciliation_created_at) = DATE(exchange_rate_gateway_reconciliation.effective)
)
UNION ALL
SELECT * FROM (SELECT DISTINCT
	payment_uuid,
	FIRST_VALUE(reference IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reference,
	FIRST_VALUE(processing_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_country,
	FIRST_VALUE(payout_country IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_country,
	FIRST_VALUE(processing_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as processing_currency,
	FIRST_VALUE(payout_currency IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as payout_currency,
	FIRST_VALUE(exchange_rate_gateway_processing_payout IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as exchange_rate_gateway_processing_payout,

	FIRST_VALUE(TIMESTAMP_TRUNC(reconciliation_created_at, SECOND) IGNORE NULLS) OVER (PARTITION BY payment_uuid ORDER BY reconciliation_created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as reconciliation_created_at,

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
    reference,
    issuer_country as processing_country,
    entity_country as payout_country,
    processing_currency as processing_currency,
    holding_currency as payout_currency,
    IF(holding_currency = processing_currency, 1, CAST(REGEXP_EXTRACT(breakdown_type, '@(.*)') AS NUMERIC)) as exchange_rate_gateway_processing_payout, -- If same currency, then exchange rate = 1, else extract
  
    TIMESTAMP(processed_on, '-8:00') as reconciliation_created_at, -- Need to leave it as -8:00 NOT +8:00
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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS exchange_rate_gateway_reconciliation

ON co.processing_currency = exchange_rate_gateway_reconciliation.base AND DATE(co.reconciliation_created_at) = DATE(exchange_rate_gateway_reconciliation.effective)
)) as reconciliation
		ON payments.payment_ref = reconciliation.payment_uuid

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
	          AND centralized_payment.centralized_payment_currency_code = centralized_gateway_fees.payment_currency
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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
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
	  ON centralized_payments_gateway_fees.payment_ref = payments.payment_ref

	  LEFT JOIN
		/* -- temp_payment_sources --
		source_id
		card_type
		card_category
		card_scheme
		card_issuer
		card_issuer_country_code
		*/
		(SELECT
    DISTINCT id as source_id,
    FIRST_VALUE(card_type IGNORE NULLS) OVER (
        PARTITION BY source_id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as card_type,
    FIRST_VALUE(card_category IGNORE NULLS) OVER (
        PARTITION BY source_id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as card_category,
    FIRST_VALUE(scheme IGNORE NULLS) OVER (
        PARTITION BY source_id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as card_scheme,
    FIRST_VALUE(issuer IGNORE NULLS) OVER (
        PARTITION BY source_id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as card_issuer,
    FIRST_VALUE(issuer_country_code IGNORE NULLS) OVER (
        PARTITION BY source_id
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as card_issuer_country_code
FROM
    `integrated_bookings_flights.payment_sources`) as payment_sources
		/*The data is from the integrated_bookings_flights dataset*/
		ON payment_sources.source_id = payments.source_id

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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS exchange_rate_centralized_gateway_authorisation_fees ON fees_partner_account_payment_method_fees.centralized_est_gateway_authorization_fee_currency = exchange_rate_centralized_gateway_authorisation_fees.base AND DATE(centralized_payment.centralized_payment_created_at) = exchange_rate_centralized_gateway_authorisation_fees.effective
	

	WHERE fees_partner_account_payment_method_fees_0.centralized_est_gateway_authorization_fee_id IS NOT NULL
) as orchestrator_centralized_payments_gateway_fees
		/* temp_orchestrator_centralized_payments_gateway_fees will contain fees for payment records that have final_partner_id */
		ON orchestrator_centralized_payments_gateway_fees.payment_ref = payments.payment_ref

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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS exchange_rate_payment ON payments.payment_currency_code = exchange_rate_payment.base AND DATE(payments.payment_created_at) = exchange_rate_payment.effective
		QUALIFY ROW_NUMBER() OVER (PARTITION BY payments.payment_id ORDER BY end_datetime_blended DESC) = 1
	)
)
GROUP BY 1) combined_order_level ON b.id = combined_order_level.booking_id
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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) user_exchange_rates ON p.user_currency_code = user_exchange_rates.base AND DATE(p.created_at) = user_exchange_rates.effective
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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) vendor_exchange_rates ON p.currency_code = vendor_exchange_rates.base AND DATE(p.created_at) = vendor_exchange_rates.effective
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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS erSAR
		ON erSAR.base = 'SAR' AND DATE(erSAR.effective) = DATE(b.created_at)

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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS erAED
		ON erAED.base = 'AED' AND DATE(erAED.effective) = DATE(b.created_at)

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

	(	SELECT
	  DISTINCT FORMAT_DATE("%Y%m%d", DATE(created_at)) AS formatted_created_date
	FROM
	  `wego-cloud.back_office.prices`
	WHERE
	  created_at IS NOT NULL)
) AS erEGP
		ON erEGP.base = 'EGP' AND DATE(erEGP.effective) = DATE(b.created_at)
	/*End of joining exchange rate for Netsuite integration*/
	QUALIFY
			b.parent_booking_ref IS NULL
		OR
			ROW_NUMBER() OVER (PARTITION BY b.parent_booking_ref ORDER BY i.created_at DESC) = 1
)
{% endraw %}
