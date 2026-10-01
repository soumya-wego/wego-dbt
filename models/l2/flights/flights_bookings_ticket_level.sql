{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Flights Bookings Ticket Level
-- Destination: wego_analytics.flights_bookings_ticket_level  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('aaaaa_temporary_export_folder', 'flights_ancillary_logs') }}
-- depends_on: {{ source('analytics', 'exchange_rates') }}
-- depends_on: {{ source('integrated_bookings_flights', 'etickets') }}
-- depends_on: {{ source('integrated_bookings_flights', 'gds') }}
-- depends_on: {{ source('integrated_bookings_flights', 'ipccs') }}
-- depends_on: {{ source('integrated_bookings_flights', 'itineraries') }}
-- depends_on: {{ source('integrated_bookings_flights', 'legs') }}
-- depends_on: {{ source('integrated_bookings_flights', 'pccs') }}
-- depends_on: {{ source('integrated_bookings_flights', 'prices') }}
-- depends_on: {{ source('integrated_bookings_flights', 'segments') }}
-- depends_on: {{ source('integrated_bookings_flights', 'vendor_fees') }}
-- depends_on: {{ source('integrated_bookings_flights', 'vendors') }}
-- depends_on: {{ source('place_services', 'airports') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
{% raw %}
CREATE OR REPLACE TABLE `wego-cloud.wego_analytics.flights_bookings_ticket_level`
PARTITION BY DATE(ticket_created_at) AS
SELECT
	*,
	CASE
    WHEN eticket_status = "TICKETED" THEN IFNULL(markup_amount_usd,0) + IFNULL(anclillary_margin_amount_usd,0) + IFNULL(total_commissions_usd,0)
    WHEN eticket_status IN ("REFUND_INITIATED", "REFUNDED", "CANCELLED") THEN IFNULL(anclillary_margin_amount_usd,0)
    WHEN eticket_status = "EXCHANGED" THEN IFNULL(anclillary_margin_amount_usd,0)
    ELSE 0
	END AS gross_revenue_in_usd,
	CASE
    WHEN eticket_status = "TICKETED" THEN (IFNULL(markup_amount_usd,0) + IFNULL(anclillary_margin_amount_usd,0) + IFNULL(total_commissions_usd,0)) - IFNULL(cost_of_sales_usd,0)
    WHEN eticket_status IN ("REFUND_INITIATED", "REFUNDED", "CANCELLED") THEN IFNULL(anclillary_margin_amount_usd,0) - IFNULL(cost_of_sales_usd,0)
    WHEN eticket_status = "EXCHANGED" THEN IFNULL(anclillary_margin_amount_usd,0) - IFNULL(cost_of_sales_usd,0)
    ELSE 0
	END AS revenue_in_usd
FROM
(
	SELECT
		*,
		IFNULL(vendor_commissions_upfront_backend_usd,0)
			+ IFNULL(vendor_commissions_iata_usd,0)
				+ IFNULL(vendor_commissions_plb_usd,0) AS total_commissions_usd,
		CASE
			WHEN eticket_status = "TICKETED" THEN ticket_issuance_fee_usd
			WHEN eticket_status IN ("REFUND_INITIATED", "REFUNDED", "CANCELLED") THEN ticket_refund_fee_usd
			WHEN eticket_status = "EXCHANGED" THEN ticket_exchange_fee_usd
			ELSE 0
		END AS cost_of_sales_usd
	FROM
	(
		SELECT
			main0.ticket_number,
			main0.booking_id,
			main0.ticket_created_at,
			main0.ticket_updated_at,
			main0.supplier_payment_method,
			main0.gds_ref,
			main0.itinerary_id,
			main0.integration_type,
			main0.total_amount,
			main0.total_amount * markup_ex.amount AS total_amount_usd,
			main0.vendor_total_amount,
			main0.vendor_total_amount * markup_ex.amount AS vendor_total_amount_usd,
			main0.passenger_id,
			main0.eticket_status,
			main0.passenger_type,
			main0.segment_id,
			main0.departure_airport_code,
			main0.arrival_airport_code,
			main0.scd_dep_date_time,
			main0.scd_arr_date_time,
			main0.departure_country_code,
			main0.arrival_country_code,
			main0.route_countries,
			main0.cabin,
			main0.gds,
			main0.ipcc_currency_code,
			main0.ipcc,
			main0.pcc,
			main0.vendor_id,
			main0.vendor,
			main0.ticket_issuance_currency,
			main0.ticket_refund_currency,
			main0.ticket_exchange_currency,
			main0.ticket_issuance_fee,
			main0.ticket_issuance_fee * issue_ex.amount AS ticket_issuance_fee_usd,
			main0.ticket_refund_fee,
			main0.ticket_refund_fee * refund_ex.amount AS ticket_refund_fee_usd,
			main0.ticket_exchange_fee,
			main0.ticket_exchange_fee * exchg_ex.amount AS ticket_exchange_fee_usd,
			main0.markup_amount,
			main0.markup_currency_code,
			main0.markup_amount * markup_ex.amount AS markup_amount_usd,
			main0.vendor_commissions_upfront_backend,
			main0.vendor_commissions_iata,
			main0.vendor_commissions_plb,
			main0.vendor_commissions_currency_code,
			main0.vendor_commissions_upfront_backend * vcomm_ex.amount AS vendor_commissions_upfront_backend_usd,
			main0.vendor_commissions_iata * vcomm_ex.amount AS vendor_commissions_iata_usd,
			main0.vendor_commissions_plb * vcomm_ex.amount AS vendor_commissions_plb_usd,
			flights_ancillary_logs.ancillary_selection_status,
			flights_ancillary_logs.ancillary_type,
			flights_ancillary_logs.anclillary_margin_amount_usd,
			flights_ancillary_logs.anclillary_vendor_total_amount,
			flights_ancillary_logs.anclillary_vendor_currency_code
		FROM
		/* -- main0 --
		ticket_number
		booking_id
		ticket_created_at
		ticket_updated_at
		supplier_payment_method
		gds_ref
		itinerary_id
		integration_type
		total_amount
		vendor_total_amount
		passenger_id
		eticket_status
		passenger_type
		segment_id
		departure_airport_code
		arrival_airport_code
		scd_dep_date_time
		scd_arr_date_time
		departure_country_code
		arrival_country_code
		route_countries
		cabin
		gds
		ipcc_currency_code
		ipcc
		pcc
		vendor_id
		vendor
		ticket_issuance_currency
		ticket_refund_currency
		ticket_exchange_currency
		ticket_issuance_fee
		ticket_refund_fee
		ticket_exchange_fee
		markup_amount
		markup_currency_code
		vendor_commissions_upfront_backend
		vendor_commissions_iata
		vendor_commissions_plb
		vendor_commissions_currency_code
		*/
		(SELECT
  ticket_number,
  ANY_VALUE(booking_id) AS booking_id,
  ANY_VALUE(ticket_created_at) AS ticket_created_at,
  ANY_VALUE(ticket_updated_at) AS ticket_updated_at,
  ANY_VALUE(supplier_payment_method) AS supplier_payment_method,
  ANY_VALUE(gds_ref) AS gds_ref,
  ANY_VALUE(itinerary_id) AS itinerary_id,
  ANY_VALUE(integration_type) AS integration_type,
  ANY_VALUE(total_amount) AS total_amount,
  ANY_VALUE(vendor_total_amount) AS vendor_total_amount,
  ANY_VALUE(passenger_id) AS passenger_id,
  ANY_VALUE(eticket_status) AS eticket_status,
  ANY_VALUE(passenger_type) AS passenger_type,
  ARRAY_AGG(segment_id ORDER BY segment_id) AS segment_id,
  ARRAY_AGG(departure_airport_code ORDER BY segment_id) AS departure_airport_code,
  ARRAY_AGG(arrival_airport_code ORDER BY segment_id) AS arrival_airport_code,
  ARRAY_AGG(scd_dep_date_time ORDER BY segment_id) AS scd_dep_date_time,
  ARRAY_AGG(scd_arr_date_time ORDER BY segment_id) AS scd_arr_date_time,
  ARRAY_AGG(departure_country_code ORDER BY segment_id) AS departure_country_code,
  ARRAY_AGG(arrival_country_code ORDER BY segment_id) AS arrival_country_code,
  STRING_AGG(CONCAT(IFNULL(departure_country_code, ''), '-', IFNULL(arrival_country_code, '')), '=' ORDER BY segment_id) AS route_countries,
  STRING_AGG(cabin, '=' ORDER BY segment_id) AS cabin,
  ANY_VALUE(gds) AS gds,
  ANY_VALUE(ipcc_currency_code) AS ipcc_currency_code,
  ANY_VALUE(ipcc) AS ipcc,
  ANY_VALUE(pcc) AS pcc,
  ANY_VALUE(vendor_id) AS vendor_id,
  ANY_VALUE(vendor) AS vendor,
  ANY_VALUE(ticket_issuance_currency) AS ticket_issuance_currency,
  ANY_VALUE(ticket_refund_currency) AS ticket_refund_currency,
  ANY_VALUE(ticket_exchange_currency) AS ticket_exchange_currency,
  ANY_VALUE(ticket_issuance_fee) AS ticket_issuance_fee,
  ANY_VALUE(ticket_refund_fee) AS ticket_refund_fee,
  ANY_VALUE(ticket_exchange_fee) AS ticket_exchange_fee,
  ANY_VALUE(markup_amount) AS markup_amount,
  ANY_VALUE(markup_currency_code) AS markup_currency_code,
  ANY_VALUE(vendor_commissions_upfront_backend) AS vendor_commissions_upfront_backend,
  ANY_VALUE(vendor_commissions_iata) AS vendor_commissions_iata,
  ANY_VALUE(vendor_commissions_plb) AS vendor_commissions_plb,
  ANY_VALUE(vendor_commissions_currency_code) AS vendor_commissions_currency_code
FROM
/* -- temp_main --
booking_id
ticket_number
ticket_created_at
supplier_payment_method
gds_ref
itinerary_id
integration_type
total_amount
vendor_total_amount
vendor_currency_code
passenger_id
eticket_status
passenger_type
segment_id
departure_airport_code
arrival_airport_code
scd_dep_date_time
scd_arr_date_time
departure_country_code
arrival_country_code
route_countries
cabin
gds
ipcc_currency_code
ipcc
pcc
vendor_id
vendor
ticket_issuance_currency
ticket_refund_currency
ticket_exchange_currency
ticket_issuance_fee
ticket_refund_fee
ticket_exchange_fee
markup_amount
markup_currency_code
vendor_commissions_upfront_backend
vendor_commissions_iata
vendor_commissions_plb
vendor_commissions_currency_code
*/
(SELECT
  temp_etickets_segments_legs_itineraries_prices_bfc.booking_id,
  temp_etickets_segments_legs_itineraries_prices_bfc.ticket_number,
  temp_etickets_segments_legs_itineraries_prices_bfc.ticket_created_at,
  temp_etickets_segments_legs_itineraries_prices_bfc.ticket_updated_at,
  temp_etickets_segments_legs_itineraries_prices_bfc.supplier_payment_method,
  temp_etickets_segments_legs_itineraries_prices_bfc.gds_ref,
  temp_etickets_segments_legs_itineraries_prices_bfc.itinerary_id,
  temp_etickets_segments_legs_itineraries_prices_bfc.integration_type,
  temp_etickets_segments_legs_itineraries_prices_bfc.total_amount,
  temp_etickets_segments_legs_itineraries_prices_bfc.vendor_total_amount,
  temp_etickets_segments_legs_itineraries_prices_bfc.passenger_id,
  temp_etickets_segments_legs_itineraries_prices_bfc.eticket_status,
  temp_etickets_segments_legs_itineraries_prices_bfc.passenger_type,
  temp_etickets_segments_legs_itineraries_prices_bfc.segment_id,
  temp_etickets_segments_legs_itineraries_prices_bfc.departure_airport_code,
  temp_etickets_segments_legs_itineraries_prices_bfc.arrival_airport_code,
  temp_etickets_segments_legs_itineraries_prices_bfc.scd_dep_date_time,
  temp_etickets_segments_legs_itineraries_prices_bfc.scd_arr_date_time,
  temp_etickets_segments_legs_itineraries_prices_bfc.departure_country_code,
  temp_etickets_segments_legs_itineraries_prices_bfc.arrival_country_code,
  temp_etickets_segments_legs_itineraries_prices_bfc.cabin,
  gds_table.gds,
	ipcc_table.ipcc_currency_code,
  ipcc_table.ipcc,
	pcc_table.pcc,
	vendor_table.vendor_id,
	vendor_table.vendor,
  vendor_fees.ticket_issuance_currency,
  vendor_fees.ticket_refund_currency,
  vendor_fees.ticket_exchange_currency,
  vendor_fees.ticket_issuance_fee,
  IF(eticket_status = 'REFUND_INITIATED', vendor_fees.ticket_refund_fee, 0) AS ticket_refund_fee,
  IF(eticket_status = 'EXCHANGED', vendor_fees.ticket_exchange_fee, 0) AS ticket_exchange_fee,
  temp_etickets_segments_legs_itineraries_prices_bfc.total_amount - temp_etickets_segments_legs_itineraries_prices_bfc.vendor_total_amount AS markup_amount,
  temp_etickets_segments_legs_itineraries_prices_bfc.vendor_currency_code AS markup_currency_code,
  IF(fb.conversions_tracked = 1,temp_etickets_segments_legs_itineraries_prices_bfc.vendor_commissions_upfront_backend, 0) AS vendor_commissions_upfront_backend,
	IF(fb.conversions_tracked = 1,temp_etickets_segments_legs_itineraries_prices_bfc.vendor_commissions_iata, 0) AS vendor_commissions_iata,
	IF(fb.conversions_tracked = 1,temp_etickets_segments_legs_itineraries_prices_bfc.vendor_commissions_plb, 0) AS vendor_commissions_plb,
	IF(fb.conversions_tracked = 1,temp_etickets_segments_legs_itineraries_prices_bfc.vendor_commissions_currency_code, NULL) AS vendor_commissions_currency_code,
FROM
/* -- temp_etickets_segments_legs_itineraries_prices_bfc --
ticket_number
ticket_created_at
ticket_updated_at
supplier_payment_method
gds_ref
booking_id
itinerary_id
gds_code
ipcc
integration_type
total_amount
vendor_total_amount
vendor_commissions_upfront_backend
vendor_commissions_iata
vendor_commissions_plb
vendor_commissions_currency_code
vendor_currency_code
passenger_id
eticket_status
passenger_type
segment_id
departure_airport_code
arrival_airport_code
scd_dep_date_time
scd_arr_date_time
departure_country_code
arrival_country_code
cabin
*/
(WITH airport_details AS (
  (SELECT airport.code as airport_code,
--airport.base_name as airport_name,
ANY_VALUE(location.code) as city_code,
--location.base_name as city_name,
ANY_VALUE(cou.code) as country_code
--, cou.base_name as country_name
FROM 
    (SELECT * EXCEPT(dedupe)
    FROM
    (SELECT code, location_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY enabled DESC, updated_at DESC, created_at DESC) as dedupe
    FROM `place_services.airports`)
    WHERE dedupe = 1) as airport
LEFT JOIN 
    (SELECT * EXCEPT(dedupe)
    FROM
    (SELECT code, id, country_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY active DESC, updated_at DESC, created_at DESC) as dedupe
    FROM `place_services.locations`)
    WHERE dedupe = 1) as location ON airport.location_id=location.id
LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id
GROUP BY airport_code)
),
city_details AS (
  (SELECT
location.code as city_code,
--location.base_name as city_name,
ANY_VALUE(cou.code) as country_code
--, cou.base_name as country_name
FROM 
    (SELECT * EXCEPT(dedupe)
    FROM
    (SELECT code, country_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY active DESC, updated_at DESC, created_at DESC) as dedupe
    FROM `place_services.locations`)
    WHERE dedupe = 1) as location
LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id
GROUP BY city_code)
)
SELECT DISTINCT
  et.ticket_number,
  et.created_at AS ticket_created_at,
  et.updated_at AS ticket_updated_at,
  i.b2b_payment_method AS supplier_payment_method,
  i.gds_ref,
  i.booking_id,
  i.id AS itinerary_id,
  i.gds AS gds_code,
  i.ipcc,
  i.integration_type,
  CASE
    WHEN et.passenger_type = 'ADULT' THEN IFNULL(CAST(JSON_QUERY(p.breakdown, '$.adultPrice.totalAmount') AS NUMERIC),0)
    WHEN et.passenger_type = 'INFANT' THEN IFNULL(CAST(JSON_QUERY(p.breakdown, '$.infantPrice.totalAmount') AS NUMERIC),0)
    WHEN et.passenger_type = 'CHILD' THEN IFNULL(CAST(JSON_QUERY(p.breakdown, '$.childPrice.totalAmount') AS NUMERIC),0)
  END AS total_amount,
  CASE
    WHEN et.passenger_type = 'ADULT' THEN IFNULL(CAST(JSON_QUERY(p.vendor_breakdown, '$.adultPrice.totalAmount') AS NUMERIC),0)
    WHEN et.passenger_type = 'INFANT' THEN IFNULL(CAST(JSON_QUERY(p.vendor_breakdown, '$.infantPrice.totalAmount') AS NUMERIC),0)
    WHEN et.passenger_type = 'CHILD' THEN IFNULL(CAST(JSON_QUERY(p.vendor_breakdown, '$.childPrice.totalAmount') AS NUMERIC),0)
  END AS vendor_total_amount,
  CASE
    WHEN et.passenger_type = 'ADULT' THEN IFNULL(bfc.adult_total_commission,0)
    WHEN et.passenger_type = 'INFANT' THEN 0
    WHEN et.passenger_type = 'CHILD' THEN IFNULL(bfc.child_total_commission,0)
  END AS vendor_commissions_upfront_backend,
  CASE
    WHEN et.passenger_type = 'ADULT' THEN IFNULL(bfc.adult_total_iata,0)
    WHEN et.passenger_type = 'INFANT' THEN 0
    WHEN et.passenger_type = 'CHILD' THEN IFNULL(bfc.child_total_iata,0)
  END AS vendor_commissions_iata,
  CASE
    WHEN et.passenger_type = 'ADULT' THEN IFNULL(bfc.adult_total_plb,0)
    WHEN et.passenger_type = 'INFANT' THEN 0
    WHEN et.passenger_type = 'CHILD' THEN IFNULL(bfc.child_total_plb,0)
  END AS vendor_commissions_plb,
  bfc.currency_code AS vendor_commissions_currency_code,
  p.currency_code AS vendor_currency_code,
  et.passenger_id,
  et.status AS eticket_status,
  et.passenger_type,
  s.id AS segment_id,
  s.departure_airport_code,
  s.arrival_airport_code,
  s.scd_dep_date_time,
  s.scd_arr_date_time,
  COALESCE(dep_airport.country_code, dep_city.country_code) AS departure_country_code,
  COALESCE(arr_airport.country_code, arr_city.country_code) AS arrival_country_code,
  s.cabin
FROM
  `integrated_bookings_flights.etickets` AS et
LEFT JOIN
  (SELECT id, leg_id, departure_airport_code, arrival_airport_code, scd_dep_date_time, scd_arr_date_time, cabin FROM `integrated_bookings_flights.segments*` WHERE LENGTH(_TABLE_SUFFIX) = 8) AS s ON et.segment_id = s.id
LEFT JOIN
  (SELECT id, itinerary_id FROM `integrated_bookings_flights.legs*` WHERE LENGTH(_TABLE_SUFFIX) = 8) AS l ON s.leg_id = l.id
LEFT JOIN
  (SELECT id, branded_fare_id, b2b_payment_method, gds_ref, booking_id, gds, ipcc, integration_type FROM `integrated_bookings_flights.itineraries*` WHERE LENGTH(_TABLE_SUFFIX) = 8) AS i ON l.itinerary_id = i.id
LEFT JOIN
  (SELECT DISTINCT itinerary_id, currency_code, breakdown, vendor_breakdown FROM `integrated_bookings_flights.prices*` WHERE LENGTH(_TABLE_SUFFIX) = 8) AS p ON i.id = p.itinerary_id
LEFT JOIN
/* -- temp_branded_fare_calculations --
branded_fare_id
adult_total_commission
child_total_commission
adult_total_iata
child_total_iata
adult_total_plb
child_total_plb
currency_code
*/
(SELECT
	branded_fare_id,
	adult_total_commission,
	child_total_commission,
	adult_total_iata,
	child_total_iata,
	adult_total_plb,
	child_total_plb,
	currency_code
FROM
`services_curiosity.branded_fare_calculations*` AS bfc
QUALIFY ROW_NUMBER() OVER (PARTITION BY bfc.branded_fare_id ORDER BY bfc.created_at DESC, bfc.final_total_usd DESC) = 1) AS bfc ON i.branded_fare_id = bfc.branded_fare_id

LEFT JOIN airport_details AS dep_airport ON dep_airport.airport_code = s.departure_airport_code
LEFT JOIN airport_details AS arr_airport ON arr_airport.airport_code = s.arrival_airport_code
LEFT JOIN city_details AS dep_city ON dep_city.city_code = s.departure_airport_code
LEFT JOIN city_details AS arr_city ON arr_city.city_code = s.arrival_airport_code
WHERE LENGTH(et.ticket_number) > 0 AND i.id IS NOT NULL) AS temp_etickets_segments_legs_itineraries_prices_bfc
LEFT JOIN
/* -- temp_gds --
gds_code
gds
*/
(SELECT
    DISTINCT code as gds_code,
    LAST_VALUE(name) OVER (
        PARTITION BY code
        ORDER BY
            created_at ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ) as gds
FROM
    `integrated_bookings_flights.gds`) as gds_table ON gds_table.gds_code = temp_etickets_segments_legs_itineraries_prices_bfc.gds_code
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
    `integrated_bookings_flights.ipccs`) as ipcc_table ON ipcc_table.ipcc = temp_etickets_segments_legs_itineraries_prices_bfc.ipcc
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
    `integrated_bookings_flights.pccs`) as pcc_table ON ipcc_table.pcc_id = pcc_table.pcc_id
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
    `integrated_bookings_flights.vendors`) as vendor_table ON vendor_table.vendor_code = ipcc_table.vendor_code
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
    1) as vendor_fees ON vendor_table.vendor_id = vendor_fees.vendor_id

LEFT JOIN `wego-cloud.wego_analytics.flights_bookings` AS fb ON temp_etickets_segments_legs_itineraries_prices_bfc.booking_id = fb.booking_id)
GROUP BY ticket_number) AS main0
		LEFT JOIN
		/*-- temp_flights_ancillary_logs --
		booking_id
		itinerary_id
		passenger_id
		ancillary_selection_status
		ancillary_type
		anclillary_margin_amount_usd
		anclillary_vendor_total_amount
		anclillary_vendor_currency_code
		*/
		(SELECT
	booking_id,
	itinerary_id,
	passenger_id,
	ARRAY_AGG(ancillary_selection_status ORDER BY created_at DESC) AS ancillary_selection_status,
	ARRAY_AGG(ancillary_type ORDER BY created_at DESC) AS ancillary_type,
	SUM(anclillary_margin_amount_usd) AS anclillary_margin_amount_usd,
	SUM(vendor_total_amount) AS anclillary_vendor_total_amount,
	ANY_VALUE(vendor_currency_code) AS anclillary_vendor_currency_code
FROM
(
	SELECT
	  booking_id,
	  itinerary_id,
	  passenger_id,
	  selection_status as ancillary_selection_status,
	  ancillary_type,
	  margin_amount_usd as anclillary_margin_amount_usd,
	  vendor_total_amount,
	  vendor_currency_code,
	  created_at
	FROM
	  `aaaaa_temporary_export_folder.flights_ancillary_logs`
	QUALIFY RANK() OVER (PARTITION BY booking_id, itinerary_id, passenger_id, ancillary_type ORDER BY created_at DESC) = 1
)
GROUP BY booking_id, itinerary_id, passenger_id) AS flights_ancillary_logs
			ON main0.booking_id = flights_ancillary_logs.booking_id
				AND main0.itinerary_id = flights_ancillary_logs.itinerary_id
					AND main0.passenger_id = flights_ancillary_logs.passenger_id

		-- markup FX
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE
  LENGTH(_TABLE_SUFFIX) = 8
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE('2020-08-11') AND CURRENT_DATE()) AS markup_ex
			ON main0.markup_currency_code = markup_ex.base
			AND DATE(main0.ticket_created_at) = markup_ex.effective

		-- issuance FX
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE
  LENGTH(_TABLE_SUFFIX) = 8
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE('2020-08-11') AND CURRENT_DATE()) AS issue_ex
			ON main0.ticket_issuance_currency = issue_ex.base
			AND DATE(main0.ticket_created_at) = issue_ex.effective

		-- refund FX
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE
  LENGTH(_TABLE_SUFFIX) = 8
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE('2020-08-11') AND CURRENT_DATE()) AS refund_ex
			ON main0.ticket_refund_currency = refund_ex.base
			AND DATE(main0.ticket_created_at) = refund_ex.effective

		-- exchange FX
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE
  LENGTH(_TABLE_SUFFIX) = 8
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE('2020-08-11') AND CURRENT_DATE()) AS exchg_ex
			ON main0.ticket_exchange_currency = exchg_ex.base
			AND DATE(main0.ticket_created_at) = exchg_ex.effective

		-- vendor commissions FX
		LEFT JOIN (SELECT
  base,
  amount,
  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
FROM
  `analytics.exchange_rates*`
WHERE
  LENGTH(_TABLE_SUFFIX) = 8
  AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE('2020-08-11') AND CURRENT_DATE()) AS vcomm_ex
			ON main0.vendor_commissions_currency_code = vcomm_ex.base
			AND DATE(main0.ticket_created_at) = vcomm_ex.effective
	)
)
{% endraw %}
