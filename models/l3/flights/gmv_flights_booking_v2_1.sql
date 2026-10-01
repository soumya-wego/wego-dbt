{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bow_flights_bookings_netsuite_details_full
-- Destination: aaaaa_temporary_export_folder.gmv_flights_booking_v2_1  (unchanged)
-- Schedule   : every day 07:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
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

CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_booking_v2_1` AS (
	with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
select distinct
	flight_logs_ticketed.booking_ref,
	flight_logs_ticketed.booking_id,
	flight_logs_ticketed.vendor,
	flight_logs_ticketed.gds,
	flight_logs_ticketed.gds_ref,
	cm.ns_market as market,
	IFNULL(flight_logs_ticketed.total_price_usd,0)
	    - IFNULL(flight_logs_ticketed.booking_fee_usd,0)
	    - IFNULL(flight_logs_ticketed.payment_fee_amount_usd,0) as total_amount_usd,
	flight_logs_ticketed.booking_fee_usd,
	flight_logs_ticketed.gds_segment_fee_usd,
	flight_logs_ticketed.vendor_commissions_upfront_backend_usd,
	flight_logs_ticketed.vendor_commissions_iata_usd,
	flight_logs_ticketed.vendor_commissions_plb_usd,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then flight_logs_ticketed.payment_fee_amount_usd
	end as payment_fee_usd,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then fi.total_price_usd
	end as insurance_total_amount_usd,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then fi.wego_commission_usd
	end as wego_commission_usd,
	(flight_logs_ticketed.optimize_cost_usd + flight_logs_ticketed.optimize_revenue_usd) as optimized_value_usd,
	if(flight_logs_ticketed.markup_amount_usd > 0, flight_logs_ticketed.markup_amount_usd, 0) as markup,
	if(flight_logs_ticketed.markup_amount_usd < 0, flight_logs_ticketed.markup_amount_usd, 0) as markdown,
	fb.unused_tickets_amount_usd,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then fb.payment_gateway
	end as payment_gateway_provider,
	/* Newly added fields */
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.flights_discount_vat
	end as flights_discount_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.markup_vat
	end as markup_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.insurance_vat
	end as insurance_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.booking_fee_vat
	end as booking_fee_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.payment_fee_vat
	end as payment_fee_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.baggage_fee_vat
	end as baggage_fee_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.meals_fee_vat
	end as meals_fee_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
		 then vat.flights_seats_markup_vat
	end as flights_seats_markup_vat,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then
				struct(
          
            flight_logs_ticketed.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount as invoice_gov_tax_amount_sar,
            flight_logs_ticketed.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount as invoice_gov_tax_amount_egp,
            flight_logs_ticketed.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount as invoice_gov_tax_amount_pkr,
          
					flight_logs_ticketed.invoice_gov_tax_details.vat_amount as invoice_gov_tax_amount_usd /*Default in USD*/
				)
	end as invoice_gov_tax_amount, /*Create invoice gov tax amount in SAR, AED, EGP, PKR and USD*/
	flight_logs_ticketed.integration_type,
	flight_logs_ticketed.created_time,
	CASE
    
      WHEN flight_logs_ticketed.site_code IN ('SA', 'EG', 'PK') THEN flight_logs_ticketed.site_code
    
		ELSE 'All Others'
  END AS Site_Code,
  
    /*AE site code is not supported in this version of the query*/
    IF(flight_logs_ticketed.site_code = 'AE', 1.0, flight_logs_ticketed.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
    IF(flight_logs_ticketed.site_code = 'AE', 'USD', flight_logs_ticketed.other_currency_code) as other_currency_code,
  
	flight_logs_ticketed.vendor_total_amount_usd,
	flight_logs_ticketed.promo_discount_amount_usd,
	
	(CASE
    -- Ongoing, still within budget
    WHEN TRIM(fb.promo_code) = 'KFH100' AND cutoff_KFH100 IS NULL THEN SAFE_CAST(0 AS NUMERIC)
    
    -- Eventually overbudget but it's in the future
    WHEN TRIM(fb.promo_code) = 'KFH100' AND flight_logs_ticketed.created_time < cutoff_KFH100 THEN SAFE_CAST(0 AS NUMERIC)
    
    -- Partially beared by Wego at exact cutoff booking
    WHEN TRIM(fb.promo_code) = 'KFH100' AND flight_logs_ticketed.booking_ref = cutoff_ref_KFH100 THEN cutoff_partial_KFH100

    -- All overbudget bookings, and also normal promo (non-partnership)
    ELSE SAFE_CAST(fb.promo_discount_amount_usd AS NUMERIC)
  END) AS wego_beared_promo_discount_amount_usd,
	
  /*
  * In the flights_bookings_ticketed_logs_audit, we will have multiple rows for the same booking_ref if the booking is Sum of One Way
  * We want to get the vendor_total_amount_usd and margin_amount_usd only for the first row of the results as the join with the flights_ancillary_ota_logs table
  * will give us multiple rows for the same booking_ref if the booking is Sum of One Way
  */
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then flights_ancillary_ota_logs.vendor_total_amount_usd
	end as ancillary_vendor_total_amount_usd,
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then flights_ancillary_ota_logs.margin_amount_usd
	end as ancillary_margin_amount_usd,
	
from (
	SELECT
    * EXCEPT(
			source_product,
			conversions_tracked
		)
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_ticketed_logs_audit`
  WHERE TRUE
    AND source_product="bow"
		AND conversions_tracked = 1
) AS flight_logs_ticketed
left join `wego_analytics.flights_bookings` fb on flight_logs_ticketed.booking_ref = fb.booking_ref
left join `wego-cloud.aaaaa_temporary_export_folder.flights_ancillary_ota_logs` flights_ancillary_ota_logs on flight_logs_ticketed.booking_ref = flights_ancillary_ota_logs.booking_ref

  left join exchange_rates as exchange_rates_sar on 'SAR' = exchange_rates_sar.base and date(flight_logs_ticketed.invoice_gov_tax_details.created_at) = date(exchange_rates_sar.effective)
  left join exchange_rates as exchange_rates_egp on 'EGP' = exchange_rates_egp.base and date(flight_logs_ticketed.invoice_gov_tax_details.created_at) = date(exchange_rates_egp.effective)
  left join exchange_rates as exchange_rates_pkr on 'PKR' = exchange_rates_pkr.base and date(flight_logs_ticketed.invoice_gov_tax_details.created_at) = date(exchange_rates_pkr.effective)

left join
/*-- flights_insurance --
booking_ref
total_price_usd
wego_commission_usd
*/
(
	select
		booking_ref,
		sum(total_price_usd) as total_price_usd,
		sum(wego_commission_usd) as wego_commission_usd
	from `wego_analytics.flights_insurance`
	group by 1
) fi on flight_logs_ticketed.booking_ref = fi.booking_ref
left join `analytics.countries_misc` cm on flight_logs_ticketed.site_code = cm.country_code
/* -- vat --
booking_ref
flights_discount_vat: STRUCT
	flights_discount_vat_usd
	flights_discount_vat_sar
	flights_discount_vat_aed
	flights_discount_vat_egp
	flights_discount_vat_pkr
markup_vat: STRUCT
	markup_vat_usd
	markup_vat_sar
	markup_vat_aed
	markup_vat_egp
	markup_vat_pkr
insurance_vat: STRUCT
	insurance_vat_usd
	insurance_vat_sar
	insurance_vat_aed
	insurance_vat_egp
	insurance_vat_pkr
refund_fee_vat: STRUCT
	refund_fee_vat_usd
	refund_fee_vat_sar
	refund_fee_vat_aed
	refund_fee_vat_egp
	refund_fee_vat_pkr
exchange_fee_vat: STRUCT
	exchange_fee_vat_usd
	exchange_fee_vat_sar
	exchange_fee_vat_aed
	exchange_fee_vat_egp
	exchange_fee_vat_pkr
booking_fee_vat: STRUCT
	booking_fee_vat_usd
	booking_fee_vat_sar
	booking_fee_vat_aed
	booking_fee_vat_egp
	booking_fee_vat_pkr
payment_fee_vat: STRUCT
	payment_fee_vat_usd
	payment_fee_vat_sar
	payment_fee_vat_aed
	payment_fee_vat_egp
	payment_fee_vat_pkr
baggage_fee_vat: STRUCT
	baggage_fee_usd
	baggage_fee_sar
	baggage_fee_aed
	baggage_fee_egp
	baggage_fee_pkr
meals_fee_vat: STRUCT
	meals_fee_usd
	meals_fee_sar
	meals_fee_aed
	meals_fee_egp
	meals_fee_pkr
flights_seats_markup_vat: STRUCT
	flights_seats_markup_usd
	flights_seats_markup_sar
	flights_seats_markup_aed
	flights_seats_markup_egp
	flights_seats_markup_pkr
*/
left join (SELECT
  a.booking_ref,
  struct(
    a.flights_discount_vat_usd,
    b.flights_discount_vat_sar,
    c.flights_discount_vat_aed,
    d.flights_discount_vat_egp,
    e.flights_discount_vat_pkr
  ) as flights_discount_vat,
  struct(
    a.markup_vat_usd,
    b.markup_vat_sar,
    c.markup_vat_aed,
    d.markup_vat_egp,
    e.markup_vat_pkr
  ) as markup_vat,
  struct(
    a.insurance_vat_usd,
    b.insurance_vat_sar,
    c.insurance_vat_aed,
    d.insurance_vat_egp,
    e.insurance_vat_pkr
  ) as insurance_vat,
  struct(
    a.refund_fee_vat_usd,
    b.refund_fee_vat_sar,
    c.refund_fee_vat_aed,
    d.refund_fee_vat_egp,
    e.refund_fee_vat_pkr
  ) as refund_fee_vat,
  struct(
    a.exchange_fee_vat_usd,
    b.exchange_fee_vat_sar,
    c.exchange_fee_vat_aed,
    d.exchange_fee_vat_egp,
    e.exchange_fee_vat_pkr
  ) as exchange_fee_vat,
  struct(
    a.booking_fee_vat_usd,
    b.booking_fee_vat_sar,
    c.booking_fee_vat_aed,
    d.booking_fee_vat_egp,
    e.booking_fee_vat_pkr
  ) as booking_fee_vat,
  struct(
    a.payment_fee_vat_usd,
    b.payment_fee_vat_sar,
    c.payment_fee_vat_aed,
    d.payment_fee_vat_egp,
    e.payment_fee_vat_pkr
  ) as payment_fee_vat,
  struct(
    a.baggage_fee_usd,
    b.baggage_fee_sar,
    c.baggage_fee_aed,
    d.baggage_fee_egp,
    e.baggage_fee_pkr
  ) as baggage_fee_vat,
  struct(
  	a.meals_fee_usd,
		b.meals_fee_sar,
		c.meals_fee_aed,
		d.meals_fee_egp,
		e.meals_fee_pkr
	) as meals_fee_vat,
	struct(
		a.flights_seats_markup_usd,
		b.flights_seats_markup_sar,
		c.flights_seats_markup_aed,
		d.flights_seats_markup_egp,
		e.flights_seats_markup_pkr
	) as flights_seats_markup_vat
FROM
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_usd,
		first_value(markup_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_usd,
		first_value(insurance_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_usd,
		first_value(refund_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_usd,
		first_value(exchange_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_usd,
		first_value(booking_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_usd,
		first_value(payment_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_usd,
		first_value(baggage_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_usd,
		first_value(meals_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_usd,
		first_value(flights_seats_markup_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_usd,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_usd,
			`Markup` as markup_vat_usd,
			`Insurance` as insurance_vat_usd,
			`Refund Fee` as refund_fee_vat_usd,
			`Exchange Fee` as exchange_fee_vat_usd,
			`Booking Fee` as booking_fee_vat_usd,
			`Payment Fee` as payment_fee_vat_usd,
			`Baggage Fee` as baggage_fee_usd,
			`Meals Fee` as meals_fee_usd,
			`Flights Seats Markup` as flights_seats_markup_usd,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_usd,0)) as tax_amount_usd
				from
				(
					select
						flights_vat_report.*,
						flights_vat_report.tax_amount * exchange_rates.amount as tax_amount_usd
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_usd,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) a
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_sar,
		first_value(markup_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_sar,
		first_value(insurance_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_sar,
		first_value(refund_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_sar,
		first_value(exchange_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_sar,
		first_value(booking_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_sar,
		first_value(payment_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_sar,
		first_value(baggage_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_sar,
		first_value(meals_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_sar,
		first_value(flights_seats_markup_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_sar,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_sar,
			`Markup` as markup_vat_sar,
			`Insurance` as insurance_vat_sar,
			`Refund Fee` as refund_fee_vat_sar,
			`Exchange Fee` as exchange_fee_vat_sar,
			`Booking Fee` as booking_fee_vat_sar,
			`Payment Fee` as payment_fee_vat_sar,
			`Baggage Fee` as baggage_fee_sar,
			`Meals Fee` as meals_fee_sar,
			`Flights Seats Markup` as flights_seats_markup_sar,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_sar,0)) as tax_amount_sar
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_SAR.amount as tax_amount_sar
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_SAR on exchange_rates_SAR.base = 'SAR' and exchange_rates_SAR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_sar,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) b
ON
  a.booking_ref = b.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_aed,
		first_value(markup_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_aed,
		first_value(insurance_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_aed,
		first_value(refund_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_aed,
		first_value(exchange_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_aed,
		first_value(booking_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_aed,
		first_value(payment_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_aed,
		first_value(baggage_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_aed,
		first_value(meals_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_aed,
		first_value(flights_seats_markup_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_aed,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_aed,
			`Markup` as markup_vat_aed,
			`Insurance` as insurance_vat_aed,
			`Refund Fee` as refund_fee_vat_aed,
			`Exchange Fee` as exchange_fee_vat_aed,
			`Booking Fee` as booking_fee_vat_aed,
			`Payment Fee` as payment_fee_vat_aed,
			`Baggage Fee` as baggage_fee_aed,
			`Meals Fee` as meals_fee_aed,
			`Flights Seats Markup` as flights_seats_markup_aed,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_aed,0)) as tax_amount_aed
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_AED.amount as tax_amount_aed
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_AED on exchange_rates_AED.base = 'AED' and exchange_rates_AED.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_aed,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) c
ON
  a.booking_ref = c.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_egp,
		first_value(markup_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_egp,
		first_value(insurance_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_egp,
		first_value(refund_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_egp,
		first_value(exchange_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_egp,
		first_value(booking_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_egp,
		first_value(payment_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_egp,
		first_value(baggage_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_egp,
		first_value(meals_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_egp,
		first_value(flights_seats_markup_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_egp,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_egp,
			`Markup` as markup_vat_egp,
			`Insurance` as insurance_vat_egp,
			`Refund Fee` as refund_fee_vat_egp,
			`Exchange Fee` as exchange_fee_vat_egp,
			`Booking Fee` as booking_fee_vat_egp,
			`Payment Fee` as payment_fee_vat_egp,
			`Baggage Fee` as baggage_fee_egp,
			`Meals Fee` as meals_fee_egp,
			`Flights Seats Markup` as flights_seats_markup_egp,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_egp,0)) as tax_amount_egp
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_EGP.amount as tax_amount_egp
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_EGP on exchange_rates_EGP.base = 'EGP' and exchange_rates_EGP.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_egp,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) d
ON
  a.booking_ref = d.booking_ref
LEFT JOIN
	 (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_pkr,
		first_value(markup_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_pkr,
		first_value(insurance_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_pkr,
		first_value(refund_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_pkr,
		first_value(exchange_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_pkr,
		first_value(booking_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_pkr,
		first_value(payment_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_pkr,
		first_value(baggage_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_pkr,
		first_value(meals_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_pkr,
		first_value(flights_seats_markup_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_pkr,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_pkr,
			`Markup` as markup_vat_pkr,
			`Insurance` as insurance_vat_pkr,
			`Refund Fee` as refund_fee_vat_pkr,
			`Exchange Fee` as exchange_fee_vat_pkr,
			`Booking Fee` as booking_fee_vat_pkr,
			`Payment Fee` as payment_fee_vat_pkr,
			`Baggage Fee` as baggage_fee_pkr,
			`Meals Fee` as meals_fee_pkr,
			`Flights Seats Markup` as flights_seats_markup_pkr,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_pkr,0)) as tax_amount_pkr
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_PKR.amount as tax_amount_pkr
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_PKR on exchange_rates_PKR.base = 'PKR' and exchange_rates_PKR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_pkr,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) e
ON
	a.booking_ref = e.booking_ref) vat on flight_logs_ticketed.booking_ref = vat.booking_ref
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_cancellation_v2_1` AS (
	with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
select distinct
	flight_logs_cancelled.booking_ref,
	flight_logs_cancelled.vendor,
	flight_logs_cancelled.gds,
	flight_logs_cancelled.gds_ref,
  cm.ns_market as market,
	fb.payment_gateway as payment_gateway_provider,
	flight_logs_cancelled.refund_revenue_usd,
	flight_logs_cancelled.refund_fee_usd,
	/* Newly added fields */
	case
		when flight_logs_cancelled.rank_by_ticket_status = 1 /*Refund fee vat will be aggregated no matter how many times refund is initiated*/
			then vat.refund_fee_vat
	end as refund_fee_vat,
	case
		when flight_logs_cancelled.rank_by_ticket_status = 1 /*Refund fee vat will be aggregated no matter how many times refund is initiated*/
			then
				struct(
          
            flight_logs_cancelled.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount as invoice_gov_tax_amount_sar,
            flight_logs_cancelled.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount as invoice_gov_tax_amount_egp,
            flight_logs_cancelled.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount as invoice_gov_tax_amount_pkr,
          
					flight_logs_cancelled.invoice_gov_tax_details.vat_amount as invoice_gov_tax_amount_usd /*Default in USD*/
				)
	end as invoice_gov_tax_amount,
	flight_logs_cancelled.created_time,
	case
    
      WHEN flight_logs_cancelled.site_code IN ('SA', 'EG', 'PK') THEN flight_logs_cancelled.site_code
    
		ELSE 'All Others'
  end as Site_Code,
  flight_logs_cancelled.integration_type,
  flight_logs_cancelled.vendor_commissions_upfront_backend_usd,
	flight_logs_cancelled.vendor_commissions_iata_usd,
	flight_logs_cancelled.vendor_commissions_plb_usd,
	flight_logs_cancelled.gds_segment_fee_usd,
  
    /*AE site code is not supported in this version of the query*/
    IF(flight_logs_cancelled.site_code = 'AE', 1.0, flight_logs_cancelled.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
    IF(flight_logs_cancelled.site_code = 'AE', 'USD', flight_logs_cancelled.other_currency_code) as other_currency_code,
  
	ifnull(flight_logs_cancelled.refund_amount_usd, 0) as user_refund_amount_usd,
	ifnull(flight_logs_cancelled.gds_refund_amount_usd, 0) as gds_refund_amount_usd,
	flight_logs_cancelled.user_fare_refunded_usd,
	flight_logs_cancelled.user_fee_refunded_usd,
	flight_logs_cancelled.refund_method
from (
	SELECT
    * EXCEPT(
			source_product,
			conversions_tracked
		)
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_cancelled_logs_audit`
  WHERE TRUE
    AND source_product="bow"
		AND conversions_tracked = 1
) AS flight_logs_cancelled

  left join exchange_rates as exchange_rates_sar on 'SAR' = exchange_rates_sar.base and date(flight_logs_cancelled.invoice_gov_tax_details.created_at) = date(exchange_rates_sar.effective)
  left join exchange_rates as exchange_rates_egp on 'EGP' = exchange_rates_egp.base and date(flight_logs_cancelled.invoice_gov_tax_details.created_at) = date(exchange_rates_egp.effective)
  left join exchange_rates as exchange_rates_pkr on 'PKR' = exchange_rates_pkr.base and date(flight_logs_cancelled.invoice_gov_tax_details.created_at) = date(exchange_rates_pkr.effective)

left join `wego_analytics.flights_bookings` fb on flight_logs_cancelled.booking_ref = fb.booking_ref
left join `analytics.countries_misc` cm on flight_logs_cancelled.site_code = cm.country_code
/* -- vat --
booking_ref
flights_discount_vat: STRUCT
	flights_discount_vat_usd
	flights_discount_vat_sar
	flights_discount_vat_aed
	flights_discount_vat_egp
	flights_discount_vat_pkr
markup_vat: STRUCT
	markup_vat_usd
	markup_vat_sar
	markup_vat_aed
	markup_vat_egp
	markup_vat_pkr
insurance_vat: STRUCT
	insurance_vat_usd
	insurance_vat_sar
	insurance_vat_aed
	insurance_vat_egp
	insurance_vat_pkr
refund_fee_vat: STRUCT
	refund_fee_vat_usd
	refund_fee_vat_sar
	refund_fee_vat_aed
	refund_fee_vat_egp
	refund_fee_vat_pkr
exchange_fee_vat: STRUCT
	exchange_fee_vat_usd
	exchange_fee_vat_sar
	exchange_fee_vat_aed
	exchange_fee_vat_egp
	exchange_fee_vat_pkr
booking_fee_vat: STRUCT
	booking_fee_vat_usd
	booking_fee_vat_sar
	booking_fee_vat_aed
	booking_fee_vat_egp
	booking_fee_vat_pkr
payment_fee_vat: STRUCT
	payment_fee_vat_usd
	payment_fee_vat_sar
	payment_fee_vat_aed
	payment_fee_vat_egp
	payment_fee_vat_pkr
baggage_fee_vat: STRUCT
	baggage_fee_usd
	baggage_fee_sar
	baggage_fee_aed
	baggage_fee_egp
	baggage_fee_pkr
meals_fee_vat: STRUCT
	meals_fee_usd
	meals_fee_sar
	meals_fee_aed
	meals_fee_egp
	meals_fee_pkr
flights_seats_markup_vat: STRUCT
	flights_seats_markup_usd
	flights_seats_markup_sar
	flights_seats_markup_aed
	flights_seats_markup_egp
	flights_seats_markup_pkr
*/
left join (SELECT
  a.booking_ref,
  struct(
    a.flights_discount_vat_usd,
    b.flights_discount_vat_sar,
    c.flights_discount_vat_aed,
    d.flights_discount_vat_egp,
    e.flights_discount_vat_pkr
  ) as flights_discount_vat,
  struct(
    a.markup_vat_usd,
    b.markup_vat_sar,
    c.markup_vat_aed,
    d.markup_vat_egp,
    e.markup_vat_pkr
  ) as markup_vat,
  struct(
    a.insurance_vat_usd,
    b.insurance_vat_sar,
    c.insurance_vat_aed,
    d.insurance_vat_egp,
    e.insurance_vat_pkr
  ) as insurance_vat,
  struct(
    a.refund_fee_vat_usd,
    b.refund_fee_vat_sar,
    c.refund_fee_vat_aed,
    d.refund_fee_vat_egp,
    e.refund_fee_vat_pkr
  ) as refund_fee_vat,
  struct(
    a.exchange_fee_vat_usd,
    b.exchange_fee_vat_sar,
    c.exchange_fee_vat_aed,
    d.exchange_fee_vat_egp,
    e.exchange_fee_vat_pkr
  ) as exchange_fee_vat,
  struct(
    a.booking_fee_vat_usd,
    b.booking_fee_vat_sar,
    c.booking_fee_vat_aed,
    d.booking_fee_vat_egp,
    e.booking_fee_vat_pkr
  ) as booking_fee_vat,
  struct(
    a.payment_fee_vat_usd,
    b.payment_fee_vat_sar,
    c.payment_fee_vat_aed,
    d.payment_fee_vat_egp,
    e.payment_fee_vat_pkr
  ) as payment_fee_vat,
  struct(
    a.baggage_fee_usd,
    b.baggage_fee_sar,
    c.baggage_fee_aed,
    d.baggage_fee_egp,
    e.baggage_fee_pkr
  ) as baggage_fee_vat,
  struct(
  	a.meals_fee_usd,
		b.meals_fee_sar,
		c.meals_fee_aed,
		d.meals_fee_egp,
		e.meals_fee_pkr
	) as meals_fee_vat,
	struct(
		a.flights_seats_markup_usd,
		b.flights_seats_markup_sar,
		c.flights_seats_markup_aed,
		d.flights_seats_markup_egp,
		e.flights_seats_markup_pkr
	) as flights_seats_markup_vat
FROM
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_usd,
		first_value(markup_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_usd,
		first_value(insurance_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_usd,
		first_value(refund_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_usd,
		first_value(exchange_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_usd,
		first_value(booking_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_usd,
		first_value(payment_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_usd,
		first_value(baggage_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_usd,
		first_value(meals_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_usd,
		first_value(flights_seats_markup_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_usd,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_usd,
			`Markup` as markup_vat_usd,
			`Insurance` as insurance_vat_usd,
			`Refund Fee` as refund_fee_vat_usd,
			`Exchange Fee` as exchange_fee_vat_usd,
			`Booking Fee` as booking_fee_vat_usd,
			`Payment Fee` as payment_fee_vat_usd,
			`Baggage Fee` as baggage_fee_usd,
			`Meals Fee` as meals_fee_usd,
			`Flights Seats Markup` as flights_seats_markup_usd,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_usd,0)) as tax_amount_usd
				from
				(
					select
						flights_vat_report.*,
						flights_vat_report.tax_amount * exchange_rates.amount as tax_amount_usd
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_usd,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) a
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_sar,
		first_value(markup_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_sar,
		first_value(insurance_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_sar,
		first_value(refund_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_sar,
		first_value(exchange_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_sar,
		first_value(booking_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_sar,
		first_value(payment_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_sar,
		first_value(baggage_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_sar,
		first_value(meals_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_sar,
		first_value(flights_seats_markup_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_sar,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_sar,
			`Markup` as markup_vat_sar,
			`Insurance` as insurance_vat_sar,
			`Refund Fee` as refund_fee_vat_sar,
			`Exchange Fee` as exchange_fee_vat_sar,
			`Booking Fee` as booking_fee_vat_sar,
			`Payment Fee` as payment_fee_vat_sar,
			`Baggage Fee` as baggage_fee_sar,
			`Meals Fee` as meals_fee_sar,
			`Flights Seats Markup` as flights_seats_markup_sar,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_sar,0)) as tax_amount_sar
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_SAR.amount as tax_amount_sar
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_SAR on exchange_rates_SAR.base = 'SAR' and exchange_rates_SAR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_sar,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) b
ON
  a.booking_ref = b.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_aed,
		first_value(markup_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_aed,
		first_value(insurance_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_aed,
		first_value(refund_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_aed,
		first_value(exchange_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_aed,
		first_value(booking_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_aed,
		first_value(payment_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_aed,
		first_value(baggage_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_aed,
		first_value(meals_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_aed,
		first_value(flights_seats_markup_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_aed,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_aed,
			`Markup` as markup_vat_aed,
			`Insurance` as insurance_vat_aed,
			`Refund Fee` as refund_fee_vat_aed,
			`Exchange Fee` as exchange_fee_vat_aed,
			`Booking Fee` as booking_fee_vat_aed,
			`Payment Fee` as payment_fee_vat_aed,
			`Baggage Fee` as baggage_fee_aed,
			`Meals Fee` as meals_fee_aed,
			`Flights Seats Markup` as flights_seats_markup_aed,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_aed,0)) as tax_amount_aed
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_AED.amount as tax_amount_aed
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_AED on exchange_rates_AED.base = 'AED' and exchange_rates_AED.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_aed,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) c
ON
  a.booking_ref = c.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_egp,
		first_value(markup_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_egp,
		first_value(insurance_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_egp,
		first_value(refund_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_egp,
		first_value(exchange_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_egp,
		first_value(booking_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_egp,
		first_value(payment_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_egp,
		first_value(baggage_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_egp,
		first_value(meals_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_egp,
		first_value(flights_seats_markup_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_egp,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_egp,
			`Markup` as markup_vat_egp,
			`Insurance` as insurance_vat_egp,
			`Refund Fee` as refund_fee_vat_egp,
			`Exchange Fee` as exchange_fee_vat_egp,
			`Booking Fee` as booking_fee_vat_egp,
			`Payment Fee` as payment_fee_vat_egp,
			`Baggage Fee` as baggage_fee_egp,
			`Meals Fee` as meals_fee_egp,
			`Flights Seats Markup` as flights_seats_markup_egp,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_egp,0)) as tax_amount_egp
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_EGP.amount as tax_amount_egp
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_EGP on exchange_rates_EGP.base = 'EGP' and exchange_rates_EGP.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_egp,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) d
ON
  a.booking_ref = d.booking_ref
LEFT JOIN
	 (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_pkr,
		first_value(markup_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_pkr,
		first_value(insurance_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_pkr,
		first_value(refund_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_pkr,
		first_value(exchange_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_pkr,
		first_value(booking_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_pkr,
		first_value(payment_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_pkr,
		first_value(baggage_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_pkr,
		first_value(meals_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_pkr,
		first_value(flights_seats_markup_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_pkr,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_pkr,
			`Markup` as markup_vat_pkr,
			`Insurance` as insurance_vat_pkr,
			`Refund Fee` as refund_fee_vat_pkr,
			`Exchange Fee` as exchange_fee_vat_pkr,
			`Booking Fee` as booking_fee_vat_pkr,
			`Payment Fee` as payment_fee_vat_pkr,
			`Baggage Fee` as baggage_fee_pkr,
			`Meals Fee` as meals_fee_pkr,
			`Flights Seats Markup` as flights_seats_markup_pkr,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_pkr,0)) as tax_amount_pkr
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_PKR.amount as tax_amount_pkr
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_PKR on exchange_rates_PKR.base = 'PKR' and exchange_rates_PKR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_pkr,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) e
ON
	a.booking_ref = e.booking_ref) vat on flight_logs_cancelled.booking_ref = vat.booking_ref
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_exchange_v2_1` AS (
	with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
select distinct
	flight_logs_exchanged_1.booking_ref,
  cm.ns_market as market,
  flight_logs_exchanged_1.payment_gateway as payment_gateway_provider,
  flight_logs_exchanged_1.vendor,
	flight_logs_exchanged_1.gds,
	flight_logs_exchanged_1.gds_ref,
  flight_logs_exchanged_1.vendor_total_amount_usd,
  flight_logs_exchanged_1.exchange_admin_fee_usd,
  flight_logs_exchanged_1.change_fee_usd,
	flight_logs_exchanged_1.payment_fee_amount_usd,
	/* Newly added fields */
	case
		when flight_logs_exchanged_1.rank_by_ticket_status = 1 /*Exchange fee vat will be aggregated no matter how many times tickets are exchanged*/
			then vat.exchange_fee_vat
	end as exchange_fee_vat,
	case
		when flight_logs_exchanged_1.rank_by_ticket_status = 1 /*Exchange fee vat will be aggregated no matter how many times tickets are exchanged*/
			then
				struct(
          
            flight_logs_exchanged_1.invoice_gov_tax_details.vat_amount / exchange_rates_sar.amount as invoice_gov_tax_amount_sar,
            flight_logs_exchanged_1.invoice_gov_tax_details.vat_amount / exchange_rates_egp.amount as invoice_gov_tax_amount_egp,
            flight_logs_exchanged_1.invoice_gov_tax_details.vat_amount / exchange_rates_pkr.amount as invoice_gov_tax_amount_pkr,
          
					flight_logs_exchanged_1.invoice_gov_tax_details.vat_amount as invoice_gov_tax_amount_usd /*Default in USD*/
				)
	end as invoice_gov_tax_amount, /*Create invoice gov tax amount in SAR, AED, EGP, PKR and USD*/
	flight_logs_exchanged_1.created_time,
	case
    
      when flight_logs_exchanged_1.site_code IN ('SA', 'EG', 'PK') then flight_logs_exchanged_1.site_code
    
		else 'All Others'
  end as Site_Code,
  
    /*AE site code is not supported in this version of the query*/
    IF(flight_logs_exchanged_1.site_code = 'AE', 1.0, flight_logs_exchanged_1.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
    IF(flight_logs_exchanged_1.site_code = 'AE', 'USD', flight_logs_exchanged_1.other_currency_code) as other_currency_code,
  
from
/*-- flights_bookings_exchanged_logs_audit --
booking_ref
vendor
gds
site_code
payment_gateway
vendor_total_amount_usd
exchange_admin_fee_usd
service_fee_usd
payment_fee_amount_usd
created_time
exchange_rate_to_usd_from_other_currency_code
other_currency_code
*/
(
	select
		flight_logs_exchanged.booking_ref,
		flight_logs_exchanged.vendor,
		flight_logs_exchanged.gds,
		flight_logs_exchanged.gds_ref,
    fb.site_code,
		partners.payment_gateway,
		flight_logs_exchanged.vendor_total_amount_usd,
    flight_logs_exchanged.exchange_admin_fee_usd,
    flight_logs_exchanged.change_fee_usd,
		flight_logs_exchanged.payment_fee_amount_usd,
		/* Newly added fields */
		flight_logs_exchanged.rank_by_ticket_status,
		flight_logs_exchanged.invoice_gov_tax_details,
		flight_logs_exchanged.created_time,
		flight_logs_exchanged.exchange_rate_to_usd_from_other_currency_code,
		flight_logs_exchanged.other_currency_code
	from (
		SELECT
			* EXCEPT(
				source_product,
				conversions_tracked
			)
		FROM
			`wego-cloud.aaaaa_temporary_export_folder.flights_bookings_exchanged_logs_audit`
		WHERE TRUE
			AND source_product="bow"
			AND conversions_tracked = 1
	) AS flight_logs_exchanged
	left join `wego_analytics.flights_bookings` fb on flight_logs_exchanged.booking_ref = fb.booking_ref
	left join `payments.payments` payments on flight_logs_exchanged.payment_ref_id = payments.payment_ref
	left join (
		SELECT
			id as partner_id,
			ANY_VALUE(name) as payment_gateway,
		FROM `payments.partners`
		GROUP BY 1
	) partners on payments.partner_id = partners.partner_id
) flight_logs_exchanged_1
left join `analytics.countries_misc` cm on flight_logs_exchanged_1.site_code = cm.country_code

  left join exchange_rates as exchange_rates_sar on 'SAR' = exchange_rates_sar.base and date(flight_logs_exchanged_1.invoice_gov_tax_details.created_at) = date(exchange_rates_sar.effective)
  left join exchange_rates as exchange_rates_egp on 'EGP' = exchange_rates_egp.base and date(flight_logs_exchanged_1.invoice_gov_tax_details.created_at) = date(exchange_rates_egp.effective)
  left join exchange_rates as exchange_rates_pkr on 'PKR' = exchange_rates_pkr.base and date(flight_logs_exchanged_1.invoice_gov_tax_details.created_at) = date(exchange_rates_pkr.effective)

/* -- vat --
booking_ref
flights_discount_vat: STRUCT
	flights_discount_vat_usd
	flights_discount_vat_sar
	flights_discount_vat_aed
	flights_discount_vat_egp
	flights_discount_vat_pkr
markup_vat: STRUCT
	markup_vat_usd
	markup_vat_sar
	markup_vat_aed
	markup_vat_egp
	markup_vat_pkr
insurance_vat: STRUCT
	insurance_vat_usd
	insurance_vat_sar
	insurance_vat_aed
	insurance_vat_egp
	insurance_vat_pkr
refund_fee_vat: STRUCT
	refund_fee_vat_usd
	refund_fee_vat_sar
	refund_fee_vat_aed
	refund_fee_vat_egp
	refund_fee_vat_pkr
exchange_fee_vat: STRUCT
	exchange_fee_vat_usd
	exchange_fee_vat_sar
	exchange_fee_vat_aed
	exchange_fee_vat_egp
	exchange_fee_vat_pkr
booking_fee_vat: STRUCT
	booking_fee_vat_usd
	booking_fee_vat_sar
	booking_fee_vat_aed
	booking_fee_vat_egp
	booking_fee_vat_pkr
payment_fee_vat: STRUCT
	payment_fee_vat_usd
	payment_fee_vat_sar
	payment_fee_vat_aed
	payment_fee_vat_egp
	payment_fee_vat_pkr
baggage_fee_vat: STRUCT
	baggage_fee_usd
	baggage_fee_sar
	baggage_fee_aed
	baggage_fee_egp
	baggage_fee_pkr
meals_fee_vat: STRUCT
	meals_fee_usd
	meals_fee_sar
	meals_fee_aed
	meals_fee_egp
	meals_fee_pkr
flights_seats_markup_vat: STRUCT
	flights_seats_markup_usd
	flights_seats_markup_sar
	flights_seats_markup_aed
	flights_seats_markup_egp
	flights_seats_markup_pkr
*/
left join (SELECT
  a.booking_ref,
  struct(
    a.flights_discount_vat_usd,
    b.flights_discount_vat_sar,
    c.flights_discount_vat_aed,
    d.flights_discount_vat_egp,
    e.flights_discount_vat_pkr
  ) as flights_discount_vat,
  struct(
    a.markup_vat_usd,
    b.markup_vat_sar,
    c.markup_vat_aed,
    d.markup_vat_egp,
    e.markup_vat_pkr
  ) as markup_vat,
  struct(
    a.insurance_vat_usd,
    b.insurance_vat_sar,
    c.insurance_vat_aed,
    d.insurance_vat_egp,
    e.insurance_vat_pkr
  ) as insurance_vat,
  struct(
    a.refund_fee_vat_usd,
    b.refund_fee_vat_sar,
    c.refund_fee_vat_aed,
    d.refund_fee_vat_egp,
    e.refund_fee_vat_pkr
  ) as refund_fee_vat,
  struct(
    a.exchange_fee_vat_usd,
    b.exchange_fee_vat_sar,
    c.exchange_fee_vat_aed,
    d.exchange_fee_vat_egp,
    e.exchange_fee_vat_pkr
  ) as exchange_fee_vat,
  struct(
    a.booking_fee_vat_usd,
    b.booking_fee_vat_sar,
    c.booking_fee_vat_aed,
    d.booking_fee_vat_egp,
    e.booking_fee_vat_pkr
  ) as booking_fee_vat,
  struct(
    a.payment_fee_vat_usd,
    b.payment_fee_vat_sar,
    c.payment_fee_vat_aed,
    d.payment_fee_vat_egp,
    e.payment_fee_vat_pkr
  ) as payment_fee_vat,
  struct(
    a.baggage_fee_usd,
    b.baggage_fee_sar,
    c.baggage_fee_aed,
    d.baggage_fee_egp,
    e.baggage_fee_pkr
  ) as baggage_fee_vat,
  struct(
  	a.meals_fee_usd,
		b.meals_fee_sar,
		c.meals_fee_aed,
		d.meals_fee_egp,
		e.meals_fee_pkr
	) as meals_fee_vat,
	struct(
		a.flights_seats_markup_usd,
		b.flights_seats_markup_sar,
		c.flights_seats_markup_aed,
		d.flights_seats_markup_egp,
		e.flights_seats_markup_pkr
	) as flights_seats_markup_vat
FROM
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_usd,
		first_value(markup_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_usd,
		first_value(insurance_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_usd,
		first_value(refund_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_usd,
		first_value(exchange_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_usd,
		first_value(booking_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_usd,
		first_value(payment_fee_vat_usd ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_usd,
		first_value(baggage_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_usd,
		first_value(meals_fee_usd ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_usd,
		first_value(flights_seats_markup_usd ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_usd,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_usd,
			`Markup` as markup_vat_usd,
			`Insurance` as insurance_vat_usd,
			`Refund Fee` as refund_fee_vat_usd,
			`Exchange Fee` as exchange_fee_vat_usd,
			`Booking Fee` as booking_fee_vat_usd,
			`Payment Fee` as payment_fee_vat_usd,
			`Baggage Fee` as baggage_fee_usd,
			`Meals Fee` as meals_fee_usd,
			`Flights Seats Markup` as flights_seats_markup_usd,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_usd,0)) as tax_amount_usd
				from
				(
					select
						flights_vat_report.*,
						flights_vat_report.tax_amount * exchange_rates.amount as tax_amount_usd
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_usd,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) a
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_sar,
		first_value(markup_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_sar,
		first_value(insurance_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_sar,
		first_value(refund_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_sar,
		first_value(exchange_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_sar,
		first_value(booking_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_sar,
		first_value(payment_fee_vat_sar ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_sar,
		first_value(baggage_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_sar,
		first_value(meals_fee_sar ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_sar,
		first_value(flights_seats_markup_sar ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_sar,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_sar,
			`Markup` as markup_vat_sar,
			`Insurance` as insurance_vat_sar,
			`Refund Fee` as refund_fee_vat_sar,
			`Exchange Fee` as exchange_fee_vat_sar,
			`Booking Fee` as booking_fee_vat_sar,
			`Payment Fee` as payment_fee_vat_sar,
			`Baggage Fee` as baggage_fee_sar,
			`Meals Fee` as meals_fee_sar,
			`Flights Seats Markup` as flights_seats_markup_sar,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_sar,0)) as tax_amount_sar
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_SAR.amount as tax_amount_sar
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_SAR on exchange_rates_SAR.base = 'SAR' and exchange_rates_SAR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_sar,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) b
ON
  a.booking_ref = b.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_aed,
		first_value(markup_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_aed,
		first_value(insurance_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_aed,
		first_value(refund_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_aed,
		first_value(exchange_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_aed,
		first_value(booking_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_aed,
		first_value(payment_fee_vat_aed ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_aed,
		first_value(baggage_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_aed,
		first_value(meals_fee_aed ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_aed,
		first_value(flights_seats_markup_aed ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_aed,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_aed,
			`Markup` as markup_vat_aed,
			`Insurance` as insurance_vat_aed,
			`Refund Fee` as refund_fee_vat_aed,
			`Exchange Fee` as exchange_fee_vat_aed,
			`Booking Fee` as booking_fee_vat_aed,
			`Payment Fee` as payment_fee_vat_aed,
			`Baggage Fee` as baggage_fee_aed,
			`Meals Fee` as meals_fee_aed,
			`Flights Seats Markup` as flights_seats_markup_aed,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_aed,0)) as tax_amount_aed
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_AED.amount as tax_amount_aed
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_AED on exchange_rates_AED.base = 'AED' and exchange_rates_AED.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_aed,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) c
ON
  a.booking_ref = c.booking_ref
LEFT JOIN
   (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_egp,
		first_value(markup_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_egp,
		first_value(insurance_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_egp,
		first_value(refund_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_egp,
		first_value(exchange_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_egp,
		first_value(booking_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_egp,
		first_value(payment_fee_vat_egp ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_egp,
		first_value(baggage_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_egp,
		first_value(meals_fee_egp ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_egp,
		first_value(flights_seats_markup_egp ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_egp,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_egp,
			`Markup` as markup_vat_egp,
			`Insurance` as insurance_vat_egp,
			`Refund Fee` as refund_fee_vat_egp,
			`Exchange Fee` as exchange_fee_vat_egp,
			`Booking Fee` as booking_fee_vat_egp,
			`Payment Fee` as payment_fee_vat_egp,
			`Baggage Fee` as baggage_fee_egp,
			`Meals Fee` as meals_fee_egp,
			`Flights Seats Markup` as flights_seats_markup_egp,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_egp,0)) as tax_amount_egp
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_EGP.amount as tax_amount_egp
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_EGP on exchange_rates_EGP.base = 'EGP' and exchange_rates_EGP.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_egp,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) d
ON
  a.booking_ref = d.booking_ref
LEFT JOIN
	 (with exchange_rates as (
	select
		base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
  from `analytics.exchange_rates*`
  where PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE('2024-12-11') and quote = 'USD'
)
,t as (
	select
		any_value(booking_ref) over (partition by booking_ref order by 1 desc) as booking_ref,
		first_value(flights_discount_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_discount_vat_pkr,
		first_value(markup_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as markup_vat_pkr,
		first_value(insurance_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as insurance_vat_pkr,
		first_value(refund_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as refund_fee_vat_pkr,
		first_value(exchange_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as exchange_fee_vat_pkr,
		first_value(booking_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as booking_fee_vat_pkr,
		first_value(payment_fee_vat_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as payment_fee_vat_pkr,
		first_value(baggage_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as baggage_fee_pkr,
		first_value(meals_fee_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as meals_fee_pkr,
		first_value(flights_seats_markup_pkr ignore nulls) over (partition by booking_ref order by 1 desc) as flights_seats_markup_pkr,
		row_number() over (partition by booking_ref order by 1 desc) as rn
	from
	(
		select
			booking_ref,
			`Flights Discount` as flights_discount_vat_pkr,
			`Markup` as markup_vat_pkr,
			`Insurance` as insurance_vat_pkr,
			`Refund Fee` as refund_fee_vat_pkr,
			`Exchange Fee` as exchange_fee_vat_pkr,
			`Booking Fee` as booking_fee_vat_pkr,
			`Payment Fee` as payment_fee_vat_pkr,
			`Baggage Fee` as baggage_fee_pkr,
			`Meals Fee` as meals_fee_pkr,
			`Flights Seats Markup` as flights_seats_markup_pkr,
		from
		(
			select * from
			(
				select
					split(transaction_no, '-')[safe_offset(0)] as booking_ref,
					memo,
					sum(ifnull(tax_amount_pkr,0)) as tax_amount_pkr
				from
				(
					select
						flights_vat_report.*,
						(flights_vat_report.tax_amount * exchange_rates.amount) / exchange_rates_PKR.amount as tax_amount_pkr
					from
					`wego-cloud.aaaaa_temporary_export_folder.flights_vat_report` as flights_vat_report
					left join exchange_rates on exchange_rates.base = flights_vat_report.local_currency and exchange_rates.effective = date(flights_vat_report.transaction_date)
					left join exchange_rates as exchange_rates_PKR on exchange_rates_PKR.base = 'PKR' and exchange_rates_PKR.effective = date(flights_vat_report.transaction_date)
				)
				group by 1,2
			)
			pivot (
				sum(ifnull(tax_amount_pkr,0)) for memo in (
					'Flights Discount',
					'Markup',
					'Insurance',
					'Refund Fee',
					'Exchange Fee',
					'Booking Fee',
					'Payment Fee',
					'Baggage Fee',
					'Meals Fee',
					'Flights Seats Markup'
				)
			)
		)
	)
) select * except(rn) from t where rn = 1) e
ON
	a.booking_ref = e.booking_ref) vat on flight_logs_exchanged_1.booking_ref = vat.booking_ref
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_booking_v2_1` AS (
	select
	flight_logs_ticketed.booking_ref,
	flight_logs_ticketed.booking_id,
	flight_logs_ticketed.vendor,
	flight_logs_ticketed.gds,
	flight_logs_ticketed.gds_ref,
	cm.ns_market as market,
	flight_logs_ticketed.vendor_total_amount_usd,
	/*
  * In the flights_bookings_ticketed_logs_audit, we will have multiple rows for the same booking_ref if the booking is Sum of One Way
  * We want to get the est_vendor_issuance_fee_usd only for the first row of the results as the join with the flights_bookings table
  * will give us multiple rows for the same booking_ref if the booking is Sum of One Way
  */
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then fb.est_vendor_issuance_fee_usd
	end as est_vendor_issuance_fee_usd,
	fb.payment_gateway as payment_gateway_provider,
	/* Newly added fields */
	flight_logs_ticketed.created_time,
	CASE
    
      WHEN flight_logs_ticketed.site_code IN ('SA', 'EG', 'PK') THEN flight_logs_ticketed.site_code
    
		ELSE 'All Others'
  END AS Site_Code,
  
      /*AE site code is not supported in this version of the query*/
      IF(flight_logs_ticketed.site_code = 'AE', 1.0, flight_logs_ticketed.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
			IF(flight_logs_ticketed.site_code = 'AE', 'USD', flight_logs_ticketed.other_currency_code) as other_currency_code,
  
	flight_logs_ticketed.optimize_cost_usd,
  /*
  * In the flights_bookings_ticketed_logs_audit, we will have multiple rows for the same booking_ref if the booking is Sum of One Way
  * We want to get the vendor_total_amount_usd only for the first row of the results as the join with the flights_ancillary_ota_logs table
  * will give us multiple rows for the same booking_ref if the booking is Sum of One Way
  */
	case
		when flight_logs_ticketed.is_soonest_itinerary_soo is true or flight_logs_ticketed.is_soonest_itinerary_soo is null
			then flights_ancillary_ota_logs.vendor_total_amount_usd
	end as ancillary_vendor_total_amount_usd,
from (
  SELECT
    * EXCEPT(
      source_product,
      conversions_tracked
    )
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_ticketed_logs_audit`
  WHERE TRUE
    AND source_product="bow"
    AND conversions_tracked = 1
 ) AS flight_logs_ticketed
left join `wego-cloud.aaaaa_temporary_export_folder.flights_ancillary_ota_logs` flights_ancillary_ota_logs on flight_logs_ticketed.booking_ref = flights_ancillary_ota_logs.booking_ref
left join `wego_analytics.flights_bookings` fb on flight_logs_ticketed.booking_ref = fb.booking_ref
left join `analytics.countries_misc` cm on flight_logs_ticketed.site_code = cm.country_code
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_insurance_v2_1` AS (
	with exchange_rates as (
	SELECT
	  base,
	  amount,
	  CAST(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS DATE) AS effective
	FROM `analytics.exchange_rates*`
	WHERE PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND quote = 'USD'
)
select
	booking_ref,
	fi.gds_ref,
	supplier,
  CASE
    
      WHEN site_code IN ('SA', 'EG', 'PK') THEN site_code
    
		ELSE 'All Others'
	END AS Site_Code,
	created_at,
	cm.ns_market as market,
	total_price_usd as insurance_total_amount_usd,
	wego_commission_usd as wego_commission_usd,
	/*Start of generating a currency conversion using the site code for Netsuite integration*/
  CASE
    
      WHEN site_code = 'SA' THEN erSAR.amount
      WHEN site_code = 'EG' THEN erEGP.amount
      WHEN site_code = 'PK' THEN erPKR.amount
      ELSE 1.0
    
  END AS exchange_rate_to_usd_from_other_currency_code,
  CASE
    
      WHEN site_code = 'SA' THEN 'SAR'
      WHEN site_code = 'EG' THEN 'EGP'
      WHEN site_code = 'PK' THEN 'PKR'
      ELSE 'USD'
    
  END AS other_currency_code,
  /*End of generating a currency conversion using the site code for Netsuite integration*/
from `wego_analytics.flights_insurance` fi
left join
(
	select booking_ref, conversions_tracked
	from `wego_analytics.flights_bookings`
) fb using(booking_ref)
left join `analytics.countries_misc` cm on fi.site_code = cm.country_code
/*Start of joining exchange rate for Netsuite integration*/

  LEFT JOIN exchange_rates AS erSAR ON erSAR.base = 'SAR' AND DATE(erSAR.effective) = DATE(created_at)
  LEFT JOIN exchange_rates AS erEGP ON erEGP.base = 'EGP' AND DATE(erEGP.effective) = DATE(created_at)
  LEFT JOIN exchange_rates AS erPKR ON erPKR.base = 'PKR' AND DATE(erPKR.effective) = DATE(created_at)

/*End of joining exchange rate for Netsuite integration*/
WHERE (fi.conversions_tracked = 1 or fi.conversions_tracked = 0) and fb.conversions_tracked = 1
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_cancellation_v2_1` AS (
	select
	flight_logs_cancelled.booking_ref,
	flight_logs_cancelled.vendor,
	flight_logs_cancelled.gds,
	flight_logs_cancelled.gds_ref,
  cm.ns_market as market,
  CASE
    
      WHEN flight_logs_cancelled.site_code IN ('SA', 'EG', 'PK') THEN flight_logs_cancelled.site_code
    
		ELSE 'All Others'
  END AS Site_Code,
  
    /*AE site code is not supported in this version of the query*/
    IF(flight_logs_cancelled.site_code = 'AE', 1.0, flight_logs_cancelled.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
    IF(flight_logs_cancelled.site_code = 'AE', 'USD', flight_logs_cancelled.other_currency_code) as other_currency_code,
  
	fb.payment_gateway as payment_gateway_provider,
	flight_logs_cancelled.vendor_total_amount_usd,
  flight_logs_cancelled.refund_amount_usd as user_refund_amount_usd,
  flight_logs_cancelled.gds_refund_amount_usd,
  /*
  * In the flights_bookings_cancelled_logs_audit, we will have multiple rows for the same booking_ref if the booking is refunded multiple times
  * We want to get the est_vendor_refund_fee_usd only for the first row of the results as the join with the flights_bookings table
  * will give us multiple rows for the same booking_ref if the booking is refunded multiple times
  */
  IF(
    ROW_NUMBER() OVER (PARTITION BY flight_logs_cancelled.booking_ref ORDER BY flight_logs_cancelled.created_time ASC) = 1,
    fb.est_vendor_refund_fee_usd,
    NULL
  ) AS est_vendor_refund_fee_usd,
  /* Newly added fields */
	flight_logs_cancelled.created_time,
	/*
  * In the flights_bookings_cancelled_logs_audit, we will have multiple rows for the same booking_ref if the booking is refunded multiple times
  * We want to get the est_vendor_issuance_fee_usd only for the first row of the results as the join with the flights_bookings table
  * will give us multiple rows for the same booking_ref if the booking is refunded multiple times
  */
	IF(
		ROW_NUMBER() OVER (PARTITION BY flight_logs_cancelled.booking_ref ORDER BY flight_logs_cancelled.created_time ASC) = 1,
    fb.est_vendor_issuance_fee_usd,
    NULL
  ) AS est_vendor_issuance_fee_usd
from (
  SELECT
    * EXCEPT(
      source_product,
      conversions_tracked
    )
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_cancelled_logs_audit`
  WHERE TRUE
    AND source_product="bow"
    AND conversions_tracked = 1
) AS flight_logs_cancelled
left join `wego_analytics.flights_bookings` fb on flight_logs_cancelled.booking_ref = fb.booking_ref
left join `analytics.countries_misc` cm on fb.site_code = cm.country_code
);
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_exchange_v2_1` AS (
	select
	flight_logs_exchanged.booking_ref,
	flight_logs_exchanged.booking_id,
	flight_logs_exchanged.vendor,
	flight_logs_exchanged.gds,
	flight_logs_exchanged.gds_ref,
	cm.ns_market as market,
	flight_logs_exchanged.vendor_total_amount_usd,
	/*
	* In the flights_bookings_exchanged_logs_audit, we will have multiple rows for the same booking_ref if the booking is exchanged multiple times
	* We want to get the est_vendor_exchange_fee_usd only for the first row of the results as the join with the flights_bookings table
	* will give us multiple rows for the same booking_ref if the booking is exchanged multiple times
	*/
	IF(
	  ROW_NUMBER() OVER (PARTITION BY flight_logs_exchanged.booking_ref ORDER BY flight_logs_exchanged.created_time ASC) = 1,
	  fb.est_vendor_exchange_fee_usd,
	  NULL
	) AS est_vendor_exchange_fee_usd,
	IF(
	  ROW_NUMBER() OVER (PARTITION BY flight_logs_exchanged.booking_ref ORDER BY flight_logs_exchanged.created_time ASC) = 1,
	  fb.est_vendor_issuance_fee_usd,
	  NULL
	) AS est_vendor_issuance_fee_usd,
	fb.payment_gateway as payment_gateway_provider,
	/* Newly added fields */
	flight_logs_exchanged.optimize_cost_usd,
	flight_logs_exchanged.created_time,
	CASE
    
      WHEN flight_logs_exchanged.site_code IN ('SA', 'EG', 'PK') THEN flight_logs_exchanged.site_code
    
		ELSE 'All Others'
  END AS Site_Code,
  
    /*AE site code is not supported in this version of the query*/
    IF(flight_logs_exchanged.site_code = 'AE', 1.0, flight_logs_exchanged.exchange_rate_to_usd_from_other_currency_code) as exchange_rate_to_usd_from_other_currency_code,
    IF(flight_logs_exchanged.site_code = 'AE', 'USD', flight_logs_exchanged.other_currency_code) as other_currency_code,
  
	flight_logs_exchanged.change_fee_usd
from (
  SELECT
    * EXCEPT(
      source_product,
      conversions_tracked
    )
  FROM
    `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_exchanged_logs_audit`
  WHERE TRUE
    AND source_product="bow"
    AND conversions_tracked = 1
) AS flight_logs_exchanged
left join `wego_analytics.flights_bookings` fb on flight_logs_exchanged.booking_ref = fb.booking_ref
left join `analytics.countries_misc` cm on flight_logs_exchanged.site_code = cm.country_code
);
-- ============================================================
-- GMV Booking Details
-- Source: gmv_flights_booking
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_booking_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Sales_GMV_OTA_Flights components
        markup                                                  AS `Markup`,
        markdown                                                AS `Markdown`,
        vendor_total_amount_usd                                 AS `Vendor Total Amount USD`,
        ancillary_vendor_total_amount_usd                       AS `Ancillary Vendor Total Amount USD`,
        ancillary_margin_amount_usd                             AS `Ancillary Margin Amount USD`,
        vendor_commissions_plb_usd                              AS `Vendor Commissions PLB USD`,
        vendor_commissions_iata_usd                             AS `Vendor Commissions IATA USD`,
        vendor_commissions_upfront_backend_usd                  AS `Vendor Commissions Upfront Backend USD`,
        gds_segment_fee_usd                                     AS `GDS Segment Fee USD`,

        -- Service_Fee_GMV_OTA_Flights components
        payment_fee_usd                                         AS `Payment Fee USD`,
        booking_fee_usd                                         AS `Booking Fee USD`,

        -- Insurance_GMV_OTA_Flights component
        insurance_total_amount_usd                              AS `Insurance Total Amount USD`,

        -- Discount_GMV_OTA_Flights component
        promo_discount_amount_usd                               AS `Promo Discount Amount USD`,

        -- VAT_Output components
        invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_pkr       AS `Invoice Gov Tax Amount PKR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

    FROM `aaaaa_temporary_export_folder.gmv_flights_booking_v2_1`
);

-- ============================================================
-- GMV Cancellation Details
-- Source: gmv_flights_cancellation
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_cancellation_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Sales_GMV_OTA_Flights component
        user_refund_amount_usd                                  AS `User Refund Amount USD`,
        vendor_commissions_plb_usd                              AS `Vendor Commissions PLB USD`,
        vendor_commissions_iata_usd                             AS `Vendor Commissions IATA USD`,
        vendor_commissions_upfront_backend_usd                  AS `Vendor Commissions Upfront Backend USD`,
        gds_segment_fee_usd                                     AS `GDS Segment Fee USD`,

        -- Service_Fee_GMV_OTA_Flights components
        refund_fee_usd                                          AS `Refund Fee USD`,

        -- VAT_Output components
        invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_pkr       AS `Invoice Gov Tax Amount PKR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

    FROM `aaaaa_temporary_export_folder.gmv_flights_cancellation_v2_1`
);

-- ============================================================
-- GMV Exchange Details
-- Source: gmv_flights_exchange
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.gmv_flights_exchange_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Sales_GMV_OTA_Flights components
        vendor_total_amount_usd                                 AS `Vendor Total Amount USD`,
        change_fee_usd                                          AS `Change Fee USD`,

        -- Service_Fee_GMV_OTA_Flights components
        exchange_admin_fee_usd                                  AS `Exchange Admin Fee USD`,
        payment_fee_amount_usd                                  AS `Payment Fee Amount USD`,

        -- VAT_Output components
        invoice_gov_tax_amount.invoice_gov_tax_amount_sar       AS `Invoice Gov Tax Amount SAR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_egp       AS `Invoice Gov Tax Amount EGP`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_pkr       AS `Invoice Gov Tax Amount PKR`,
        invoice_gov_tax_amount.invoice_gov_tax_amount_usd       AS `Invoice Gov Tax Amount USD`

    FROM `aaaaa_temporary_export_folder.gmv_flights_exchange_v2_1`
);

-- ============================================================
-- COGS Booking Details
-- Source: cogs_flights_booking
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_booking_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        vendor                                                  AS `Vendor`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Flights_Supplier_Costs_COGS_OTA_Flights components
        vendor_total_amount_usd                                 AS `Vendor Total Amount USD`,
        optimize_cost_usd                                       AS `Optimize Cost USD`,
        est_vendor_issuance_fee_usd                             AS `Est Vendor Issuance Fee USD`,
        ancillary_vendor_total_amount_usd                       AS `Ancillary Vendor Total Amount USD`

    FROM `aaaaa_temporary_export_folder.cogs_flights_booking_v2_1`
);

-- ============================================================
-- COGS Insurance Details
-- Source: cogs_flights_insurance
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_insurance_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        supplier                                                AS `Supplier`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_at                                              AS `Created At`,

        -- Insurance_Costs_COGS_OTA_Flights components
        insurance_total_amount_usd                              AS `Insurance Total Amount USD`,
        wego_commission_usd                                     AS `Wego Commission USD`

    FROM `aaaaa_temporary_export_folder.cogs_flights_insurance_v2_1`
);

-- ============================================================
-- COGS Cancellation Details
-- Source: cogs_flights_cancellation
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_cancellation_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        vendor                                                  AS `Vendor`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Flights_Supplier_Costs_COGS_OTA_Flights components
        gds_refund_amount_usd                                   AS `GDS Refund Amount USD`,
        est_vendor_issuance_fee_usd                             AS `Est Vendor Issuance Fee USD`

    FROM `aaaaa_temporary_export_folder.cogs_flights_cancellation_v2_1`
);

-- ============================================================
-- COGS Exchange Details
-- Source: cogs_flights_exchange
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.cogs_flights_exchange_details` AS (
    SELECT
        booking_ref                                             AS `Booking Ref`,
        gds_ref                                                 AS `GDS Ref`,
        Site_Code                                               AS `Site Code`,
        market                                                  AS `Market`,
        other_currency_code                                     AS `Other Currency Code`,
        vendor                                                  AS `Vendor`,
        exchange_rate_to_usd_from_other_currency_code           AS `Exchange Rate To USD From Other Currency Code`,
        created_time                                            AS `Created Time`,

        -- Flights_Supplier_Costs_COGS_OTA_Flights components
        vendor_total_amount_usd                                 AS `Vendor Total Amount USD`,
        est_vendor_issuance_fee_usd                             AS `Est Vendor Issuance Fee USD`,
        optimize_cost_usd                                       AS `Optimize Cost USD`,
        change_fee_usd                                          AS `Change Fee USD`

    FROM `aaaaa_temporary_export_folder.cogs_flights_exchange_v2_1`
);

-- ============================================================
-- VCC / UATP Incentive Details
-- Source: wego_analytics.flights_bookings (conversions_tracked = 1)
-- Covers both VCC partners (Apiso / Nium via vcc_provider) and UATP (vc_partner_account LIKE '%UATP%')
-- ============================================================
CREATE OR REPLACE TABLE `aaaaa_temporary_export_folder.vcc_uatp_flights_details` AS (
    SELECT
        fb.booking_ref                                          AS `Booking Ref`,
        fb.gds_ref                                              AS `GDS Ref`,
        fb.site_code                                            AS `Site Code`,
        fb.market                                               AS `Market`,
        fb.created_at                                           AS `Created Time`,

        -- Partner identifiers
        fb.vc_partner_account                                   AS `VC Partner Account`,
        fb.vc_partner_card_pool_id                              AS `VC Partner Card Pool ID`,
        CASE
            WHEN vcc_provider.Partner IN ('Apiso', 'Nium') THEN 'Apiso_Nium'
            WHEN vcc_provider.Partner IS NOT NULL           THEN vcc_provider.Partner
            WHEN fb.vc_partner_account LIKE '%UATP%'         THEN 'UATP'
            ELSE NULL
        END                                                     AS `VC Partner`,

        -- VCC Rebate components (USD at source)
        fb.vc_rebate_value_usd                                  AS `VC Rebate Value USD`,
        fb.vc_rebate_fee_usd                                    AS `VC Rebate Fee USD`,
        IFNULL(fb.vc_rebate_value_usd, 0)
          + IFNULL(fb.vc_rebate_fee_usd, 0)                     AS `VCC Rebate Accrual USD`

    FROM `wego_analytics.flights_bookings` AS fb
    LEFT JOIN `wego-cloud.ota_data_upload_template_working_30Oct24.vcc_provider` AS vcc_provider
        ON fb.vc_partner_card_pool_id = vcc_provider.ClientID
    WHERE fb.conversions_tracked = 1
);
{% endraw %}
