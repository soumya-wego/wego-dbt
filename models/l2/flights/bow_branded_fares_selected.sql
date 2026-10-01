{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : bow_branded_fares_selected
-- Destination: analysis.bow_branded_fares_selected  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('saba_cross_supplier_netcost') }}
-- depends_on: {{ source('integrated_bookings_flights', 'branded_fares') }}
-- depends_on: {{ source('place_services', 'airports') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'trips') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
{% raw %}
-- create or replace table
-- analysis.bow_branded_fares_selected
-- partition by created_at_date
-- as

-- with    
-- branded_fares as    
-- (
-- SELECT id,
-- ms_Fare_id,
-- round(booking_price.total_amount_usd,2) as total_amount_usd,
-- round(booking_price. total_booking_fee,2) as booking_fee
--  FROM `wego-cloud.integrated_bookings_flights.branded_fares*` where selected is true and endpoint = "COMPARE"
--  and _table_suffix >= "20260101"
--   and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
--  qualify row_number() over(partition by ms_Fare_id order by booking_price.total_amount_usd asc) = 1),
-- final as 


-- (SELECT 

-- a.* except(payment_gateway_fee_ids,booking_margin_rbd,fare_basis_codes) ,
-- date(created_at) as created_at_date,

-- b.total_amount_usd,b.booking_fee,

-- round(b.total_amount_usd-a.final_total_usd,2) as booking_fee_usd   FROM 

-- (select * from `wego-cloud.services_curiosity.branded_fare_calculations*` where endpoint = "COMPARE" and fare_id like "%:ss"
-- and _table_suffix >= "20260101"
--   and  _TABLE_SUFFIX <= (SELECT format('%s', format_date("%Y%m%d", current_date())))
--   qualify row_number() over(partition by search_id,fare_id,branded_fare_id,fare_ipcc,booking_ipcc order by created_at asc) = 1) as a   
-- inner join branded_Fares as b  
-- on a.fare_id = b.ms_fare_id   
-- and
-- a.branded_fare_id = b.id),

-- base as 
-- (select * from final where date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR)) <= date_sub(current_date(), interval 1 day)),

-- ipcc_integration_type_lkp as    
-- (
-- SELECT ipcc,max(integration_type) as integration_type FROM `wego-cloud.wego_analytics.flights_bookings` 
--  group by 1 

-- )

-- select a.*,b.integration_type from base as a 
-- left join ipcc_integration_type_lkp as  b      
-- on a.fare_ipcc = b.ipcc 



WITH
  branded_fares AS (
    SELECT
      id,
      ms_fare_id,
      saba_details.provider_code AS saba_provider_code,
      ROUND(booking_price.total_amount_usd, 2) AS total_amount_usd,
      ROUND(booking_price.total_booking_fee, 2) AS booking_fee
    FROM `wego-cloud.integrated_bookings_flights.branded_fares*`
    WHERE selected IS TRUE                          -- outer: pick the chosen branded fare
      AND endpoint = 'COMPARE'
      AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
                            AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY ms_fare_id
      ORDER BY booking_price.total_amount_usd ASC
    ) = 1
  ),
  final AS (
    SELECT
      a.*
        EXCEPT (payment_gateway_fee_ids, booking_margin_rbd, fare_basis_codes),
      date(created_at) AS created_at_date,
      b.total_amount_usd,
      b.booking_fee,
      b.saba_provider_code,
      round(b.total_amount_usd-a.final_total_usd, 2) AS booking_fee_usd
    FROM
      (
        SELECT *
        FROM `wego-cloud.services_curiosity.branded_fare_calculations*`
        WHERE
          endpoint = "COMPARE"
          AND fare_id LIKE "%:ss"
          AND _table_suffix >= (
            SELECT
              format(
                '%s',
                format_date("%Y%m%d", date_sub(current_date(), INTERVAL 1 day)))
          )
          AND _TABLE_SUFFIX <= (
            SELECT format('%s', format_date("%Y%m%d", current_date()))
          )
        QUALIFY
          row_number()
            OVER (
              PARTITION BY
                search_id, fare_id, branded_fare_id, fare_ipcc, booking_ipcc
              ORDER BY created_at ASC
            )
          = 1
      ) AS a
    INNER JOIN branded_Fares AS b
      ON
        a.fare_id = b.ms_fare_id
        AND a.branded_fare_id = b.id
  ),
  base AS (
    SELECT *
    FROM final
    WHERE
      date(TIMESTAMP_ADD(created_at, INTERVAL 8 HOUR))
      = date_sub(current_date(), INTERVAL 1 day)
  ),
  ipcc_integration_type_lkp AS (
    SELECT ipcc, max(integration_type) AS integration_type
    FROM `wego-cloud.wego_analytics.flights_bookings`
    GROUP BY 1
  ),
  airports AS (
  SELECT
  a.code AS airport_code,
  a.base_name AS airport_name,
  l.code AS city_code,
  l.base_name AS city_name,
  c.code AS country_code,
  c.base_name AS country_name
  FROM `wego-cloud.place_services.airports` AS a
  LEFT JOIN `place_services.locations` AS l
  ON a.location_id = l.id
  LEFT JOIN `place_services.countries` AS c
  ON c.id = l.country_id
),
trips AS(
  SELECT
  search_id,
  id,
  MIN_BY(segments.departure_airport.code, segments.departure_time) AS departure_airport_code,
  MAX_BY(segments.arrival_airport.code, segments.departure_time) AS arrival_airport_code
  FROM `wego-cloud.services_curiosity.trips*`,
  UNNEST(legs) AS legs, UNNEST(legs.segments) AS segments
  WHERE _table_suffix >= (
        SELECT
          format(
            '%s',
            format_date("%Y%m%d", date_sub(current_date(), INTERVAL 1 day)))
      )
      AND _TABLE_SUFFIX <= (
        SELECT format('%s', format_date("%Y%m%d", current_date()))
      ) 
  AND legs.order=0
  GROUP BY 1,2
),
  final_trips AS(
  SELECT
  search_id,
  id,
  departure_airport_code,
  b.city_code AS departure_city_code,
  b.country_code AS departure_country_code,
  arrival_airport_code,
  c.city_code AS arrival_city_code,
  c.country_code AS arrival_country_code
  FROM trips a
  LEFT JOIN
  airports b
  ON
  a.departure_airport_code=b.airport_code
  LEFT JOIN
  airports c
  ON
  a.arrival_airport_code=c.airport_code
),
fare_price AS(
  SELECT
  fare_id,
  final_total_usd AS fare_final_total_usd,
  FROM
  `wego-cloud.services_curiosity.fare_calculations*`
  WHERE _table_suffix >= (
        SELECT
          format(
            '%s',
            format_date("%Y%m%d", date_sub(current_date(), INTERVAL 1 day)))
      )
      AND _TABLE_SUFFIX <= (
        SELECT format('%s', format_date("%Y%m%d", current_date()))
      ) 
)
SELECT 
DISTINCT
  a.search_id,
  flight_id,
  a.fare_id,
  a.branded_fare_id,
  fare_ipcc,
  booking_ipcc,
  search_site_code,
  validating_airline_code,
  original_total,
  original_total_base,
  original_total_tax,
  original_adult,
  original_adult_base,
  original_adult_tax,
  original_child,
  original_child_base,
  original_child_tax,
  original_infant,
  original_infant_base,
  original_infant_tax,
  gds_commission,
  gds_commission_id,
  adult_total_commission,
  adult_total_iata,
  adult_total_plb,
  child_total_commission,
  child_total_iata,
  child_total_plb,
  adult_total_cat35_commission,
  child_total_cat35_commission,
  infant_total_cat35_commission,
  vendor_commission_id,
  vendor_commission_rbd,
  payment_gateway_fee,
  vendor_fee,
  vendor_fee_ids,
  vcc_rebate_amount_vendor_currency,
  vcc_rebate_limit_applied,
  vcc_rebate_amount_card_currency,
  vcc_rebate_limit_amount_card_currency,
  vcc_rebate_card_currency,
  vcc_rebate_exchange_rate,
  vcc_rebate_rate,
  net_margin,
  net_margin_percentage,
  min_margin_percentage,
  max_margin_percentage,
  booking_margin_id,
  final_total_usd,
  final_total,
  final_total_base,
  final_total_tax,
  final_adult,
  final_adult_base,
  final_adult_tax,
  final_child,
  final_child_base,
  final_child_tax,
  final_infant,
  final_infant_base,
  final_infant_tax,
  currency_code,
  endpoint,
  action,
  created_at,
  created_at_date,
  booking_fee,
  booking_fee_usd,
  integration_type,
  departure_airport_code,
  departure_city_code,
  departure_country_code,
  arrival_airport_code,
  arrival_city_code,
  arrival_country_code,
CONCAT(departure_airport_code, '-', arrival_airport_code) AS airport_route,
CONCAT(departure_city_code, '-', arrival_city_code) AS city_route,
CONCAT(departure_country_code, '-',arrival_country_code) AS country_route,
fare_final_total_usd,
total_amount_usd,
CASE
  WHEN saba_provider_code IS NOT NULL THEN TRUE
  ELSE FALSE END AS saba_flag,
  s.original_provider              AS saba_original_provider,
  s.selected_provider             AS saba_selected_provider,
  s.original_price_usd            AS saba_original_price_usd,
  s.selected_price_usd            AS saba_selected_price_usd,
  s.final_price_usd               AS saba_final_price_usd,
  s.selected_ms_fare_id,
  s.compare_key_agrees,
  s.fx_selected,
  s.fx_from_calc,
  s.search_net_cost_original_usd,
  s.search_net_cost_selected_usd,
  s.compare_net_cost_selected_usd,
  ROUND(s.search_net_cost_original_usd - s.search_net_cost_selected_usd, 2)  AS saba_net_cost_benefit_usd,
  ROUND(s.compare_net_cost_selected_usd - s.search_net_cost_original_usd, 2) AS realized_net_cost_delta_usd,
  (s.branded_fare_id IS NOT NULL) AS is_saba_switched,
  net_margin_usd_original, 
  net_margin_usd_selected,
  net_margin_usd_compare
FROM base AS a
LEFT JOIN ipcc_integration_type_lkp AS b
  ON a.fare_ipcc = b.ipcc
LEFT JOIN final_trips c
ON a.search_id=c.search_id 
AND a.flight_id=c.id
LEFT JOIN fare_price d
ON
a.fare_id=d.fare_id
LEFT JOIN `wego-cloud.analysis.saba_cross_supplier_netcost` s
  ON a.branded_fare_id = s.branded_fare_id
 AND a.created_at_date = s.report_date
{% endraw %}
