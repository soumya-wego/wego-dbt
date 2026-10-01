{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : pricing_fares_analysis_daily_append
-- Destination: analysis.pricing_fares_analysis  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('analytics', 'exchange_rates') }}
-- depends_on: {{ source('flights', 'providers') }}
-- depends_on: {{ source('place_services', 'airports') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('services_curiosity', 'fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'fares') }}
-- depends_on: {{ source('services_curiosity', 'trips') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_searches') }}
{% raw %}
-- create table analysis.pricing_fares_analysis
-- partition by created_at
-- as

WITH t1 AS
  (SELECT created_at,
          created_at_timestamp,
          fare_id,
          search_id,
          f.trip_id,
          designator_codes,
          fare_count_concat,
          departure_date,
          arrival_date,
          first_departure_airport_name,
          first_departure_airport_code,
          first_arrival_airport_name,
          first_arrival_airport_code,
          first_departure_city_name,
          first_departure_city_code,
          first_arrival_city_name,
          first_arrival_city_code,
          first_departure_country_name,
          first_departure_country_code,
          first_arrival_country_name,
          first_arrival_country_code,
          first_stop_count,
          CASE WHEN provider_code LIKE '%wego.com' THEN 'wego.com' ELSE provider_code END AS provider_code,
          fare_amount_usd,
          fare_amount_local_currency,
          fare_currency_code,
          rank,
          wego_top_10,
          wego_not_top_10,
          trip_legs,
          trip_segments,
          trip_airport_codes,
          trip_airport_names,
          trip_city_codes,
          trip_city_names,
          trip_country_codes,
          trip_country_names,
          trip_airline_codes,
          trip_airline_names,
          trip_depature_times,
          trip_arrival_times,
          trip_cabin_names,
          trip_designator_codes,
          trip_timings,
          trip_dates,
          provider_type,
          site_code,
          trip_type,
          trip_category,
          trip_duration,
          adults_count,
          children_count,
          infants_count,
          total_pax_count,
          click,
          conversions_tracked
   FROM
     (SELECT search_id,
             created_at,
             created_at_timestamp,
             trip_id,
             fare_id,
             provider_code,
             fare_amount_usd,
             fare_amount_local_currency,
             fare_currency_code,
             rank,
             wego_top_10,
             wego_not_top_10,
             provider_type,
             fare_count_concat,
             site_code,
             trip_type,
             trip_category,
             trip_duration,
             adults_count,
             children_count,
             infants_count,
             total_pax_count,
             click,
             conversions_tracked

      FROM (  -- wego_fares V2 edit:
WITH
  fares_overall AS (
  SELECT
    DATE(timestamp_add(created_at, interval 8 HOUR)) AS created_at,
    # created_at denotes date when fare was generated, _table_suffix is the traffic date
    timestamp_add(created_at, interval 8 HOUR) AS created_at_timestamp,
    search_id,
    trip_id,
    id AS fare_id,
    provider.code AS provider_code,
    provider.name AS provider_name,
    price.amount_usd AS fare_amount_usd,
    price.amount AS fare_amount_local_currency,
    price.currency_code AS fare_currency_code,
    
    DENSE_RANK() OVER (PARTITION BY trip_id ORDER BY price.amount_usd ASC) AS rank # rank all fares by the trips_itineraries they were in
  FROM
    `wego-cloud.services_curiosity.fares*`
  WHERE
    _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
    # some fare_id records are duplicated, need to deduplicate later on
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1

    ),

  # Filter for trip_ids for which there was a fare that was clicked --> fare in clicks table
  ## Some cases dont have corresponding clicked provider present in the fares table, filtering only those trip id's which contains atleast one  


fares as 
    
  (select 
  * from fares_overall 
  where trip_id in 
    (select distinct(fares.trip_id) as trip_id from 
        (select trip_id,fare_id from fares_overall group by 1,2) as fares
        inner join 
        (SELECT  
          trip_id,fare_id
          FROM `wego-cloud.wego_analytics.flights_clicks`
          WHERE
          DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY) group by 1,2)  as clicked_trips
        on fares.trip_id = clicked_trips.trip_id
        and  fares.fare_id = clicked_trips.fare_id )),


  provider_type AS # get provider_type ("OTA", "Airline") from flights.providers table
  (
  SELECT
    pos_provider_code,
    provider_type
  FROM
    `wego-cloud.flights.providers`
  GROUP BY
    1,
    2 ),

  searches AS # get search dimensions, interval 3 days in case searches and fares dont align exactly
  (
  SELECT
    search_id,
    site_code,
    trip_type,
    trip_category,
    trip_duration,
    adults_count,
    children_count,
    infants_count,
    IFNULL(adults_count,0) + IFNULL(children_count,0) + IFNULL(infants_count,0) AS total_pax_count,
  FROM
    `wego-cloud.wego_analytics.flights_searches`
  WHERE
    DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY) ),

  clicks AS (
  select  
fare_id,
1 AS click,

  # For cases where conv. tracked > 1, revert to just 1
case when max(conversions_tracked) > 0 then 1 else 0 end as conversions_tracked
from
`wego-cloud.wego_analytics.flights_clicks`
WHERE
  DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)
  group by 1,2)

SELECT
  fares.*,
  CONCAT(rank, COUNT(trip_id) OVER (PARTITION BY trip_id)) AS fare_count_concat,
  MAX(CASE
      WHEN rank <= 10 AND provider_code LIKE '%wego.com' THEN 1
    ELSE
    0
  END
    ) OVER (PARTITION BY trip_id) AS wego_top_10,
  # to denote trip_ids where wego was a top 10 fare
  MAX(CASE
      WHEN rank > 10 AND provider_code LIKE '%wego.com' THEN 1
    ELSE
    0
  END
    ) OVER (PARTITION BY trip_id) AS wego_not_top_10,
  # to denote trip_ids where wego was not a top 10 fare
  provider_type.provider_type,
  searches.* EXCEPT(search_id),
  clicks.* EXCEPT(fare_id)
FROM
  fares
LEFT JOIN
  provider_type
ON
  fares.provider_code = provider_type.pos_provider_code
LEFT JOIN
  searches
ON
  fares.search_id = searches.search_id
LEFT JOIN
  clicks
ON
  fares.fare_id = clicks.fare_id

  )) f
   LEFT JOIN
     (SELECT trip_legs,
             trip_segments,
             trip_id,
             designator_codes,
             trip_airport_codes,
             trip_airport_names,
             trip_city_codes,
             trip_city_names,
             trip_country_codes,
             trip_country_names,
             trip_airline_codes,
             trip_airline_names,
             trip_depature_times,
             trip_arrival_times,
             trip_cabin_names,
             trip_designator_codes,
             trip_timings,
             trip_dates,
             departure_date,
             arrival_date,
             first_departure_airport_name,
             first_departure_airport_code,
             first_arrival_airport_name,
             first_arrival_airport_code,
             first_departure_city_name,
             first_departure_city_code,
             first_arrival_city_name,
             first_arrival_city_code,
             first_departure_country_name,
             first_departure_country_code,
             first_arrival_country_name,
             first_arrival_country_code,
             first_stop_count,
      FROM (-- wego_trips_itineraries V2 edits:

# generate table of airport + city + country info to be used later

WITH 
airport_places as
  (SELECT
   airports.* EXCEPT(location_id),
   places.*
   FROM
    (SELECT 
     code as airport_code,
     base_name as airport_name,
     location_id as location_id
     FROM `wego-cloud.place_services.airports`
      ) as airports
   left join
    (SELECT
     location.id as location_id,
     location.code as city_code, 
     location.base_name as city_name,
     cou.code as country_code, cou.base_name as country_name
     FROM `place_services.locations` as location
     LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id
    ) as places on airports.location_id = places.location_id
  ),

# assign row_number to every trip_id to dedupe by row_num = 1 later on
# only row_number function will dedupe down to 1 record because it does not repeat unlike rank or dense_rank
# row_number must be assigned and duplicates removed before cross-join unnest in flatten_segments step

trips as 
  (SELECT
   *,
   row_number() over (partition by id order by created_at desc) as trip_id_rank
   FROM `wego-cloud.services_curiosity.trips*`
      WHERE _table_suffix >= FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY)) 
  ),

# flatten out trips.segment nesting by cross join unnest and repeat trip_ids for every segment
# deduplicate multiple trip_ids in curiosity.trips table by taking last first row

flatten_segments as
  (SELECT 
   trips.id as trip_id,
   timestamp_add(trips.created_at, interval 8 HOUR) as created_at,
   trips.code as designator_codes,
   legs.order + 1 as legs_order,
   segments.order + 1 as segments_order,
   min(date(departure_time)) over (partition by trips.id) as departure_date,
   max(date(arrival_time)) over (partition by trips.id) as arrival_date,
   -- CONCAT(segments.departure_airport.code, '-', segments.arrival_airport.code) as segment_airport_codes,
   segments.* EXCEPT(`order`),
   FROM trips, unnest(legs) as legs, unnest(legs.segments) as segments
   WHERE trip_id_rank = 1
   order by trip_id, created_at asc, legs.order asc, segments.order asc
  ),

# join flatten_segments with airport_places to get departure / arrival airport places info
# concat departure + arrival param values into a string for aggregation later on
# flatten_segments only has airport_code info, pull out city + country info from airport_places table, for use later on

flatten_segments_2 as
  (SELECT
   flatten_segments.*,
   departure_airport.airport_name as departure_airport_name,
   departure_airport.airport_code as departure_airport_code, 
   arrival_airport.airport_name as arrival_airport_name,
   arrival_airport.airport_code as arrival_airport_code,
     departure_airport.city_name as departure_city_name, 
     departure_airport.city_code as departure_city_code,
     arrival_airport.city_name as arrival_city_name,
     arrival_airport.city_code as arrival_city_code,
     departure_airport.country_name as departure_country_name,
     departure_airport.country_code as departure_country_code,
     arrival_airport.country_name as arrival_country_name,
     arrival_airport.country_code as arrival_country_code,
     airline.code as airline_code,
     CONCAT(departure_time, ' - ', arrival_time) as segment_timings,
   CONCAT(date(departure_time), ' - ', date(arrival_time)) as segment_dates,
   CONCAT(flatten_segments.departure_airport.code, '-', flatten_segments.arrival_airport.code) as segment_airport_codes,
   CONCAT(departure_airport.airport_name,' - ', arrival_airport.airport_name) AS segment_airport_names,
   CONCAT(departure_airport.city_code,' - ', arrival_airport.city_code) AS segment_city_codes,
   CONCAT(departure_airport.city_name,' - ', arrival_airport.city_name) AS segment_city_names,
   CONCAT(departure_airport.country_code,' - ', arrival_airport.country_code) AS segment_country_codes,
   CONCAT(departure_airport.country_name,' - ', arrival_airport.country_name) AS segment_country_names,
   FROM flatten_segments
   left join airport_places as departure_airport on flatten_segments.departure_airport.code = departure_airport.airport_code
   left join airport_places as arrival_airport on flatten_segments.arrival_airport.code = arrival_airport.airport_code
  ),

# extract out the departure + arrival code & names (airport, city, country) for the first leg to become the "route" of that trip, regardless of oneway, roundtrip, multicity
# count number of stopovers in the first leg

flatten_segments_3 as 
  (SELECT
   *,
   first_value(IF(legs_order = 1, departure_airport_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_airport_name,
   first_value(IF(legs_order = 1, departure_airport_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_airport_code,
   last_value(IF(legs_order = 1, arrival_airport_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_airport_name,
   last_value(IF(legs_order = 1, arrival_airport_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_airport_code,
   first_value(IF(legs_order = 1, departure_city_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_city_name,
   first_value(IF(legs_order = 1, departure_city_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_city_code,
   last_value(IF(legs_order = 1, arrival_city_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_city_name,
   last_value(IF(legs_order = 1, arrival_city_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_city_code,
   first_value(IF(legs_order = 1, departure_country_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_country_name,
   first_value(IF(legs_order = 1, departure_country_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_country_code,
   last_value(IF(legs_order = 1, arrival_country_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_country_name,
   last_value(IF(legs_order = 1, arrival_country_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_country_code,
   first_value(IF(legs_order = 1, airline_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_airline_code,
   MAX(IF(legs_order = 1, segments_order, null)) over (partition by trip_id) - 1 as first_stop_count #total segments - 1 = stops within the leg
   from flatten_segments_2
  )

# fully flatten out all trip segment info into 1 record per trip_id by aggregating it to 1 row

SELECT
trip_id,
designator_codes,
departure_date,
arrival_date,
first_departure_airport_name,
first_departure_airport_code,
first_arrival_airport_name,
first_arrival_airport_code,
first_departure_city_name,
first_departure_city_code,
first_arrival_city_name,
first_arrival_city_code,
first_departure_country_name,
first_departure_country_code,
first_arrival_country_name,
first_arrival_country_code,
first_airline_code,
first_stop_count,
STRING_AGG(CAST(legs_order as STRING), ' : ' order by legs_order asc) as trip_legs,
STRING_AGG(CAST(segments_order as STRING), ' : ' order by legs_order, segments_order asc) as trip_segments,
STRING_AGG(segment_airport_codes, ' : ' order by legs_order, segments_order asc) as trip_airport_codes,
STRING_AGG(segment_airport_names, ' : ' order by legs_order, segments_order asc) as trip_airport_names,
STRING_AGG(segment_city_codes, ' : ' order by legs_order, segments_order asc) as trip_city_codes,
STRING_AGG(segment_city_names, ' : ' order by legs_order, segments_order asc) as trip_city_names,
STRING_AGG(segment_country_codes, ' : ' order by legs_order, segments_order asc) as trip_country_codes,
STRING_AGG(segment_country_names, ' : ' order by legs_order, segments_order asc) as trip_country_names,
STRING_AGG(airline.code, ' : ' order by legs_order, segments_order asc) as trip_airline_codes,
STRING_AGG(airline.name, ' : ' order by legs_order, segments_order asc) as trip_airline_names,
STRING_AGG(CAST(departure_time as STRING), ' : ' order by legs_order, segments_order asc) as trip_depature_times,
STRING_AGG(CAST(arrival_time as STRING), ' : ' order by legs_order, segments_order asc) as trip_arrival_times,
STRING_AGG(cabin, ' : ' order by legs_order, segments_order asc) as trip_cabin_names,
STRING_AGG(designator_code, ' : ' order by legs_order, segments_order asc) as trip_designator_codes,
STRING_AGG(segment_timings, ' : ' order by legs_order, segments_order asc) as trip_timings,
STRING_AGG(segment_dates, ' : ' order by legs_order, segments_order asc) as trip_dates,
from flatten_segments_3
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
      ) i ON f.trip_id = i.trip_id
   WHERE departure_date IS NOT NULL),

     t2 AS
  (SELECT *,
          MIN(fare_amount_usd) OVER(PARTITION BY trip_id) AS trip_min_fare_amount_usd,
          FIRST_VALUE(provider_code) OVER(PARTITION BY trip_id ORDER BY fare_amount_usd ASC) AS cheapest_provider,
          FIRST_VALUE(provider_code) OVER(PARTITION BY trip_id ORDER BY click DESC) AS clicked_provider,
          FIRST_VALUE(if(conversions_tracked = 1, provider_code, null)) OVER(PARTITION BY trip_id ORDER BY conversions_tracked DESC) AS converted_provider
   FROM t1),

     t3 AS
  (SELECT *,
          CASE
              WHEN fare_amount_usd = trip_min_fare_amount_usd THEN TRUE
              ELSE FALSE
          END AS trip_if_cheapest_provider_code
   FROM t2),

     t4 AS
  (SELECT *,
          COUNT(provider_code) OVER(PARTITION BY trip_id) AS trip_provider_code_count,
                               COUNT(CASE
                                         WHEN trip_if_cheapest_provider_code THEN 1
                                         ELSE NULL
                                     END) OVER(PARTITION BY trip_id) AS trip_cheapest_provider_code_count -- no of provider code that has the cheapest fare, can be more than 1

   FROM t3),

     t5 AS
  (SELECT *,
          STRING_AGG(CAST(fare_amount_usd AS STRING), ',') OVER(PARTITION BY trip_id
                                                                ORDER BY fare_amount_usd RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS trip_fare_agg -- concat all fares in ascending order

   FROM t4),

     t6 AS
  (SELECT *,
          CASE
              WHEN trip_if_cheapest_provider_code
                   AND trip_cheapest_provider_code_count = 1 THEN CAST(SPLIT(trip_fare_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC)
              ELSE trip_min_fare_amount_usd -- if it's the only cheapest provider, take the next cheapest, else take the cheapest fare, auto null if it's the only fare

          END AS trip_min_fare_amount_usd_exclude_own_fare
   FROM t5),

     t7 AS
  (SELECT *,
          CASE
              WHEN trip_if_cheapest_provider_code
                   AND trip_provider_code_count = 1 THEN NULL -- NULL when it's the only cheapest provider
 
              WHEN trip_if_cheapest_provider_code 
                   AND trip_cheapest_provider_code_count > 1 THEN 0 -- 0 parity when it's not the only cheapest provider

              ELSE safe_divide((fare_amount_usd - trip_min_fare_amount_usd_exclude_own_fare), fare_amount_usd) * 100
          END AS fare_parity_pct
   FROM t6),

  t8 AS
  (SELECT *
   FROM t7
WHERE 
TRIM(SPLIT(trip_airport_codes, ':')[safe_ordinal(1)]) != IFNULL(TRIM(SPLIT(trip_airport_codes, ':')[safe_ordinal(2)]),'null')),

  bow_pricing as 
  (SELECT DISTINCT
  fare_id,
  fare_ipcc,
  booking_ipcc,
  validating_airline_code,
  gds_commission,
  adult_total_iata + child_total_iata as iata_commission,
  adult_total_plb + child_total_plb as plb_commission,
  vendor_commission_rbd,
  payment_gateway_fee,
  vendor_fee,
  net_margin as original_net_margin,
  net_margin_percentage * 100 as original_net_margin_percentage,
  CASE
    WHEN safe_divide((final_total - original_total), original_total) * 100 < 0 THEN max_margin_percentage * final_total
    WHEN safe_divide((final_total - original_total), original_total) * 100 > 0 THEN min_margin_percentage * final_total
    ELSE net_margin
   END AS final_net_margin,
  CASE
    WHEN safe_divide((final_total - original_total), original_total) * 100 < 0 THEN max_margin_percentage
    WHEN safe_divide((final_total - original_total), original_total) * 100 > 0 THEN min_margin_percentage
    ELSE net_margin_percentage
   END * 100 AS final_net_margin_percentage,
  min_margin_percentage,
  max_margin_percentage,
  booking_margin_id,
  booking_margin_rbd,
  action,
  final_total - original_total as markup,
  safe_divide((final_total - original_total), original_total) * 100 as markup_percentage,
  original_total as original_total_local_currency,
  currency_code as bow_local_currency,
  final_total_usd
FROM
  `services_curiosity.fare_calculations*`
WHERE _table_suffix >= FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY)))

SELECT DISTINCT
created_at,
created_at_timestamp,
t8.fare_id,
search_id,
trip_id,
designator_codes,
fare_count_concat,
departure_date,
arrival_date,
first_departure_airport_name,
first_departure_airport_code,
first_arrival_airport_name,
first_arrival_airport_code,
first_departure_city_name,
first_departure_city_code,
first_arrival_city_name,
first_arrival_city_code,
first_departure_country_name,
first_departure_country_code,
first_arrival_country_name,
first_arrival_country_code,
TRIM(REGEXP_EXTRACT(trip_airline_codes, r'^([a-zA-Z0-9\s]*):*')) AS first_airline,
CONCAT(first_departure_country_code," - ", first_arrival_country_code) AS country_route,
CONCAT(first_departure_city_code," - ", first_arrival_city_code) AS city_route,
CONCAT(first_departure_airport_code," - ", first_arrival_airport_code) AS airport_route,
first_stop_count,
provider_code,
fare_amount_usd,
fare_amount_local_currency,
fare_currency_code,
rank,
wego_top_10,
wego_not_top_10,
trip_legs,
trip_segments,
trip_airport_codes,
trip_airport_names,
trip_city_codes,
trip_city_names,
trip_country_codes,
trip_country_names,
trip_airline_codes,
trip_airline_names,
trip_depature_times,
trip_arrival_times,
trip_cabin_names,
trip_designator_codes,
trip_timings,
trip_dates,
provider_type,
site_code,
trip_type,
trip_category,
trip_duration,
adults_count,
children_count,
infants_count,
total_pax_count,
click,
CAST(conversions_tracked AS FLOAT64) as conversions_tracked,
trip_min_fare_amount_usd,
trip_if_cheapest_provider_code,
trip_provider_code_count,
trip_cheapest_provider_code_count,
trip_fare_agg,
trip_min_fare_amount_usd_exclude_own_fare,
fare_parity_pct,
cheapest_provider,
clicked_provider,
converted_provider,
if(fare_parity_pct < 0, 1, 0) as price_win,
if(provider_code = clicked_provider, 1, 0) as click_win,
if(provider_code = converted_provider, 1, 0) as conversion_win,
if(fare_count_concat = "11", "only provider", "multiple providers") as providers_status,
fare_ipcc as bow_fare_ipcc,
booking_ipcc as bow_booking_ipcc,
validating_airline_code as bow_validating_airline_code,
gds_commission * amount as bow_gds_commission_usd,
iata_commission * amount as bow_iata_commission_usd,
plb_commission * amount as bow_plb_commission_usd,
vendor_commission_rbd as bow_vendor_commission_rbd,
payment_gateway_fee * amount as bow_payment_gateway_fee_usd,
vendor_fee * amount as bow_vendor_fee_usd,
original_net_margin as bow_original_net_margin,
original_net_margin * amount as bow_original_net_margin_usd,
original_net_margin_percentage as bow_original_net_margin_percentage,
final_net_margin as bow_final_net_margin,
final_net_margin * amount as bow_final_net_margin_usd,
final_net_margin_percentage as bow_final_net_margin_percentage,
markup as bow_markup,
markup * amount as bow_markup_usd,
markup_percentage as bow_markup_percentage,
min_margin_percentage as bow_min_margin_percentage,
max_margin_percentage as bow_max_margin_percentage,
booking_margin_id as bow_booking_margin_id,
booking_margin_rbd as bow_booking_margin_rbd,
action as bow_action,
original_total_local_currency as bow_original_total_local_currency,
bow_local_currency,
original_total_local_currency * amount as bow_original_total_fare_usd,
safe_divide((original_total_local_currency * amount), total_pax_count) as bow_original_fare_usd,
final_total_usd as bow_final_total_fare_usd,
safe_divide(final_total_usd, total_pax_count) as bow_final_fare_usd
FROM t8 
left join 
bow_pricing on t8.fare_id = bow_pricing.fare_id
left join 
(SELECT base,
          amount,
          effective
   FROM `analytics.exchange_rates*`
   WHERE _TABLE_SUFFIX >= '20200824'
     AND quote = 'USD') exchange on bow_pricing.bow_local_currency = exchange.base and t8.created_at = cast(exchange.effective as date)
{% endraw %}
