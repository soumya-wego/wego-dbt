{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : pricing_engine_analysis
-- Destination: analysis.pricing_engine_analysis  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- -- backfill:

-- create table 
-- analysis.pricing_engine_analysis
-- partition by date 
-- as

-- # generate table of airport + city + country info to be used later
-- WITH 
-- airport_places as
--   (SELECT
--    airports.* EXCEPT(location_id),
--    places.*
--    FROM
--     (SELECT 
--      code as airport_code,
--      base_name as airport_name,
--      location_id as location_id
--      FROM `wego-cloud.place_services.airports`
--       ) as airports
--    left join
--     (SELECT
--      location.id as location_id,
--      location.code as city_code, 
--      location.base_name as city_name,
--      cou.code as country_code, cou.base_name as country_name
--      FROM `place_services.locations` as location
--      LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id
--     ) as places on airports.location_id = places.location_id
--   ),

-- # assign row_number to every trip_id to dedupe by row_num = 1 later on
-- # only row_number function will dedupe down to 1 record because it does not repeat unlike rank or dense_rank
-- # row_number must be assigned and duplicates removed before cross-join unnest in flatten_segments step

-- trips as 
--   (SELECT
--    *,
--    row_number() over (partition by id order by created_at desc) as trip_id_rank
--    FROM `wego-cloud.services_curiosity.trips*`
--       #WHERE _table_suffix >= FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 2 DAY))
--       WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
--       # _table_suffix = '20210131' 
--       #WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))  
--   ),

-- # flatten out trips.segment nesting by cross join unnest and repeat trip_ids for every segment
-- # deduplicate multiple trip_ids in curiosity.trips table by taking last first row

-- flatten_segments as
--   (SELECT 
--    trips.id as trip_id,
--    trips.created_at as created_at,
--    trips.code as designator_codes,
--    legs.order + 1 as legs_order,
--    segments.order + 1 as segments_order,
--    min(date(departure_time)) over (partition by trips.id) as departure_date,
--    max(date(arrival_time)) over (partition by trips.id) as arrival_date,
--    -- CONCAT(segments.departure_airport.code, '-', segments.arrival_airport.code) as segment_airport_codes,
--    segments.* EXCEPT(`order`),
--    FROM trips, unnest(legs) as legs, unnest(legs.segments) as segments
--    WHERE trip_id_rank = 1
--    order by trip_id, created_at asc, legs.order asc, segments.order asc
--   ),

-- # join flatten_segments with airport_places to get departure / arrival airport places info
-- # concat departure + arrival param values into a string for aggregation later on
-- # flatten_segments only has airport_code info, pull out city + country info from airport_places table, for use later on

-- flatten_segments_2 as
--   (SELECT
--    flatten_segments.*,
--    departure_airport.airport_name as departure_airport_name,
--    departure_airport.airport_code as departure_airport_code, 
--    arrival_airport.airport_name as arrival_airport_name,
--    arrival_airport.airport_code as arrival_airport_code,
--    departure_airport.city_name as departure_city_name, 
--    departure_airport.city_code as departure_city_code,
--    arrival_airport.city_name as arrival_city_name,
--    arrival_airport.city_code as arrival_city_code,
--    departure_airport.country_name as departure_country_name,
--    departure_airport.country_code as departure_country_code,
--    arrival_airport.country_name as arrival_country_name,
--    arrival_airport.country_code as arrival_country_code,
--    airline.code as airline_code,
--    CONCAT(departure_time, ' - ', arrival_time) as segment_timings,
--    CONCAT(date(departure_time), ' - ', date(arrival_time)) as segment_dates,
--    CONCAT(flatten_segments.departure_airport.code, '-', flatten_segments.arrival_airport.code) as segment_airport_codes,
--    CONCAT(departure_airport.airport_name,' - ', arrival_airport.airport_name) AS segment_airport_names,
--    CONCAT(departure_airport.city_code,' - ', arrival_airport.city_code) AS segment_city_codes,
--    CONCAT(departure_airport.city_name,' - ', arrival_airport.city_name) AS segment_city_names,
--    CONCAT(departure_airport.country_code,' - ', arrival_airport.country_code) AS segment_country_codes,
--    CONCAT(departure_airport.country_name,' - ', arrival_airport.country_name) AS segment_country_names,
--    FROM flatten_segments
--    left join airport_places as departure_airport on flatten_segments.departure_airport.code = departure_airport.airport_code
--    left join airport_places as arrival_airport on flatten_segments.arrival_airport.code = arrival_airport.airport_code
--   ),

-- # extract out the departure + arrival code & names (airport, city, country) for the first leg to become the "route" of that trip, regardless of oneway, roundtrip, multicity
-- # count number of stopovers in the first leg

-- flatten_segments_3 as 
--   (SELECT
--    *,
--    first_value(IF(legs_order = 1, designator_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_designator_code,
--    first_value(IF(legs_order = 1, departure_airport_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_airport_name,
--    first_value(IF(legs_order = 1, departure_airport_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_airport_code,
--    last_value(IF(legs_order = 1, arrival_airport_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_airport_name,
--    last_value(IF(legs_order = 1, arrival_airport_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_airport_code,
--    first_value(IF(legs_order = 1, departure_city_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_city_name,
--    first_value(IF(legs_order = 1, departure_city_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_city_code,
--    last_value(IF(legs_order = 1, arrival_city_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_city_name,
--    last_value(IF(legs_order = 1, arrival_city_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_city_code,
--    first_value(IF(legs_order = 1, departure_country_name, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_country_name,
--    first_value(IF(legs_order = 1, departure_country_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_departure_country_code,
--    last_value(IF(legs_order = 1, arrival_country_name, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_country_name,
--    last_value(IF(legs_order = 1, arrival_country_code, null) IGNORE NULLS) over (partition by trip_id order by arrival_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_arrival_country_code,
--    first_value(IF(legs_order = 1, airline_code, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_airline_code,
--    first_value(IF(legs_order = 1, cabin, null) IGNORE NULLS) over (partition by trip_id order by departure_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_cabin_class,
--    MAX(IF(legs_order = 1, segments_order, null)) over (partition by trip_id) - 1 as first_stop_count #total segments - 1 = stops within the leg
--    from flatten_segments_2
--   ),

-- # fully flatten out all trip segment info into 1 record per trip_id by aggregating it to 1 row

-- trips_final AS
--   (SELECT
--   trip_id,
--   first_designator_code,
--   designator_codes,
--   departure_date,
--   arrival_date,
--   first_departure_airport_name,
--   first_departure_airport_code,
--   first_arrival_airport_name,
--   first_arrival_airport_code,
--   first_departure_city_name,
--   first_departure_city_code,
--   first_arrival_city_name,
--   first_arrival_city_code,
--   first_departure_country_name,
--   first_departure_country_code,
--   first_arrival_country_name,
--   first_arrival_country_code,
--   first_airline_code,
--   first_cabin_class,
--   first_stop_count, # this is stop count for 1st leg
--   MAX(segments_order) - 1 AS stop_count, # this is the max of stop count across the legs
--   STRING_AGG(CAST(legs_order as STRING), ' : ' order by legs_order asc) as trip_legs,
--   STRING_AGG(CAST(segments_order as STRING), ' : ' order by legs_order, segments_order asc) as trip_segments,
--   STRING_AGG(segment_airport_codes, ' : ' order by legs_order, segments_order asc) as trip_airport_codes,
--   STRING_AGG(segment_airport_names, ' : ' order by legs_order, segments_order asc) as trip_airport_names,
--   STRING_AGG(segment_city_codes, ' : ' order by legs_order, segments_order asc) as trip_city_codes,
--   STRING_AGG(segment_city_names, ' : ' order by legs_order, segments_order asc) as trip_city_names,
--   STRING_AGG(segment_country_codes, ' : ' order by legs_order, segments_order asc) as trip_country_codes,
--   STRING_AGG(segment_country_names, ' : ' order by legs_order, segments_order asc) as trip_country_names,
--   STRING_AGG(airline.code, ' : ' order by legs_order, segments_order asc) as trip_airline_codes,
--   STRING_AGG(airline.name, ' : ' order by legs_order, segments_order asc) as trip_airline_names,
--   STRING_AGG(CAST(departure_time as STRING), ' : ' order by legs_order, segments_order asc) as trip_departure_times,
--   STRING_AGG(CAST(arrival_time as STRING), ' : ' order by legs_order, segments_order asc) as trip_arrival_times,
--   STRING_AGG(cabin, ' : ' order by legs_order, segments_order asc) as trip_cabin_names,
--   STRING_AGG(designator_code, ' : ' order by legs_order, segments_order asc) as trip_designator_codes,
--   STRING_AGG(segment_timings, ' : ' order by legs_order, segments_order asc) as trip_timings,
--   STRING_AGG(segment_dates, ' : ' order by legs_order, segments_order asc) as trip_dates,
--   from flatten_segments_3
--   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20),

-- fares_raw as
--   (SELECT
--    created_at, # created_at denotes traffic timestamp i.e. timestamp of the user seeing the fare , _table_suffix is the insertion date, don't use _table_suffix unless required
--    -- created_at as created_at_timestamp,
--    search_id,
--    trip_id,
--    id as fare_id,
--    CASE WHEN provider.code LIKE '%wego.com' THEN 'wego.com' ELSE provider.code END as provider_code, # standardise all wego's provider_code e.g. ae.wego.com, sa.wego.com, etc
--    provider.name as provider_name,
--    price.amount as fare_amount, # this is PER PAX
--    price.amount_usd as fare_amount_usd, # this is PER PAX
--    row_number() over (partition by id) as id_row, # some fare_id records are duplicated, need to deduplicate later on
--    /*
--    this is obsolete as to calculate the initial rank, we need the original fare amount, but wego's fares in curiosity fares are already marked up/down
--    dense_rank() over (partition by trip_id order by price.amount_usd asc) as rank # rank all fares by the trips_itineraries they were in
--    */
--    FROM `wego-cloud.services_curiosity.fares*`
--    /*
--    WHERE _table_suffix >= FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 2 DAY))
--    */
--   WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
--    # WHERE _table_suffix = '20210131'
--    #WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY)) 
--     /*
--      and trip_id in # Filter for trip_ids in akasha for which there was a fare that was clicked --> fare in clicks table
--                     (SELECT
--                      distinct trip_id 
--                      FROM `wego-cloud.services_curiosity.fares*`
--                      WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
--                        and id in (SELECT # filter for fare_ids which became a click_id
--                                   DISTINCT CASE WHEN strpos(pageviews.fare_id, 'wego.com') > 0 then pageviews.fare_id
--                                                 ELSE clicks.fare_id END click_id 
--                                   FROM
--                                     (SELECT
--                                      fare_id, # is different from click_id unlike WA hotel_clicks
--                                      pageview_id
--                                      FROM `wego-cloud.wego_analytics.flights_clicks` 
--                                      WHERE DATE(_PARTITIONTIME) >= DATE_SUB(@run_date, INTERVAL 3 DAY)
--                                     ) as clicks
                                  
--                                   LEFT JOIN
--                                     (SELECT
--                                      pageview_id,
--                                      split(replace(regexp_extract(page_url, r'\/[a-z0-9_]+:[a-z0-9_]+:[a-z0-9_]+\/booking\?'),'_','.'),'/')[offset(1)] as fare_id,
--                                      FROM `wego-cloud.wego_analytics.pageviews` 
--                                      WHERE DATE(_PARTITIONTIME) >= DATE_SUB(@run_date, INTERVAL 3 DAY)
--                                        and page_type = 'flights_handoff'
--                                     ) as pageviews on clicks.pageview_id = pageviews.pageview_id
--                                  )
--                     )
--                     */
--   ),

-- # outdated method to get branded_fare_id to join
-- integrated_bookings_itineraries as
--   (SELECT
--     ms_fare_id,
--     branded_fare_id,
--     ms_trip_id
--    FROM integrated_bookings_flights.itineraries
--    ),

-- # new method get branded_fare_id
-- branded_fare_calculations AS 
-- (SELECT 
--   DISTINCT branded_fare_id,
--   fare_id
--   FROM `wego-cloud.services_curiosity.branded_fare_calculations*`
--   WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))),

--  # create new column reworked_fare_id, which for wego, uses branded_fare_id instead of fare_id to join clicks
-- fares as
--   (SELECT
--     fares_raw.*,
--     branded_fare_calculations.branded_fare_id,
--     CASE WHEN provider_code = 'wego.com' THEN branded_fare_calculations.branded_fare_id ELSE fares_raw.fare_id END AS reworked_fare_id,
--    FROM
--    fares_raw 
--    LEFT JOIN branded_fare_calculations ON fares_raw.fare_id = branded_fare_calculations.fare_id),

-- provider_type as # get provider_type ("OTA", "Airline") from flights.providers table 
--   (SELECT
--    pos_provider_code,
--    provider_type
--    FROM `wego-cloud.flights.providers`
--    group by 1,2      
--   ),

-- # get the pax count from Backend table instead because there can be dropoffs in WA searches and pax count is needed to calculate total amount
-- search_requests as
--   (SELECT DISTINCT
--    search.id as search_id,
--    search.adults_count,
--    search.children_count,
--    search.infants_count,
--    search.adults_count + search.children_count + search.infants_count AS total_pax_count
--    FROM `wego-cloud.services_curiosity.search_requests*`
--    WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 30 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))),


-- # get search dimensions, interval 4 days in case searches and fares dont align exactly
-- searches as 
--   (SELECT DISTINCT
--    search_id,
--    site_code,
--    trip_type,
--    trip_category,
--    trip_duration,
--    -- adults_count,
--    -- children_count,
--    -- infants_count,
--    -- IFNULL(adults_count,0) + IFNULL(children_count,0) + IFNULL(infants_count,0) AS total_pax_count,
--    FROM `wego-cloud.wego_analytics.flights_searches`
--    WHERE DATE(_PARTITIONTIME) >= DATE_SUB(@run_date, INTERVAL 30 DAY)      
--   ),

-- # get clicks dimensions, interval 4 days in case searches and fares dont align exactly
-- clicks as
--   (SELECT
--    click_id,
--    fare_id, # note wego's fare_id is branded_fare_id
--    trip_id,
--    1 as click,
--    CASE WHEN conversions_tracked > 0 THEN 1 
--         ELSE conversions_tracked 
--         END AS conversions_tracked, # For cases where conv. tracked > 1, revert to just 1
--    FROM `wego-cloud.wego_analytics.flights_clicks` 
--    WHERE DATE(_PARTITIONTIME) >= DATE_SUB(@run_date, INTERVAL 30 DAY)
--   ),

-- # get trip_id which are seen/have impressions
-- trips_w_impressions as
--   (SELECT DISTINCT
--    trip_id
--    FROM clicks),

-- # get trip_id which are seen/have impressions
-- trips_w_bookings as
--   (SELECT DISTINCT
--    trip_id
--    FROM clicks WHERE conversions_tracked > 0),

-- # join fares to clicks first to aggregate up to fares
-- fares_clicks as
--   (SELECT
--    fares.* EXCEPT (branded_fare_id,
--                    reworked_fare_id
--                    ),
--    COUNT(DISTINCT click_id) AS click,
--    CASE
--     WHEN SUM(conversions_tracked) > 0 THEN 1
--     ELSE SUM(conversions_tracked)
--    END AS conversions_tracked
--    FROM
--    fares
--    LEFT JOIN clicks on fares.reworked_fare_id = clicks.fare_id
--    GROUP BY 1,2,3,4,5,6,7,8,9),

-- # join fares to respective tables
-- fares_final as
--   (SELECT DISTINCT
--   fares_clicks.* EXCEPT (click,
--                          conversions_tracked)
--                  REPLACE(fare_amount * search_requests.total_pax_count AS fare_amount, # this is total amount, NOT amount per pax
--                   fare_amount_usd * search_requests.total_pax_count AS fare_amount_usd # this is total amount, NOT amount per pax
--                   ),
--   provider_type.provider_type,
--   search_requests.* EXCEPT(search_id),
--   searches.* EXCEPT(search_id),
--   fares_clicks.click,
--   fares_clicks.conversions_tracked,
--   CASE
--     WHEN trips_w_bookings.trip_id IS NOT NULL THEN 'booked impression'
--     WHEN trips_w_impressions.trip_id IS NOT NULL THEN 'non-booked impression'
--     ELSE 'no impression'
--   END AS fare_impression
--   FROM fares_clicks
--   INNER JOIN search_requests ON fares_clicks.search_id = search_requests.search_id
--   LEFT JOIN provider_type on fares_clicks.provider_code = provider_type.pos_provider_code
--   LEFT JOIN searches on search_requests.search_id = searches.search_id
--   LEFT JOIN trips_w_impressions on fares_clicks.trip_id = trips_w_impressions.trip_id
--   LEFT JOIN trips_w_bookings on fares_clicks.trip_id = trips_w_bookings.trip_id
--   WHERE id_row = 1 #deduplicate fare_ids
--   ),

-- exchange_rates AS
--   (SELECT base,
--            CAST(1/amount AS NUMERIC) AS exchange_rate_from_usd,
--            CAST(amount AS NUMERIC) AS exchange_rate_to_usd,
--            cast(effective AS DATE) AS effective
--    FROM `analytics.exchange_rates*`
--    WHERE _TABLE_SUFFIX >= '20201201'
--      AND quote = 'USD'),

-- # flight_id in fare_calculations is trip_id
-- fare_calculations_raw AS
--   (SELECT DISTINCT *
--    EXCEPT (fare_basis_codes,
--            payment_gateway_fee_ids,
--            vendor_fee_ids),
--    final_total_usd/final_total AS exchange_rate_to_usd,
--    final_total/final_total_usd AS exchange_rate_from_usd
--    FROM `wego-cloud.services_curiosity.fare_calculations*`
--    /*
--    WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 2 DAY))
--    */
--    WHERE _table_suffix BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY)) AND FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 30 DAY))
--    #WHERE _table_suffix = '20210131'
--    #WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(@run_date, INTERVAL 1 DAY))
--    ),

-- fare_calculations AS
--   (SELECT 
--    fare_calculations_raw.* EXCEPT(exchange_rate_to_usd, exchange_rate_from_usd),
--    (final_total - original_total) AS markup_amount,
--    (final_total - original_total) * COALESCE(fare_calculations_raw.exchange_rate_to_usd, exchange_rates.exchange_rate_to_usd) AS markup_amount_usd,
--    (final_total - original_total)/original_total * 100 AS markup_percentage,
--    CASE WHEN action = 'no_action' THEN final_total_usd ELSE original_total * COALESCE(fare_calculations_raw.exchange_rate_to_usd, exchange_rates.exchange_rate_to_usd) END AS original_total_usd,
--    net_margin * COALESCE(fare_calculations_raw.exchange_rate_to_usd, exchange_rates.exchange_rate_to_usd) AS net_margin_usd,
--    CASE
--     WHEN (final_total - original_total)/original_total * 100 < 0 THEN max_margin_percentage * final_total
--     WHEN (final_total - original_total)/original_total * 100 > 0 THEN min_margin_percentage * final_total
--     ELSE net_margin
--    END AS new_margin,
--    CASE
--     WHEN (final_total - original_total)/original_total * 100 < 0 THEN max_margin_percentage * final_total_usd
--     WHEN (final_total - original_total)/original_total * 100 > 0 THEN min_margin_percentage * final_total_usd
--     ELSE net_margin * COALESCE(fare_calculations_raw.exchange_rate_to_usd, exchange_rates.exchange_rate_to_usd)
--    END AS new_margin_usd,
--    CASE
--     WHEN (final_total - original_total)/original_total * 100 < 0 THEN max_margin_percentage
--     WHEN (final_total - original_total)/original_total * 100 > 0 THEN min_margin_percentage
--     ELSE net_margin_percentage
--    END AS new_margin_percentage,
--    CAST(COALESCE(fare_calculations_raw.exchange_rate_from_usd, exchange_rates.exchange_rate_from_usd) AS NUMERIC) AS exchange_rate_from_usd,
--    CAST(COALESCE(fare_calculations_raw.exchange_rate_to_usd, exchange_rates.exchange_rate_to_usd) AS NUMERIC) AS exchange_rate_to_usd
--    FROM fare_calculations_raw
--    LEFT JOIN exchange_rates ON DATE(fare_calculations_raw.created_at) = exchange_rates.effective AND fare_calculations_raw.currency_code = exchange_rates.base
--           ),

-- # get wego fares flight_id/trip_id
-- wego_fares_trips AS
--   (SELECT flight_id FROM fare_calculations),

-- /*
-- join fares, trips and fare_calculations
-- not ALL competitors fares are inside, only inside if they share the same flight/trip_id
-- */
-- t1 AS
--   (SELECT
--   TIMESTAMP_ADD(fares_final.created_at, INTERVAL 8 HOUR) AS created_at, # convert to SGT
--   fares_final.fare_id,
--   -- fares_final.branded_fare_id,
--   trips_final.trip_id,
--   -- fares_final.click_id,
--   fare_calculations.* 
--   EXCEPT (
--           created_at, 
--           flight_id,
--           fare_id),
--   trips_final.* 
--   EXCEPT (trip_id),
--   fares_final.* 
--   EXCEPT(
--          created_at,
--          trip_id,
--          fare_id,
--          search_id),
--   FROM
--   fares_final
--   INNER JOIN trips_final ON fares_final.trip_id = trips_final.trip_id
--   # left join to keep competitors' fares
--   LEFT JOIN fare_calculations ON fares_final.fare_id = fare_calculations.fare_id
--   # filter for fares that share wego fares trip_id
--   WHERE fares_final.trip_id IN (SELECT * FROM wego_fares_trips)),

-- # get each trip's minimum fare
-- t2 AS
--   (SELECT 
--    *,
--    MIN(fare_amount_usd) OVER(PARTITION BY trip_id) AS trip_min_fare_amount_usd
--    FROM t1),

-- # determine if its fare is the cheapest fare
-- t3 AS
--   (SELECT 
--    *,
--    CASE 
--     WHEN fare_amount_usd = trip_min_fare_amount_usd THEN TRUE
--     ELSE FALSE
--    END AS trip_if_cheapest_provider_code
--    FROM t2),

-- # get count of each trip's providers, and count of providers which have the cheapest fare, can be more than 1
-- t4 AS
--   (SELECT 
--    *,
--    COUNT(provider_code) OVER(PARTITION BY trip_id) AS trip_provider_code_count,
--    COUNT(CASE 
--           WHEN trip_if_cheapest_provider_code THEN 1
--           ELSE NULL
--          END) OVER(PARTITION BY trip_id) AS trip_cheapest_provider_code_count

--    FROM t3),

-- # concat all fares in ascending order
-- t5 AS
--   (SELECT 
--    *,
--    STRING_AGG(CAST(fare_amount_usd AS STRING), ',') 
--     OVER(PARTITION BY trip_id ORDER BY fare_amount_usd RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS trip_fare_usd_agg
--    FROM t4),

-- # if it's the only cheapest provider, take the next cheapest, else take the cheapest fare, auto null if it's the trip's only fare
-- t6 AS
--   (SELECT 
--    *,
--    CASE
--      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1 THEN CAST(SPLIT(trip_fare_usd_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC) 
--      ELSE trip_min_fare_amount_usd 
--    END AS trip_min_fare_amount_usd_excluding_itself
--    FROM t5),

-- t7 AS
--   (SELECT 
--    *,
--    trip_min_fare_amount_usd_excluding_itself * exchange_rate_from_usd AS trip_min_fare_amount_excluding_itself,
--    CASE
--     # NULL when it's the only cheapest provider
--      WHEN trip_if_cheapest_provider_code AND trip_provider_code_count = 1 THEN NULL
--     # 0 parity when it's not the only cheapest provider
--      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count > 1 THEN 0
--      ELSE (fare_amount_usd - trip_min_fare_amount_usd_excluding_itself)/fare_amount_usd * 100
--    END AS fare_parity_pct,
--    CASE
--      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1 THEN fare_amount_usd
--      ELSE trip_min_fare_amount_usd * 99.98/100
--    END AS recommended_selling_price_usd
--    FROM t6),

-- t8 AS
--   (SELECT *
--    EXCEPT (
--            id_row,
--            trip_legs,
--            trip_segments,
--            trip_timings,
--            trip_departure_times,
--            trip_arrival_times,
--            trip_cabin_names,
--            trip_designator_codes,
--            trip_dates,
--            trip_airport_names,
--            trip_city_codes,
--            trip_city_names,
--            trip_country_codes,
--            trip_country_names,
--            trip_airline_codes,
--            trip_airline_names,
--            trip_fare_usd_agg,
--            trip_provider_code_count,
--            trip_cheapest_provider_code_count),
--    (net_margin_usd + (recommended_selling_price_usd - fare_amount_usd)) / recommended_selling_price_usd AS recommended_margin_percentage,
--    (original_total - trip_min_fare_amount_excluding_itself)/original_total * 100 AS fare_price_diff_original,
--    (final_total - trip_min_fare_amount_excluding_itself)/final_total * 100 AS fare_price_diff_new,
--    DENSE_RANK() 
--      OVER(PARTITION BY trip_id 
--      ORDER BY (CASE WHEN provider_code = 'wego.com' THEN original_total_usd ELSE fare_amount_usd END)) AS initial_fare_rank,
--    DENSE_RANK() 
--      OVER(PARTITION BY trip_id 
--      ORDER BY (CASE WHEN provider_code = 'wego.com' THEN final_total_usd ELSE fare_amount_usd END)) AS new_fare_rank,
--    CONCAT(DENSE_RANK() 
--      OVER(PARTITION BY trip_id
--      ORDER BY (CASE WHEN provider_code = 'wego.com' THEN original_total_usd ELSE fare_amount_usd END)), '-', COUNT(trip_id) OVER(PARTITION BY trip_id)) AS initial_fare_rank_count_concat,
--    CONCAT(DENSE_RANK() 
--      OVER(PARTITION BY trip_id 
--      ORDER BY (CASE WHEN provider_code = 'wego.com' THEN final_total_usd ELSE fare_amount_usd END)), '-', COUNT(trip_id) OVER(PARTITION BY trip_id)) AS new_fare_rank_count_concat,
--    FROM t7
--    WHERE # remove routes which has duplicate 1st and 2nd segment, need to use IFNULL because it's not possible to compare with NULL
--    TRIM(SPLIT(trip_airport_codes, ':')[safe_ordinal(1)]) != IFNULL(TRIM(SPLIT(trip_airport_codes, ':')[safe_ordinal(2)]),'null')),

-- # filter for wego fares
-- t9 AS
-- (
-- SELECT * FROM t8
-- WHERE provider_code = 'wego.com')

-- SELECT
-- created_at,
-- date(created_at) as date,
-- fare_id,
-- trip_id,
-- provider_fare_id,
-- provider_fare_commission_id,
-- search_id,
-- fare_ipcc,
-- booking_ipcc,
-- search_site_code,
-- validating_airline_code,
-- original_total,
-- original_total_base,
-- original_total_tax,
-- gds_commission,
-- gds_commission_id,
-- adult_total_commission,
-- adult_total_iata,
-- adult_total_plb,
-- child_total_commission,
-- child_total_iata,
-- child_total_plb,
-- vendor_commission_id,
-- vendor_commission_rbd,
-- payment_gateway_fee,
-- vendor_fee,
-- net_margin,
-- net_margin_percentage,
-- min_margin_percentage,
-- max_margin_percentage,
-- booking_margin_id,
-- booking_margin_rbd,
-- final_total_usd,
-- final_total,
-- final_total_base,
-- final_total_tax,
-- currency_code,
-- action,
-- markup_amount,
-- markup_amount_usd,
-- markup_percentage,
-- original_total_usd,
-- net_margin_usd,
-- new_margin,
-- new_margin_usd,
-- new_margin_percentage,
-- exchange_rate_from_usd,
-- exchange_rate_to_usd,
-- first_designator_code,
-- designator_codes,
-- departure_date,
-- arrival_date,
-- first_departure_airport_name,
-- first_departure_airport_code,
-- first_arrival_airport_name,
-- first_arrival_airport_code,
-- first_departure_city_name,
-- first_departure_city_code,
-- first_arrival_city_name,
-- first_arrival_city_code,
-- first_departure_country_name,
-- first_departure_country_code,
-- first_arrival_country_name,
-- first_arrival_country_code,
-- first_airline_code,
-- first_cabin_class,
-- first_stop_count,
-- stop_count,
-- trip_airport_codes,
-- provider_code,
-- provider_name,
-- fare_amount,
-- fare_amount_usd,
-- provider_type,
-- adults_count,
-- children_count,
-- infants_count,
-- total_pax_count,
-- site_code,
-- trip_type,
-- trip_category,
-- trip_duration,
-- click,
-- conversions_tracked,
-- fare_impression,
-- trip_min_fare_amount_usd,
-- trip_if_cheapest_provider_code,
-- trip_min_fare_amount_usd_excluding_itself,
-- trip_min_fare_amount_excluding_itself,
-- fare_parity_pct,
-- recommended_selling_price_usd,
-- recommended_margin_percentage,
-- fare_price_diff_original,
-- fare_price_diff_new,
-- initial_fare_rank,
-- new_fare_rank,
-- initial_fare_rank_count_concat,
-- new_fare_rank_count_concat
-- FROM t9
-- where fare_impression != "no impression"


-- daily append:

# generate table of airport + city + country info to be used later
WITH 
airport_places AS (
  SELECT
    airports.* EXCEPT(location_id),
    places.*
  FROM (
    SELECT 
      code       AS airport_code,
      base_name  AS airport_name,
      location_id
    FROM `wego-cloud.place_services.airports`
  ) AS airports
  LEFT JOIN (
    SELECT
      location.id        AS location_id,
      location.code      AS city_code, 
      location.base_name AS city_name,
      cou.code           AS country_code,
      cou.base_name      AS country_name
    FROM `place_services.locations` AS location
    LEFT JOIN `place_services.countries` AS cou ON cou.id = location.country_id
  ) AS places USING (location_id)
),

trips AS (
  SELECT *
  FROM `wego-cloud.services_curiosity.trips*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) = 1
),

flatten_segments AS (
  SELECT 
    trips.id                                                        AS trip_id,
    trips.created_at                                                AS created_at,
    trips.code                                                      AS designator_codes,
    legs.order + 1                                                  AS legs_order,
    segments.order + 1                                              AS segments_order,
    MIN(DATE(departure_time)) OVER (PARTITION BY trips.id)          AS departure_date,
    MAX(DATE(arrival_time))   OVER (PARTITION BY trips.id)          AS arrival_date,
    segments.* EXCEPT(`order`)
  FROM trips, UNNEST(legs) AS legs, UNNEST(legs.segments) AS segments
),

-- removed all CONCAT columns here, build them directly in trips_final STRING_AGG
flatten_segments_2 AS (
  SELECT
    flatten_segments.*,
    departure_airport.airport_name  AS departure_airport_name,
    departure_airport.airport_code  AS departure_airport_code,
    departure_airport.city_name     AS departure_city_name,
    departure_airport.city_code     AS departure_city_code,
    departure_airport.country_name  AS departure_country_name,
    departure_airport.country_code  AS departure_country_code,
    arrival_airport.airport_name    AS arrival_airport_name,
    arrival_airport.airport_code    AS arrival_airport_code,
    arrival_airport.city_name       AS arrival_city_name,
    arrival_airport.city_code       AS arrival_city_code,
    arrival_airport.country_name    AS arrival_country_name,
    arrival_airport.country_code    AS arrival_country_code,
    airline.code                    AS airline_code
  FROM flatten_segments
  LEFT JOIN airport_places AS departure_airport 
    ON flatten_segments.departure_airport.code = departure_airport.airport_code
  LEFT JOIN airport_places AS arrival_airport   
    ON flatten_segments.arrival_airport.code = arrival_airport.airport_code
),

flatten_segments_3 AS (
  SELECT
    *,
    -- departure window: ORDER BY departure_time ASC, take first
    FIRST_VALUE(IF(legs_order = 1, designator_code,       NULL) IGNORE NULLS) OVER w_dep AS first_designator_code,
    FIRST_VALUE(IF(legs_order = 1, departure_airport_name,NULL) IGNORE NULLS) OVER w_dep AS first_departure_airport_name,
    FIRST_VALUE(IF(legs_order = 1, departure_airport_code,NULL) IGNORE NULLS) OVER w_dep AS first_departure_airport_code,
    FIRST_VALUE(IF(legs_order = 1, departure_city_name,   NULL) IGNORE NULLS) OVER w_dep AS first_departure_city_name,
    FIRST_VALUE(IF(legs_order = 1, departure_city_code,   NULL) IGNORE NULLS) OVER w_dep AS first_departure_city_code,
    FIRST_VALUE(IF(legs_order = 1, departure_country_name,NULL) IGNORE NULLS) OVER w_dep AS first_departure_country_name,
    FIRST_VALUE(IF(legs_order = 1, departure_country_code,NULL) IGNORE NULLS) OVER w_dep AS first_departure_country_code,
    FIRST_VALUE(IF(legs_order = 1, airline_code,          NULL) IGNORE NULLS) OVER w_dep AS first_airline_code,
    FIRST_VALUE(IF(legs_order = 1, cabin,                 NULL) IGNORE NULLS) OVER w_dep AS first_cabin_class,
    -- arrival window: ORDER BY arrival_time DESC + FIRST_VALUE replaces LAST_VALUE ASC
    FIRST_VALUE(IF(legs_order = 1, arrival_airport_name,  NULL) IGNORE NULLS) OVER w_arr AS first_arrival_airport_name,
    FIRST_VALUE(IF(legs_order = 1, arrival_airport_code,  NULL) IGNORE NULLS) OVER w_arr AS first_arrival_airport_code,
    FIRST_VALUE(IF(legs_order = 1, arrival_city_name,     NULL) IGNORE NULLS) OVER w_arr AS first_arrival_city_name,
    FIRST_VALUE(IF(legs_order = 1, arrival_city_code,     NULL) IGNORE NULLS) OVER w_arr AS first_arrival_city_code,
    FIRST_VALUE(IF(legs_order = 1, arrival_country_name,  NULL) IGNORE NULLS) OVER w_arr AS first_arrival_country_name,
    FIRST_VALUE(IF(legs_order = 1, arrival_country_code,  NULL) IGNORE NULLS) OVER w_arr AS first_arrival_country_code,
    -- stop count window: no order needed
    MAX(IF(legs_order = 1, segments_order, NULL)) OVER w_stop - 1 AS first_stop_count
  FROM flatten_segments_2
  WINDOW
    w_dep  AS (PARTITION BY trip_id ORDER BY departure_time ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    w_arr  AS (PARTITION BY trip_id ORDER BY arrival_time   DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
    w_stop AS (PARTITION BY trip_id)
),

trips_final AS (
  SELECT
    trip_id,
    first_designator_code,
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
    first_cabin_class,
    first_stop_count,
    MAX(segments_order) - 1 AS stop_count,
    -- build concat directly here instead of in flatten_segments_2
    STRING_AGG(CONCAT(departure_airport_code, '-', arrival_airport_code), 
               ' : ' ORDER BY legs_order, segments_order ASC) AS trip_airport_codes
  FROM flatten_segments_3
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
),

fares_raw AS (
  SELECT
    created_at,
    search_id,
    trip_id,
    id AS fare_id,
    CASE WHEN provider.code LIKE '%wego.com' THEN 'wego.com' ELSE provider.code END AS provider_code,
    provider.name AS provider_name,
    price.amount     AS fare_amount,
    price.amount_usd AS fare_amount_usd
  FROM `wego-cloud.services_curiosity.fares*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY price.amount ASC) = 1
),

branded_fare_calculations AS (
  SELECT branded_fare_id, fare_id
  FROM `wego-cloud.services_curiosity.branded_fare_calculations*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),

provider_type AS (
  SELECT pos_provider_code, provider_type
  FROM `wego-cloud.flights.providers`
  GROUP BY 1, 2
),

search_requests AS (
  SELECT DISTINCT
    search.id AS search_id,
    search.adults_count,
    search.children_count,
    search.infants_count,
    search.adults_count + search.children_count + search.infants_count AS total_pax_count
  FROM `wego-cloud.services_curiosity.search_requests*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),

searches AS (
  SELECT DISTINCT
    search_id, site_code, trip_type, trip_category, trip_duration
  FROM `wego-cloud.wego_analytics.flights_searches`
  WHERE DATE(_PARTITIONTIME) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
                                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

clicks AS (
  SELECT
    click_id,
    fare_id,
    trip_id,
    1 AS click,
    CASE WHEN conversions_tracked > 0 THEN 1 ELSE conversions_tracked END AS conversions_tracked
  FROM `wego-cloud.wego_analytics.flights_clicks`
  WHERE DATE(_PARTITIONTIME) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
                                 AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

-- replace EXISTS correlated subquery with pre-aggregated CTEs (cheaper in BigQuery)
trips_w_impressions AS (
  SELECT DISTINCT trip_id FROM clicks
),

trips_w_bookings AS (
  SELECT DISTINCT trip_id FROM clicks
  WHERE conversions_tracked > 0
),

fares_clicks AS (
  SELECT
    fares_raw.created_at,
    fares_raw.search_id,
    fares_raw.trip_id,
    fares_raw.fare_id,
    fares_raw.provider_code,
    fares_raw.provider_name,
    fares_raw.fare_amount,
    fares_raw.fare_amount_usd,
    COUNT(DISTINCT clicks.click_id)                  AS click,
    CASE WHEN SUM(clicks.conversions_tracked) > 0 
         THEN 1 ELSE SUM(clicks.conversions_tracked) 
    END                                              AS conversions_tracked
  FROM fares_raw
  LEFT JOIN branded_fare_calculations
    ON fares_raw.fare_id = branded_fare_calculations.fare_id
  LEFT JOIN clicks
    ON CASE WHEN fares_raw.provider_code = 'wego.com'
            THEN branded_fare_calculations.branded_fare_id
            ELSE fares_raw.fare_id
       END = clicks.fare_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

fares_final AS (
  SELECT
    fares_clicks.* EXCEPT (click,
                         conversions_tracked)
                   REPLACE (
      fare_amount     * search_requests.total_pax_count AS fare_amount,
      fare_amount_usd * search_requests.total_pax_count AS fare_amount_usd
    ),
    provider_type.provider_type,
    search_requests.* EXCEPT (search_id),
    searches.*        EXCEPT (search_id),
    fares_clicks.click,
    fares_clicks.conversions_tracked,
    CASE
      WHEN trips_w_bookings.trip_id    IS NOT NULL THEN 'booked impression'
      WHEN trips_w_impressions.trip_id IS NOT NULL THEN 'non-booked impression'
      ELSE 'no impression'
    END AS fare_impression
  FROM fares_clicks
  INNER JOIN search_requests     ON fares_clicks.search_id  = search_requests.search_id
  LEFT JOIN  provider_type       ON fares_clicks.provider_code = provider_type.pos_provider_code
  LEFT JOIN  searches            ON search_requests.search_id  = searches.search_id
  LEFT JOIN  trips_w_impressions ON fares_clicks.trip_id = trips_w_impressions.trip_id
  LEFT JOIN  trips_w_bookings    ON fares_clicks.trip_id = trips_w_bookings.trip_id
),

-- use QUALIFY instead of SELECT DISTINCT * to deduplicate fare_calculations
fare_calculations_raw AS (
  SELECT
    * EXCEPT (fare_basis_codes, payment_gateway_fee_ids, vendor_fee_ids),
    SAFE_DIVIDE(final_total_usd, final_total) AS exchange_rate_to_usd,
    SAFE_DIVIDE(final_total, final_total_usd) AS exchange_rate_from_usd
  FROM `wego-cloud.services_curiosity.fare_calculations*`
  WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY fare_id) = 1
),

fare_calculations AS (
  SELECT
    * EXCEPT (exchange_rate_to_usd, exchange_rate_from_usd),
    (final_total - original_total)                                   AS markup_amount,
    (final_total - original_total) * exchange_rate_to_usd           AS markup_amount_usd,
    SAFE_DIVIDE(final_total - original_total, original_total) * 100  AS markup_percentage,
    CASE WHEN action = 'no_action'
         THEN final_total_usd
         ELSE original_total * exchange_rate_to_usd
    END                                                              AS original_total_usd,
    net_margin * exchange_rate_to_usd                                AS net_margin_usd,
    CASE
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 < 0 THEN max_margin_percentage * final_total
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 > 0 THEN min_margin_percentage * final_total
      ELSE net_margin
    END                                                              AS new_margin,
    CASE
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 < 0 THEN max_margin_percentage * final_total_usd
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 > 0 THEN min_margin_percentage * final_total_usd
      ELSE net_margin * exchange_rate_to_usd
    END                                                              AS new_margin_usd,
    CASE
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 < 0 THEN max_margin_percentage
      WHEN SAFE_DIVIDE(final_total - original_total, original_total) * 100 > 0 THEN min_margin_percentage
      ELSE net_margin_percentage
    END                                                              AS new_margin_percentage,
    CAST(exchange_rate_from_usd AS NUMERIC)                         AS exchange_rate_from_usd,
    CAST(exchange_rate_to_usd   AS NUMERIC)                         AS exchange_rate_to_usd
  FROM fare_calculations_raw
),

t1 AS (
  SELECT DISTINCT
    TIMESTAMP_ADD(fares_final.created_at, INTERVAL 8 HOUR) AS created_at,
    fares_final.fare_id,
    trips_final.trip_id,
    fare_calculations.* EXCEPT (created_at, flight_id, fare_id),
    trips_final.*        EXCEPT (trip_id),
    fares_final.*        EXCEPT (created_at, trip_id, fare_id, search_id),
    CASE WHEN fares_final.provider_code = 'wego.com'
         THEN fare_calculations.original_total_usd
         ELSE fares_final.fare_amount_usd
    END AS initial_price,
    CASE WHEN fares_final.provider_code = 'wego.com'
         THEN fare_calculations.final_total_usd
         ELSE fares_final.fare_amount_usd
    END AS new_price
  FROM fares_final
  INNER JOIN trips_final
    ON fares_final.trip_id = trips_final.trip_id
  LEFT JOIN fare_calculations
    ON fares_final.fare_id = fare_calculations.fare_id
  INNER JOIN (SELECT DISTINCT flight_id FROM fare_calculations) AS wego_trips
    ON fares_final.trip_id = wego_trips.flight_id
  WHERE TRIM(SPLIT(trip_airport_codes, ':')[SAFE_ORDINAL(1)])
     != IFNULL(TRIM(SPLIT(trip_airport_codes, ':')[SAFE_ORDINAL(2)]), 'null')
),

-- pass 1: min fare + provider count
t2 AS (
  SELECT
    *,
    MIN(fare_amount_usd) OVER (PARTITION BY trip_id) AS trip_min_fare_amount_usd,
    COUNT(provider_code) OVER (PARTITION BY trip_id) AS trip_provider_code_count
  FROM t1
),

-- pass 2: cheapest flags + 2nd cheapest (references plain column from t2)
t3 AS (
  SELECT
    *,
    IF(fare_amount_usd = trip_min_fare_amount_usd, TRUE, FALSE) AS trip_if_cheapest_provider_code,
    COUNT(IF(fare_amount_usd = trip_min_fare_amount_usd, 1, NULL))
      OVER (PARTITION BY trip_id)                               AS trip_cheapest_provider_code_count,
    MIN(CASE WHEN fare_amount_usd > trip_min_fare_amount_usd
             THEN fare_amount_usd END)
      OVER (PARTITION BY trip_id)                               AS trip_second_min_fare_usd
  FROM t2
),

t4 AS(
  SELECT
  *,
  CASE
      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
      THEN trip_second_min_fare_usd
      ELSE trip_min_fare_amount_usd
    END                                                          AS trip_min_fare_amount_usd_excluding_itself,
  FROM
  t3
),

-- pass 3: all final metrics + ranking
t5 AS (
  SELECT
    * EXCEPT (initial_price, new_price, trip_second_min_fare_usd),
  trip_min_fare_amount_usd_excluding_itself * exchange_rate_from_usd AS trip_min_fare_amount_excluding_itself,
    -- simplified: removed unreachable inner CASE
    CASE
      WHEN trip_if_cheapest_provider_code AND trip_provider_code_count = 1          THEN NULL
      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count > 1 THEN 0
      ELSE SAFE_DIVIDE(
             fare_amount_usd - trip_min_fare_amount_usd_excluding_itself,
             fare_amount_usd) * 100
    END                                                          AS fare_parity_pct,

    CASE
      WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
      THEN fare_amount_usd
      ELSE SAFE_DIVIDE(trip_min_fare_amount_usd * 99.98, 100)
    END                                                          AS recommended_selling_price_usd,

    SAFE_DIVIDE(
      net_margin_usd + (
        CASE WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
             THEN fare_amount_usd
             ELSE SAFE_DIVIDE(trip_min_fare_amount_usd * 99.98, 100)
        END - fare_amount_usd),
      CASE WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
           THEN fare_amount_usd
           ELSE SAFE_DIVIDE(trip_min_fare_amount_usd * 99.98, 100)
      END)                                                       AS recommended_margin_percentage,

    SAFE_DIVIDE(
      original_total - CASE
        WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
        THEN trip_second_min_fare_usd ELSE trip_min_fare_amount_usd
      END * exchange_rate_from_usd,
      original_total) * 100                                      AS fare_price_diff_original,

    SAFE_DIVIDE(
      final_total - CASE
        WHEN trip_if_cheapest_provider_code AND trip_cheapest_provider_code_count = 1
        THEN trip_second_min_fare_usd ELSE trip_min_fare_amount_usd
      END * exchange_rate_from_usd,
      final_total) * 100                                         AS fare_price_diff_new,

    DENSE_RANK() OVER (PARTITION BY trip_id ORDER BY initial_price) AS initial_fare_rank,
    DENSE_RANK() OVER (PARTITION BY trip_id ORDER BY new_price)     AS new_fare_rank,
    CONCAT(DENSE_RANK() OVER (PARTITION BY trip_id ORDER BY initial_price),
           '-', COUNT(*) OVER (PARTITION BY trip_id))               AS initial_fare_rank_count_concat,
    CONCAT(DENSE_RANK() OVER (PARTITION BY trip_id ORDER BY new_price),
           '-', COUNT(*) OVER (PARTITION BY trip_id))               AS new_fare_rank_count_concat

  FROM t4
),
t_final AS(
  SELECT
  *
  FROM
  t5
  WHERE provider_code = 'wego.com'
)

SELECT
  created_at,
  DATE(created_at)                 AS date,
  fare_id,
  trip_id,
  provider_fare_id,
  provider_fare_commission_id,
  search_id,
  fare_ipcc,
  booking_ipcc,
  search_site_code,
  validating_airline_code,
  original_total,
  original_total_base,
  original_total_tax,
  gds_commission,
  gds_commission_id,
  adult_total_commission,
  adult_total_iata,
  adult_total_plb,
  child_total_commission,
  child_total_iata,
  child_total_plb,
  vendor_commission_id,
  vendor_commission_rbd,
  payment_gateway_fee,
  vendor_fee,
  net_margin,
  net_margin_percentage,
  min_margin_percentage,
  max_margin_percentage,
  booking_margin_id,
  booking_margin_rbd,
  final_total_usd,
  final_total,
  final_total_base,
  final_total_tax,
  currency_code,
  action,
  markup_amount,
  markup_amount_usd,
  markup_percentage,
  original_total_usd,
  net_margin_usd,
  new_margin,
  new_margin_usd,
  new_margin_percentage,
  exchange_rate_from_usd,
  exchange_rate_to_usd,
  first_designator_code,
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
  first_cabin_class,
  first_stop_count,
  stop_count,
  trip_airport_codes,
  provider_code,
  provider_name,
  fare_amount,
  fare_amount_usd,
  provider_type,
  adults_count,
  children_count,
  infants_count,
  total_pax_count,
  site_code,
  trip_type,
  trip_category,
  trip_duration,
  click,
  conversions_tracked,
  fare_impression,
  trip_min_fare_amount_usd,
  trip_if_cheapest_provider_code,
  trip_min_fare_amount_usd_excluding_itself,
  trip_min_fare_amount_excluding_itself,
  fare_parity_pct,
  recommended_selling_price_usd,
  recommended_margin_percentage,
  fare_price_diff_original,
  fare_price_diff_new,
  initial_fare_rank,
  new_fare_rank,
  initial_fare_rank_count_concat,
  new_fare_rank_count_concat
FROM t_final
WHERE fare_impression != 'no impression'
{% endraw %}
