{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : BoWF_supplier_fare_analysis
-- Destination: analysis.bow_flights_supplier_fare_analysis  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('place_services', 'airports') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
-- depends_on: {{ source('services_curiosity', 'branded_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'provider_fare_calculations') }}
-- depends_on: {{ source('services_curiosity', 'trips') }}
{% raw %}
WITH
  fares AS (
    WITH
      fares AS (
        SELECT
          *,
          row_number()
            OVER (PARTITION BY normalized_trip_id ORDER BY final_total_usd ASC)
            AS cheapest_ipcc_rank
        FROM
          (
            -- deduplication step getting one fare for each fare_ipcc by created_at
            SELECT * EXCEPT (`dedup`)
            FROM
              (
                SELECT DISTINCT
                  concat(search_id, ":", flight_id) AS normalized_trip_id,
                  id,
                  provider_fare_id,
                  selected,
                  search_id,
                  flight_id,
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
                  created_at,
                  row_number()
                    OVER (
                      PARTITION BY concat(search_id, ":", flight_id), fare_ipcc
                      ORDER BY created_at DESC
                    ) AS dedup,
                FROM `services_curiosity.provider_fare_calculations*` AS aa
                INNER JOIN
                  (
                    SELECT
                      concat(search_id, ":", flight_id) AS normalized_trip_id,
                      COUNT(DISTINCT fare_ipcc) AS count_
                    FROM `services_curiosity.provider_fare_calculations*`
                    WHERE
                      _table_suffix = (
                        SELECT
                          format(
                            '%s',
                            format_date(
                              "%Y%m%d",
                              date_sub(current_date(), INTERVAL 1 day)))
                      )
                    GROUP BY 1
                  ) AS base
                  ON
                    base.normalized_trip_id = concat(search_id, ":", flight_id)
                    AND base.count_ > 1
                WHERE
                  _table_suffix = (
                    SELECT
                      format(
                        '%s',
                        format_date(
                          "%Y%m%d", date_sub(current_date(), INTERVAL 1 day)))
                  )
              ) AS aa
            WHERE aa.dedup = 1
          )
      ),
      first_win AS (
        SELECT
          normalized_trip_id,
          provider_fare_id,
          id,
          fare_ipcc AS cheapest_ipcc,
          final_total_usd AS cheapest_total_usd,
          net_margin,
          net_margin_percentage,
          min_margin_percentage,
          max_margin_percentage,
          booking_margin_id,
          booking_margin_rbd,
          final_total,
          final_total_base,
          final_total_tax,
          currency_code,
          action
        FROM fares
        WHERE
          cheapest_ipcc_rank = 1
          -- this is to make sure we only take searches/fare that were returned by multiple IPCCs
          AND normalized_trip_id IN (
            SELECT normalized_trip_id FROM fares WHERE cheapest_ipcc_rank > 1
          )
      ),
      second_win AS (
        SELECT
          normalized_trip_id,
          provider_fare_id,
          id,
          fare_ipcc,
          final_total_usd AS second_cheapest_total_usd,
          net_margin,
          net_margin_percentage,
          min_margin_percentage,
          max_margin_percentage,
          booking_margin_id,
          booking_margin_rbd,
          final_total,
          final_total_base,
          final_total_tax,
          currency_code,
          action
        FROM fares
        WHERE
          cheapest_ipcc_rank = 2
          -- this is to make sure we only take searches/fare that were returned by multiple IPCCs
          AND normalized_trip_id IN (
            SELECT normalized_trip_id FROM fares WHERE cheapest_ipcc_rank > 1
          )
      )
    SELECT
      date(ff.created_at) AS created_at,
      ff.*
        EXCEPT (
          cheapest_ipcc_rank,
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
          final_total,
          final_total_base,
          final_total_tax,
          currency_code,
          action,
          created_at),
      ff.cheapest_ipcc_rank AS pricing_rank,
      cheapest_ipcc,
      cheapest_total_usd,
      final_total_usd
        - IF(
          cheapest_ipcc_rank = 1, second_cheapest_total_usd, cheapest_total_usd)
        AS price_diff_cheapest_ipcc,
      (
        (final_total_usd / IF(cheapest_ipcc_rank = 1, NULL, cheapest_total_usd))
        - 1)
        * 100 AS percent_diff_cheapest_ipcc,
      -- NEW: for the cheapest fare, how far BELOW the 2nd cheapest it is (positive = bigger lead)
      IF(
        cheapest_ipcc_rank = 1,
        (1 - final_total_usd / NULLIF(second_cheapest_total_usd, 0)) * 100,
        NULL) AS percent_lead_vs_second_cheapest_ipcc
    FROM fares AS ff
    LEFT JOIN first_win AS fw
      ON fw.normalized_trip_id = ff.normalized_trip_id
    LEFT JOIN second_win AS sw
      ON sw.normalized_trip_id = ff.normalized_trip_id
    WHERE
      ff.normalized_trip_id IN (
        SELECT normalized_trip_id FROM fares WHERE cheapest_ipcc_rank > 1
      )
  ),
  itinerary_clicked AS(
    SELECT 
    DISTINCT
    REGEXP_REPLACE(flight_id, r'~\d{4}~\d{4}', '') AS normalized_trip_id
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
  ),
  trips AS (
    WITH
      airport_places AS (
        SELECT
          airports.* EXCEPT (location_id), places.*
        FROM
          (
            SELECT
              code AS airport_code,
              base_name AS airport_name,
              location_id AS location_id
            FROM `wego-cloud.place_services.airports`
          ) AS airports
        LEFT JOIN
          (
            SELECT
              location.id AS location_id,
              location.code AS city_code,
              location.base_name AS city_name,
              cou.code AS country_code,
              cou.base_name AS country_name
            FROM `place_services.locations` AS location
            LEFT JOIN `place_services.countries` AS cou
              ON cou.id = location.country_id
          ) AS places
          ON airports.location_id = places.location_id
      ),
      trips_fares AS (
        SELECT
          trips.id AS trip_id,
          normalized_trip_id,
          trips.code AS designator_codes,
          trips.created_at,
          legs.order + 1 AS legs_order,
          segments.order + 1 AS segments_order,
          min(date(departure_time))
            OVER (PARTITION BY trips.id) AS departure_date,
          max(date(arrival_time)) OVER (PARTITION BY trips.id) AS arrival_date,
          segments.* EXCEPT (`order`)
        FROM
          (
            SELECT
              sct.*,
              concat(
                sct.search_id,
                ":",
                REGEXP_REPLACE(sct.code, r'~\d{4}~\d{4}', ''))
                AS normalized_trip_id,
              row_number()
                OVER (PARTITION BY sct.id ORDER BY sct.created_at DESC)
                AS trip_id_rank
            FROM `wego-cloud.services_curiosity.trips*` AS sct
            WHERE
              concat(
                sct.search_id, ":", REGEXP_REPLACE(code, r'~\d{4}~\d{4}', ''))
                IN (
                  SELECT DISTINCT normalized_trip_id
                  FROM fares
                  WHERE
                    date(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                )
              AND _table_suffix = (
                SELECT
                  format(
                    '%s',
                    format_date(
                      "%Y%m%d", date_sub(current_date(), INTERVAL 1 day)))
              )
          ) AS trips,
          UNNEST(legs) AS legs,
          UNNEST(legs.segments) AS segments
        WHERE trips.trip_id_rank = 1
        ORDER BY trip_id, created_at ASC, legs.order ASC, segments.order ASC
      ),
      departure_arrival_name_mapping AS (
        SELECT
          trips_fares.*,
          dpt.airport_name AS departure_airport_name,
          dpt.airport_code AS departure_airport_code,
          arv.airport_name AS arrival_airport_name,
          arv.airport_code AS arrival_airport_code,
          dpt.city_name AS departure_city_name,
          dpt.city_code AS departure_city_code,
          arv.city_name AS arrival_city_name,
          arv.city_code AS arrival_city_code,
          dpt.country_name AS departure_country_name,
          dpt.country_code AS departure_country_code,
          arv.country_name AS arrival_country_name,
          arv.country_code AS arrival_country_code,
          airline.code AS airline_code,
          CONCAT(departure_time, ' - ', arrival_time) AS segment_timings,
          CONCAT(date(departure_time), ' - ', date(arrival_time))
            AS segment_dates,
          CONCAT(
            trips_fares.departure_airport.code,
            '-',
            trips_fares.arrival_airport.code) AS segment_airport_codes,
          CONCAT(dpt.airport_name, ' - ', arv.airport_name)
            AS segment_airport_names,
          CONCAT(dpt.city_code, ' - ', arv.city_code) AS segment_city_codes,
          CONCAT(dpt.city_name, ' - ', arv.city_name) AS segment_city_names,
          CONCAT(dpt.country_code, ' - ', arv.country_code)
            AS segment_country_codes,
          CONCAT(dpt.country_name, ' - ', arv.country_name)
            AS segment_country_names
        FROM trips_fares
        LEFT JOIN airport_places AS dpt
          ON trips_fares.departure_airport.code = dpt.airport_code
        LEFT JOIN airport_places AS arv
          ON trips_fares.arrival_airport.code = arv.airport_code
      ),
      first_arrival_departure_segmentation AS (
        SELECT
          *,
          first_value(
            IF(legs_order = 1, departure_airport_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_airport_name,
          first_value(
            IF(legs_order = 1, departure_airport_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_airport_code,
          last_value(
            IF(legs_order = 1, arrival_airport_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_airport_name,
          last_value(
            IF(legs_order = 1, arrival_airport_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_airport_code,
          first_value(
            IF(legs_order = 1, departure_city_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_city_name,
          first_value(
            IF(legs_order = 1, departure_city_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_city_code,
          last_value(IF(legs_order = 1, arrival_city_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_city_name,
          last_value(IF(legs_order = 1, arrival_city_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_city_code,
          first_value(
            IF(legs_order = 1, departure_country_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_country_name,
          first_value(
            IF(legs_order = 1, departure_country_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_departure_country_code,
          last_value(
            IF(legs_order = 1, arrival_country_name, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_country_name,
          last_value(
            IF(legs_order = 1, arrival_country_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY arrival_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_arrival_country_code,
          first_value(IF(legs_order = 1, airline_code, NULL) IGNORE NULLS)
            OVER (
              PARTITION BY trip_id
              ORDER BY departure_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_airline_code,
          MAX(IF(legs_order = 1, segments_order, NULL))
            OVER (PARTITION BY trip_id) - 1
            AS first_stop_count  # total segments - 1 = stops within the leg
        FROM departure_arrival_name_mapping
      )
    SELECT DISTINCT
      trip_id,
      normalized_trip_id,
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
      STRING_AGG(CAST(legs_order AS STRING), ' : ' ORDER BY legs_order ASC)
        AS trip_legs,
      STRING_AGG(
        CAST(segments_order AS STRING),
        ' : ' ORDER BY legs_order, segments_order ASC) AS trip_segments,
      STRING_AGG(
        segment_airport_codes, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_airport_codes,
      STRING_AGG(
        segment_airport_names, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_airport_names,
      STRING_AGG(
        segment_city_codes, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_city_codes,
      STRING_AGG(
        segment_city_names, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_city_names,
      STRING_AGG(
        segment_country_codes, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_country_codes,
      STRING_AGG(
        segment_country_names, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_country_names,
      STRING_AGG(airline.code, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_airline_codes,
      STRING_AGG(airline.name, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_airline_names,
      STRING_AGG(
        CAST(departure_time AS STRING),
        ' : ' ORDER BY legs_order, segments_order ASC) AS trip_depature_times,
      STRING_AGG(
        CAST(arrival_time AS STRING),
        ' : ' ORDER BY legs_order, segments_order ASC) AS trip_arrival_times,
      STRING_AGG(cabin, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_cabin_names,
      STRING_AGG(designator_code, ' : ' ORDER BY legs_order, segments_order ASC)
        AS trip_designator_codes
    FROM first_arrival_departure_segmentation
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
  )
SELECT *
FROM
  (
    SELECT
      ff.*,
      tf.* EXCEPT (normalized_trip_id, trip_id),
      CASE
      WHEN ff.normalized_trip_id=ic.normalized_trip_id THEN 1
      ELSE 0
      END AS is_clicked
    FROM fares AS ff
    INNER JOIN trips AS tf
      ON tf.normalized_trip_id = ff.normalized_trip_id
    LEFT JOIN itinerary_clicked ic
    ON ff.normalized_trip_id=ic.normalized_trip_id
  )
{% endraw %}
