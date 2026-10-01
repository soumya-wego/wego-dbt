{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotels_margins_rules_export_daily_run
-- Destination: analysis.hotels_margins_rules_export  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('analysis', 'hotels_margins_rules_cos') }}
-- depends_on: {{ source('analysis', 'hotels_margins_rules_scope') }}
-- depends_on: {{ ref('wego_rates_analysis') }}
-- depends_on: {{ source('analytics', 'countries_misc') }}
-- depends_on: {{ source('hotels', 'provider_hotels') }}
-- depends_on: {{ source('wego_analytics', 'hotels_searches') }}
{% raw %}
# daily run for updating hotels_margins_rules_export table v2
/*
changelog
v2 changes 23/06/2021;
- added join to hotels_margins_rules_COS view to retrieve hard-coded COS by AJ
*/

/*
- Clinton took this directly from pricing analysis charts and made additional CTEs to calculate, but he did not have time to trim it
- this script can be trimmed down by removing uncessary calculations
*/

  WITH wego_rates_analysis AS
    (SELECT * EXCEPT(conversions_tracked),
            CASE
              WHEN conversions_tracked > 0 THEN 1
              WHEN COUNT(conversions_tracked) OVER(PARTITION BY search_hotel_id) >= 0 THEN 0
            ELSE conversions_tracked
            END AS conversions_tracked
     FROM `wego-cloud.analysis.wego_rates_analysis`
     WHERE created_at BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND check_in BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_ADD(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR)
      AND check_out BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_ADD(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR)
      AND hotel_name IS NOT NULL
      AND hotel_provider_count > 1),
    
       hotel_rate_parity_t1 AS
    (SELECT search_hotel_id,
            provider_code,
            MIN(total_amount_per_night) AS total_amount_per_night, /* minimum amount per provider on hotel level */
            MIN(MIN(total_amount_per_night)) OVER(PARTITION BY search_hotel_id) AS hotel_min_rate, /* minimum amount per hotel */
            COUNT(DISTINCT click_id) AS hotel_clicks,
     FROM wego_rates_analysis
     GROUP BY 1,
              2),
    
       hotel_rate_parity_t2 AS
    (SELECT *,
            CASE 
              WHEN total_amount_per_night = hotel_min_rate THEN TRUE
              ELSE FALSE
            END AS is_hotel_cheapest_provider,
       COUNT(DISTINCT IF(total_amount_per_night = hotel_min_rate, provider_code, NULL)) OVER (PARTITION BY search_hotel_id) AS hotel_cheapest_provider_count,
       STRING_AGG(CAST(total_amount_per_night AS STRING), ',') OVER (PARTITION BY search_hotel_id ORDER BY total_amount_per_night RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hotel_rate_agg
      FROM hotel_rate_parity_t1),
    
       hotel_rate_parity_t3 AS
    (SELECT *,
            IF(is_hotel_cheapest_provider AND hotel_cheapest_provider_count = 1, CAST(SPLIT(hotel_rate_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC), hotel_min_rate) as hotel_min_rate_exclude_own_rate
     FROM hotel_rate_parity_t2),
    
       hotel_rate_parity_t4 AS
    (SELECT *,
            CASE
            -- WHEN hotel_provider_count = 1 THEN NULL /* if it's the only provider then NULL, but this case should have been removed */
              WHEN is_hotel_cheapest_provider AND hotel_cheapest_provider_count > 1 THEN 0 /* if >1 provider being cheapest then 0 parity */
              ELSE (total_amount_per_night - hotel_min_rate_exclude_own_rate) / total_amount_per_night * 100
            END hotel_rate_parity_pct
     FROM hotel_rate_parity_t3),
    
       hotel_rate_parity AS
    (SELECT *,
            CASE
              WHEN hotel_rate_parity_pct < 0 THEN 1 /* 1 if it's the winner AND there's more than 1 provider */
              WHEN hotel_rate_parity_pct >= 0 THEN 0 /* 0 if it's not the winner */
              ELSE NULL /* NULL if it's the only provider */
            END AS hotel_rate_won
            -- IF(hotel_rate_parity_pct IS NULL or hotel_rate_parity_pct < 0, 1, 0) as hotel_rate_won, /* 1 if it's the only provider OR the winner, 0 otherwise */
     FROM hotel_rate_parity_t4),
    
       countries_misc AS
    (SELECT country_code,
            market
     FROM analytics.countries_misc),
    
       searches AS
    (SELECT search_id,
            device_type,
     FROM wego_analytics.hotels_searches
     WHERE DATE(_PARTITIONTIME) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
    
       base AS
    (SELECT wego_rates_analysis.search_hotel_id,
            search_hotel_room_id,
            wego_rates_analysis.site_code,
            click_id,
            hotel_id,
            hotel_name,
            wego_rates_analysis.provider_code,
            check_in,
            check_out,
            wego_rates_analysis.total_amount_per_night,
            wego_rates_analysis.conversions_tracked,
            hotel_provider_count,
            is_hotel_cheapest_provider,
            hotel_rate_parity.hotel_rate_parity_pct,
            hotel_rate_parity.hotel_rate_won,
            hotel_room_rate_parity_pct,
            hotel_room_rate_won
     FROM wego_rates_analysis
     LEFT JOIN hotel_rate_parity ON wego_rates_analysis.search_hotel_id = hotel_rate_parity.search_hotel_id
      AND wego_rates_analysis.provider_code = hotel_rate_parity.provider_code
     LEFT JOIN searches ON wego_rates_analysis.search_id = searches.search_id
     LEFT JOIN countries_misc AS site_code_legend ON wego_rates_analysis.site_code = site_code_legend.country_code
     LEFT JOIN countries_misc AS destination_legend ON wego_rates_analysis.country_code = destination_legend.country_code
     WHERE 1 = 1
       AND guests_count BETWEEN 1 AND 10),
    
       t0 AS
    (SELECT DISTINCT hotel_name, 
                     hotel_id,
                     provider_code,
                     search_hotel_id,
                     hotel_rate_won,
                     site_code
     FROM base),
    
       t01 AS
    (SELECT site_code,
            hotel_name, 
            hotel_id,
            provider_code,
            SAFE_DIVIDE(SUM(hotel_rate_won), 
            COUNT(DISTINCT search_hotel_id)) AS provider_hotel_rate_win_rate
     FROM t0
     GROUP BY hotel_name, hotel_id, provider_code, site_code),
    
       t02 AS
    (SELECT site_code,
            hotel_id,
            hotel_name,
            provider_code,
            MIN(total_amount_per_night) AS provider_cheapest_rate,
            COUNT(DISTINCT search_hotel_id) AS provider_trips,
            SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN hotel_room_rate_won > 0 THEN search_hotel_room_id ELSE NULL END),COUNT(DISTINCT search_hotel_room_id)) AS provider_hotel_room_rate_win_rate,
            COUNT(DISTINCT click_id) AS provider_clicks,
            SAFE_DIVIDE(COUNT(DISTINCT click_id),SUM(COUNT(DISTINCT click_id)) OVER(PARTITION BY site_code, hotel_id, hotel_name)) AS provider_click_win_rate,
            SUM(conversions_tracked) AS provider_conversions,
            CASE WHEN SUM(SUM(conversions_tracked)) OVER(PARTITION BY site_code, hotel_id, hotel_name) = 0 THEN 0 ELSE
            SAFE_DIVIDE(SUM(conversions_tracked),SUM(SUM(conversions_tracked)) OVER(PARTITION BY site_code, hotel_id, hotel_name)) END AS provider_conversion_win_rate,
            SUM(CASE WHEN provider_code = 'hotels.wego.com' THEN conversions_tracked ELSE 0 END) AS selected_provider_conversions_tracked
     FROM base
     GROUP BY 1,2,3,4),
    
       t1 AS
    (SELECT t02.*, 
            t01.provider_hotel_rate_win_rate
     FROM t02
     LEFT JOIN t01 ON t02.hotel_id = t01.hotel_id 
       AND t02.provider_code = t01.provider_code AND t02.hotel_name = t01.hotel_name AND t02.site_code = t01.site_code),
    
       top_providers AS
    (SELECT provider_code,
            SUM(provider_trips) AS provider_trips,
            SUM(provider_clicks) AS provider_clicks,
            SUM(provider_conversions) AS provider_conversions
     FROM t1
     GROUP BY 1
     ORDER BY provider_clicks DESC
     LIMIT 10),
    
       t2 AS
    (SELECT * EXCEPT (selected_provider_conversions_tracked),
            DENSE_RANK() OVER(ORDER BY site_code, hotel_id, hotel_name) AS primary_key_id,
            SUM(provider_trips) OVER (PARTITION BY site_code, hotel_id, hotel_name) AS total_trips_by_partition,
            SUM(provider_clicks) OVER (PARTITION BY site_code, hotel_id, hotel_name) AS total_clicks_by_partition,
            SUM(provider_conversions) OVER (PARTITION BY site_code, hotel_id, hotel_name) AS total_conversions_by_partition,
            MIN(provider_cheapest_rate) OVER(PARTITION BY site_code, hotel_id, hotel_name) AS cheapest_rate,
            SUM(selected_provider_conversions_tracked) OVER(PARTITION BY site_code, hotel_id, hotel_name) AS selected_provider_conversions_tracked
      FROM t1),
    
       selected_provider_code_pk AS
    (SELECT primary_key_id
     FROM t2
     WHERE provider_code = 'hotels.wego.com'),
    
       t3 AS
    (SELECT *,
            CASE 
              WHEN provider_cheapest_rate = cheapest_rate THEN provider_code
              ELSE NULL
            END AS cheapest_provider /* there might be more than 1 cheapest provider */
      FROM t2
      WHERE primary_key_id IN (SELECT primary_key_id FROM selected_provider_code_pk)),
    
       t4 AS
    (SELECT *,
            STRING_AGG(cheapest_provider, ',') OVER(PARTITION BY primary_key_id) AS cheapest_providers, /* concat all cheapest providers */
            CASE
              WHEN provider_code = 'hotels.wego.com' THEN provider_cheapest_rate
              ELSE 0 
            END AS selected_provider_code_rate
      FROM t3),
    
       t5 AS
    (SELECT primary_key_id,
            MIN(provider_cheapest_rate) AS not_selected_cheapest_rate
     FROM t2
     WHERE provider_code != 'hotels.wego.com'
     GROUP BY 1),
    
       t6 AS
    (SELECT primary_key_id,
            provider_cheapest_rate AS selected_provider_rate,
            provider_click_win_rate AS selected_provider_click_win_rate,
            provider_conversion_win_rate AS selected_provider_conversion_win_rate,
            provider_hotel_rate_win_rate AS selected_provider_hotel_rate_win_rate,
            provider_hotel_room_rate_win_rate AS selected_provider_hotel_room_rate_win_rate
     FROM t2
     WHERE provider_code = 'hotels.wego.com'),
    
       t7 AS
    (SELECT DISTINCT site_code,
                     t4.primary_key_id,
                     hotel_id,
                     hotel_name,
                     total_clicks_by_partition,
                     total_conversions_by_partition,
                     selected_provider_conversions_tracked,
                     total_trips_by_partition,
                     cheapest_providers,
                     cheapest_rate,
                     not_selected_cheapest_rate,
                     selected_provider_rate,
                     selected_provider_rate - not_selected_cheapest_rate AS parity,
                     ROUND((1 - not_selected_cheapest_rate/selected_provider_rate),4) AS parity_pct,
                     selected_provider_click_win_rate,
                     selected_provider_conversion_win_rate,
                     selected_provider_hotel_rate_win_rate,
                     selected_provider_hotel_room_rate_win_rate,
                     CONCAT(site_code, '-', hotel_id) AS site_code_ref,
     FROM t4 
     LEFT JOIN t5 ON t4.primary_key_id = t5.primary_key_id
     LEFT JOIN t6 ON t4.primary_key_id = t6.primary_key_id),

       L7days_CTE AS
    (SELECT CONCAT(site_code, '-', hotel_id) AS site_code_ref,
            SUM(conversions_tracked) AS L7days_conversions
     FROM `wego-cloud.analysis.wego_rates_analysis`
     # dates between last 7 days and yesterday to get last 7 days
     WHERE created_at BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
       AND check_in BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 6 DAY) AND DATE_ADD(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR)
       AND check_out BETWEEN DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 6 DAY) AND DATE_ADD(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR)
       AND hotel_name IS NOT NULL
       AND hotel_provider_count > 1
       AND provider_code = 'hotels.wego.com'
     GROUP BY 1),
    
       t8 AS 
    (SELECT t7.*,
            CASE
              WHEN parity_pct < 0 THEN 'win'
              WHEN parity_pct <= 0.005 THEN 'match'
              WHEN parity_pct > 0.005 THEN 'lose'
              WHEN parity IS NULL THEN 'only provider'
            END AS win_versus_loss,
            provider_hotels.goquo_hotel_id,
            COALESCE(site_codes_base_markups.fitmdwsnaqc,fallback_base_markups.fitmdwsnaqc) AS base_markup,
            0.2 AS max_markup,
            0.05 AS tolerance_level,
            COALESCE(initial_markups.fitmdwsnaqc, site_codes_base_markups.fitmdwsnaqc, fallback_base_markups.fitmdwsnaqc) AS initial_markup,
            0.01 AS price_leadership,
            25 AS RPH,
            COALESCE(site_codes_COS.COS, fallback_COS.COS) AS COS,
            L7days_CTE.L7days_conversions
     FROM t7
     # join to get goquo hotel id
     LEFT JOIN 
       (SELECT hotel_id AS wego_hotel_id, 
               provider_property_id AS goquo_hotel_id
        FROM `wego-cloud.hotels.provider_hotels` 
        WHERE provider_id = 689) AS provider_hotels ON t7.hotel_id = CAST(provider_hotels.wego_hotel_id AS STRING)
     # join to get base and initial markups for specific site_code_ref, which this join table is different for initial run and subsequent daily runs
     LEFT JOIN 
       (SELECT * 
        FROM `wego-cloud.analysis.hotels_margins_rules_export`
        WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS initial_markups ON t7.site_code = initial_markups.site_code AND provider_hotels.goquo_hotel_id = initial_markups.goquo_hotel_id
     # join to get base and initial markups for site code
     LEFT JOIN `wego-cloud.analysis.hotels_margins_rules_scope` AS site_codes_base_markups ON t7.site_code = site_codes_base_markups.site_code
     # join to get base and initial markups for all
     CROSS JOIN (SELECT * FROM `wego-cloud.analysis.hotels_margins_rules_scope` WHERE site_code = '*') AS fallback_base_markups
     # join to get last 7 days conversions
     LEFT JOIN L7days_CTE ON initial_markups.site_code_ref = L7days_CTE.site_code_ref
     # join to get COS
     LEFT JOIN `wego-cloud.analysis.hotels_margins_rules_COS` AS site_codes_COS ON t7.site_code = site_codes_COS.site_code
     # join to get COS for other site codes
     CROSS JOIN (SELECT * FROM `wego-cloud.analysis.hotels_margins_rules_COS` WHERE site_code = '*') AS fallback_COS
     ),
     
       t9 AS
    (SELECT *,
            selected_provider_rate / (1 + initial_markup) AS BoW_base_rate
     FROM t8),
    
       t10 AS
    (SELECT *,
            CASE
              WHEN initial_markup - parity_pct >= tolerance_level
                THEN CASE
                       WHEN initial_markup - parity_pct > max_markup 
                         THEN max_markup
                         ELSE CASE
                                WHEN initial_markup - parity_pct - price_leadership > tolerance_level
                                  THEN initial_markup - parity_pct - price_leadership
                                ELSE tolerance_level 
                              END 
                     END
              ELSE CASE
                     WHEN BoW_base_rate * (initial_markup - parity_pct - COS) * L7days_conversions >= RPH
                       THEN initial_markup - parity_pct
                     ELSE base_markup
                   END
            END AS proposed_markup
     FROM t9),
    
       t11 AS
    (SELECT *,
            BoW_base_rate * (1 + proposed_markup) AS BoW_new_rate,
            BoW_base_rate * (proposed_markup - COS) AS MPB
     FROM t10),
    
       t12 AS
    (SELECT *,
            MPB * L7days_conversions AS MPH,
            proposed_markup AS fitmdwsnaqc,
            proposed_markup AS dosacgqpcwa,
            proposed_markup AS apyacwgpawq,
            proposed_markup AS tboes9kixat,
            0 AS epsbkjh9ro
     FROM t11),

       final_output AS
    (SELECT CURRENT_DATE() AS date,
            site_code_ref,
            site_code,
            '*' AS city_code,
            hotel_id AS wego_hotel_id,
            goquo_hotel_id,
            fitmdwsnaqc,
            dosacgqpcwa,
            apyacwgpawq,
            tboes9kixat,
            epsbkjh9ro
     FROM t12 
     WHERE proposed_markup != base_markup
     UNION ALL
     SELECT CURRENT_DATE() AS date, * FROM `wego-cloud.analysis.hotels_margins_rules_scope`)

  SELECT * FROM final_output
{% endraw %}
