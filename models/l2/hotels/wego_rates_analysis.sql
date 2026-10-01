{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : wego_hotels_rates_analysis
-- Destination: analysis.wego_rates_analysis  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
WITH 
ranked_rates AS
	(SELECT
	 parse_date('%Y%m%d', _table_suffix) AS created_at, # created_at denotes date when rate was generated, _table_suffix is the traffic date
	 created_at AS created_at_timestamp,
	 id AS rate_id,
	 search_id,
	 hotel_id,
	 room_ids,
	 CONCAT(search_id, ' - ', hotel_id) as search_hotel_id, # this becomes the primary key for ranking by hotels, hotel_id is used across searches
	 CONCAT(search_id, ' - ', hotel_id, ' - ', room_ids[ORDINAL(1)]) as search_hotel_room_id, # this becomes the primary key for ranking by hotels + room, hotel_id is used across searches
	 provider.code AS provider_code,
	 provider.name AS provider_name,
	 DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY) as nights,

	(coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0)) as total_amount_usd,



	(coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0)) / DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY) as total_amount_per_night,
	
	
	(coalesce(price.amount_usd,0)

 + coalesce(case when safe_divide(price.local_tax_amount_usd,rooms_count) <= 0  then 0 else safe_divide(price.local_tax_amount_usd,rooms_count) end ,0)) as amount_per_room_night, # will match with total_amount_per_night if rooms count = 1
	 
	 row_number() over (partition by id) AS id_row, # some rate_id records are duplicated, need to deduplicate later on
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 # BY HOTELS
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 COUNT(DISTINCT provider.code) OVER (PARTITION by CONCAT(search_id, ' - ', hotel_id)) as hotel_provider_count,
	 -- MIN(price.total_amount_usd / DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY)) OVER (PARTITION BY CONCAT(search_id, ' - ', hotel_id)) as hotel_min_rate,
	 -- dense_rank() over (partition by CONCAT(search_id, ' - ', hotel_id) order by price.total_amount_usd / DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY) asc) as hotel_rank,

	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 # BY HOTELS + ROOM
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 COUNT(DISTINCT provider.code) OVER (PARTITION by CONCAT(search_id, ' - ', hotel_id, ' - ', room_ids[SAFE_ORDINAL(1)])) as hotel_room_provider_count,
	 MIN((coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0)) / DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY)) OVER (PARTITION BY CONCAT(search_id, ' - ', hotel_id, ' - ', room_ids[SAFE_ORDINAL(1)])) as hotel_room_min_rate,	 
	 dense_rank() over (partition by CONCAT(search_id, ' - ', hotel_id, ' - ', room_ids[ORDINAL(1)]) order by (coalesce(price.total_amount_usd,0)

 + coalesce(case when price.total_local_tax_amount_usd <= 0  then 0 else price.total_local_tax_amount_usd end ,0)) / DATE_DIFF(CAST(check_out as DATE), CAST(check_in as DATE), DAY) asc) as hotel_room_rank,
	 
	 FROM `wego-cloud.services_akasha.rates*`
	 WHERE _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
	   AND (rooms_count = 1 and array_length(room_ids) = 1) # take only records where the hotel rate consisted of 1 room for fair ranking comparison
-- 	   AND CONCAT(search_id, ' - ', hotel_id) IN # filter for hotels where wego was a provider
-- 	   				  							(SELECT
-- 	 				  							 distinct CONCAT(search_id, ' - ', hotel_id)
-- 	 				  							 FROM `wego-cloud.services_akasha.rates*`
-- 	 				  							 where _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(current_date(), INTERVAL 1 DAY))
-- 	 				  							   AND provider.code = 'hotels.wego.com')
-- 	   AND id IN # filter for hotels which ended up becoming a click (rate id not IN WA hotels clicks table)
-- 	   		     (SELECT
-- 	 		      distinct click_id #in hotels, click_id is the same as rate_id
-- 	 		      FROM `wego-cloud.wego_analytics.hotels_clicks` 
-- 	 		      WHERE DATE(_PARTITIONTIME) = DATE_SUB(current_date(), INTERVAL 1 DAY))
	),

ranked_rates_2 AS # generate intermediate fields for fare_parity logic
	(SELECT
	 *,
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 # BY HOTELS
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 -- # check whether that rate was the cheapest rate, by hotel (for use later)
	 -- CASE WHEN total_amount_per_night = hotel_min_rate THEN TRUE
  --         ELSE FALSE
  --         END AS hotel_cheapest_provider,

  --    # check how many providers gave the cheapest rate, by hotel (for use later)
  --    COUNT(DISTINCT IF(total_amount_per_night = hotel_min_rate, provider_code, null)) OVER (PARTITION BY search_hotel_id) AS hotel_cheapest_provider_count,

  --    # arrange rates into a string from cheapest onwards, by hotel (for use later)
  --    # need to deduplicate in the next step, possible to have duplicates, e.g. for a hotel A, Booking provides $30 for room A while Wego provides $25 for room A and B, and are the lowest. 25-25-30
	 -- STRING_AGG(CAST(total_amount_per_night AS STRING), ',') OVER (PARTITION BY search_hotel_id ORDER BY total_amount_per_night RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hotel_rate_agg,  

     --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 # BY HOTELS + ROOM
	 --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     # check whether that rate was the cheapest rate, by hotel + room (for use later)
     CASE WHEN total_amount_per_night = hotel_room_min_rate THEN TRUE
          ELSE FALSE
          END AS hotel_room_cheapest_provider,

     # check how many providers gave the cheapest rate, by hotel + room (for use later)
     COUNT(DISTINCT IF(total_amount_per_night = hotel_room_min_rate, provider_code, null)) OVER (PARTITION BY search_hotel_room_id) AS hotel_room_cheapest_provider_count, 
	 
	 # arrange rates into a string from cheapest onwards, by hotel + room (for use later)
	 STRING_AGG(CAST(total_amount_per_night AS STRING), ',') OVER (PARTITION BY search_hotel_room_id ORDER BY total_amount_per_night RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hotel_room_rate_agg,

	 FROM ranked_rates
		),

ranked_rates_3 AS # deduplicate for concatenated rate string
	(SELECT
		# hotels
		* 
		-- REPLACE(ARRAY_TO_STRING(ARRAY(SELECT DISTINCT hotel_rate FROM UNNEST(SPLIT(hotel_rate_agg, ',')) AS hotel_rate ORDER BY CAST(hotel_rate AS NUMERIC)), ',') AS hotel_rate_agg) # 25-25-30 becomes 25-30
		FROM ranked_rates_2),

ranked_rates_4 AS # generate intermediate fields for fare_parity logic
	(SELECT 
	 *,
	 -- IF(hotel_cheapest_provider AND hotel_cheapest_provider_count = 1, CAST(SPLIT(hotel_rate_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC), hotel_min_rate) as hotel_min_rate_exclude_own_rate,
	 IF(hotel_room_cheapest_provider AND hotel_room_cheapest_provider_count = 1, CAST(SPLIT(hotel_room_rate_agg, ',')[SAFE_ORDINAL(2)] AS NUMERIC), hotel_room_min_rate) as hotel_room_min_rate_exclude_own_rate
	 FROM ranked_rates_3
		),

hotel_details AS #get details of hotels + brands + chains by city and country
	(SELECT * EXCEPT(rn) FROM
	 (SELECT
	  hotel_services.*,
	  locations.* EXCEPT(city_code),
	  row_number() OVER (PARTITION BY hotel_id) as rn # some duplicates happen after joining hotel_id to locations, 1 hotel id end up with >1 locations
	  FROM	
	  	(SELECT
	  	 hotels.*,
	  	 hotels_chains.id AS chain_id,
	  	 hotel_brand_code,
	  	 COALESCE(hotel_brand, 'no-brand-associated') AS hotel_brand,
	  	 hotel_chain_code,
	  	 COALESCE(hotel_chain, 'no-chain-associated') AS hotel_chain
	  	 FROM
	  	 	(SELECT 
	  	 	 CAST(id as STRING) as hotel_id,
	  	 	 name_en as hotel_name,
	  	     brand_id,
	  	     star AS hotel_stars,
	  	     city_code,
	  	     FROM `hotel_services.hotels`
	  	 	) AS hotels
	  	 
	  	 left join
	  	 	(SELECT 
	  	     id,
	  	     code AS hotel_brand_code,
	  	     permalink AS hotel_brand,
	  	     chain_id
	  	     FROM `hotel_services.brands`
	  	 	) AS hotels_brands ON hotels.brand_id = hotels_brands.id
	  	 
	  	 LEFT join
	  	 	(SELECT id,
	  	      code AS hotel_chain_code,
	  	      permalink AS hotel_chain
	  	    FROM `hotel_services.chains`
	  	 	) AS hotels_chains ON hotels_brands.chain_id = hotels_chains.id
	  	) as hotel_services
	  
	  LEFT JOIN
	  	(SELECT
	  	 locations.city_name,
	  	 locations.city_code,
	  	 countries.country_name,
	  	 countries.country_code
	  	 FROM
	  	 	(SELECT
	  	 	 base_name as city_name,
	  	 	 code as city_code,
	  	 	 country_id as country_id
	  	 	 FROM `wego-cloud.place_services.locations`
	  	 	) AS locations
	  	 LEFT JOIN
	  	 	(SELECT
	  	 	 id as country_id,
	  	 	 code as country_code,
	  	 	 base_name as country_name
	  	 	 FROM `wego-cloud.place_services.countries`
	  	 	) AS countries on locations.country_id = countries.country_id
	  	) as locations on hotel_services.city_code = locations.city_code
	 )
	 WHERE rn = 1 #deduplicate to get only 1 hotel_id per row
	),
 
provider_type AS # get provider_type, exact logic AS WA hotel_clicks
	(SELECT
	 COALESCE(par_prov.code, prov.code) AS provider_code,
	 prov.code AS provider_domain,
	 IF(COALESCE(par_prov.provider_type, prov.provider_type) IN ('chain', 'resort', 'hotel'), 'direct', COALESCE(par_prov.provider_type, prov.provider_type)) AS provider_type
	 FROM 
	 	(SELECT code, parent_id, IF(provider_type = 'direct_priority', 'direct', provider_type) AS provider_type FROM `hotels.providers`) AS prov
	 LEFT JOIN 
	 	(SELECT id, code, IF(provider_type = 'direct_priority', 'direct', provider_type) AS provider_type FROM `hotels.providers`) AS par_prov on par_prov.id=prov.parent_id
	),

searches as # get search dimensions + trip_category + lead_time
	(SELECT 
	 search.*,
	 if(user_country_code = country_code, 'domestic', 'international') AS trip_category,
	 FROM
	 	(SELECT
	 	 DATE(created_at) AS created_at,
	 	 session_id,
	 	 search_id,
	 	 site_code,
	 	 locale,
	 	 rooms_count,
	 	 guests_count,
	 	 country_code,
	 	 lead_time,
	 	 trip_duration,
	 	 check_in,
	 	 check_out,
	 	 FROM `wego-cloud.wego_analytics.hotels_searches` hotels_searches
	 	 WHERE DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)
	 		) AS search
	 LEFT JOIN # get user country to derive trip_category
	 	(SELECT
	 	 DATE(created_at) AS created_at,
	 	 session_id,
	 	 user_country_code
	 	 FROM `wego-cloud.wego_analytics.sessions`
	 	 WHERE DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)
	 		) AS sessions on search.created_at = sessions.created_at
	 					 and search.session_id = sessions.session_id

	),

clicks as # get click / conversions information
	(SELECT 
	 click_id,
	 provider_code as click_provider_code,
	 1 as click,
	 CASE WHEN conversions_tracked > 0 THEN 1 # reduce conversions_tracked > 1 to just 1
	 	  ELSE conversions_tracked 
	 	  END AS conversions_tracked,
	 FROM `wego-cloud.wego_analytics.hotels_clicks` 
	 WHERE DATE(_PARTITIONTIME) >= DATE_SUB(current_date(), INTERVAL 2 DAY)
	),

t1 as
	(SELECT
	 ranked_rates_4.* EXCEPT (id_row
							 -- hotel_cheapest_provider,
							 -- hotel_cheapest_provider_count,
							 -- hotel_rate_agg,
							 -- hotel_room_cheapest_provider,
							 -- hotel_room_cheapest_provider_count,
							 -- hotel_room_rate_agg
							 -- hotel_min_rate_exclude_own_rate,
							 -- hotel_room_min_rate_exclude_own_rate
							 ),

	clicks.*,
	provider_type.* EXCEPT(provider_code, provider_domain),
	searches.* EXCEPT(created_at, session_id, search_id, country_code),
	hotel_details.* EXCEPT(hotel_id),

	# hotels
	-- CASE WHEN hotel_provider_count = 1 THEN NULL # if it's the only provider then null
	-- 	 WHEN hotel_cheapest_provider AND hotel_cheapest_provider_count > 1 THEN 0 # if >1 provider being cheapest then 0 parity
	-- 	 ELSE (total_amount_per_night - hotel_min_rate_exclude_own_rate) / total_amount_per_night * 100
 --  		 END hotel_rate_parity_pct,
	-- MAX(case when hotel_rank <= 10 and ranked_rates_4.provider_code = 'hotels.wego.com' THEN 1 ELSE 0 END) over (partition by search_hotel_id) as hotel_wego_top_10, # to denote hotel_rates where wego was a top 10 fare
	-- MAX(case when hotel_rank > 10 and ranked_rates_4.provider_code LIKE 'hotels.wego.com' THEN 1 ELSE 0 END) over (partition by search_hotel_id) as hotel_wego_not_top_10, # to denote hotel_rates where wego was not a top 10 fare

	# hotels + room
	CASE WHEN hotel_room_provider_count = 1 THEN NULL # if it's the only provider then null
		 WHEN hotel_room_cheapest_provider AND hotel_room_cheapest_provider_count > 1 THEN 0 # if >1 provider being cheapest then 0 parity
		 ELSE (total_amount_per_night - hotel_room_min_rate_exclude_own_rate) / total_amount_per_night * 100
		 END hotel_room_rate_parity_pct,
	MAX(case when hotel_room_rank <= 10 and ranked_rates_4.provider_code = 'hotels.wego.com' THEN 1 ELSE 0 END) over (partition by search_hotel_room_id) as hotel_room_wego_top_10, # to denote hotel + room rates where wego was a top 10 fare
	MAX(case when hotel_room_rank > 10 and ranked_rates_4.provider_code LIKE 'hotels.wego.com' THEN 1 ELSE 0 END) over (partition by search_hotel_room_id) as hotel_room_wego_not_top_10, # to denote hotel + room rates where wego was not a top 10 fare

	# clinton's additional intemediate steps

	# hotels
	COUNT(DISTINCT click_id) OVER (PARTITION BY search_hotel_id) AS hotel_clicks, # can use to filter for clicked results if > 0
	COUNT(conversions_tracked) OVER (PARTITION BY search_hotel_id) AS hotel_conversions,
	SUM(conversions_tracked) OVER (PARTITION BY search_hotel_id) AS hotel_bookings,

	# hotels + room
	COUNT(DISTINCT click_id) OVER(PARTITION BY search_hotel_room_id) AS hotel_room_clicks,
	COUNT(conversions_tracked) OVER(PARTITION BY search_hotel_room_id) AS hotel_room_conversions,
	SUM(conversions_tracked) OVER (PARTITION BY search_hotel_room_id) AS hotel_room_bookings,

	FROM ranked_rates_4
	LEFT JOIN clicks on ranked_rates_4.rate_id = clicks.click_id
	LEFT JOIN provider_type on ranked_rates_4.provider_code = provider_type.provider_domain
	LEFT JOIN hotel_details on ranked_rates_4.hotel_id = hotel_details.hotel_id
	LEFT JOIN searches on ranked_rates_4.search_id = searches.search_id
	WHERE id_row = 1),

t2 as
	(SELECT * EXCEPT (hotel_clicks, hotel_conversions, hotel_room_clicks, hotel_room_conversions, hotel_bookings),

		# hotels
		-- CASE WHEN hotel_rate_parity_pct < 0 THEN 1 # 1 if it's the winner AND there's more than 1 provider
		-- 	 WHEN hotel_rate_parity_pct >= 0 THEN 0 # 0 if it's not the winner
		-- 	 ELSE NULL # NULL if it's the only provider
		-- 	 END AS hotel_rate_won,
		-- -- IF(hotel_rate_parity_pct IS NULL or hotel_rate_parity_pct < 0, 1, 0) as hotel_rate_won, # 1 if it's the only provider OR the winner, 0 otherwise
		-- CASE WHEN hotel_clicks > 0 AND click_id IS NOT NULL THEN 1 # 1 if there's clicks and it was clicked
		-- 	 WHEN hotel_clicks > 0 AND click_id IS NULL THEN 0 # 0 if there's clicks but it wasn't clicked
		-- 	 ELSE NULL # null if there's no clicks
		-- 	 END AS hotel_click_won,
		-- CASE WHEN hotel_conversions > 0 AND conversions_tracked > 0 THEN 1 # 1 if conversions are tracked and there's it was converted
		-- 	 WHEN hotel_conversions > 0 THEN 0 # 0 if conversions are tracked but not converted
		-- 	 ELSE NULL # null if conversions are not racked
		-- 	 END AS hotel_conversion_won,

		# hotels + room
		CASE WHEN hotel_room_rate_parity_pct < 0 THEN 1 # 1 if it's the winner AND there's more than 1 provider
			 WHEN hotel_room_rate_parity_pct >= 0 THEN 0 # 0 if it's not the winner
			 ELSE NULL # NULL if it's the only provider
			 END AS hotel_room_rate_won,
		-- IF(hotel_room_rate_parity_pct IS NULL or hotel_room_rate_parity_pct < 0, 1, 0) as hotel_room_rate_won, # 1 if it's the only provider OR the winner, 0 otherwise
		CASE WHEN hotel_room_clicks > 0 AND click_id IS NOT NULL THEN 1 # 1 if there's clicks and it was clicked
			 WHEN hotel_room_clicks > 0 AND click_id IS NULL THEN 0 # 0 if there's clicks but it wasn't clicked
			 ELSE NULL # null if there's no clicks
			 END AS hotel_room_click_won,
		CASE WHEN hotel_room_conversions > 0 AND conversions_tracked > 0 THEN 1 # 1 if conversions are tracked and there's it was converted
			 WHEN hotel_room_conversions > 0 THEN 0 # 0 if conversions are tracked but not converted
			 ELSE NULL # null if conversions are not racked
			 END AS hotel_room_conversion_won
		FROM t1
		WHERE hotel_clicks > 0 # filter for clicked results
		)

SELECT *
FROM t2 # don't order by, else might not complete the run
{% endraw %}
