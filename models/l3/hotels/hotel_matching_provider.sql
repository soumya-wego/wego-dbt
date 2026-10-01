{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotel_matching_provider_daily_run
-- Destination: analysis.hotel_matching_provider  (unchanged)
-- Schedule   : every day 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('hotel_services', 'property_types') }}
-- depends_on: {{ source('hotels', 'provider_countries') }}
-- depends_on: {{ source('hotels', 'provider_hotels') }}
-- depends_on: {{ source('hotels', 'provider_locations') }}
-- depends_on: {{ source('hotels', 'provider_property_types') }}
-- depends_on: {{ source('hotels', 'providers') }}
-- depends_on: {{ source('place_services', 'countries') }}
-- depends_on: {{ source('place_services', 'locations') }}
{% raw %}
DECLARE snapshot_date DATE DEFAULT CURRENT_DATE('Etc/UTC');

MERGE `wego-cloud.analysis.hotel_matching_provider` AS T
USING (
  SELECT
    snapshot_date AS snapshot_date,
    p.code AS provider_code,
    COALESCE(c.base_name, 'UNKNOWN') AS country_name,
    COALESCE(l.base_name, 'UNKNOWN') AS city_name,
    IFNULL(JSON_EXTRACT_SCALAR(pt.name, '$.en'), 'Unknown') AS property_type,
    COUNT(*) AS total_count,
    COUNTIF(ph.hotel_id IS NOT NULL AND ph.match_score >= 0.90) AS matched_count,
    COUNTIF(ph.hotel_id IS NOT NULL AND ph.match_score >= 0.90 AND matched_by = 'llm_matching') AS llm_matched_count
  FROM `wego-cloud.hotels.provider_hotels` ph
  JOIN `wego-cloud.hotels.providers` p ON ph.provider_id = p.id
  JOIN `wego-cloud.hotels.provider_locations` pl ON ph.provider_location_id = pl.id
  LEFT JOIN `wego-cloud.place_services.locations` l ON pl.location_id = l.id
  JOIN `wego-cloud.hotels.provider_countries` pc ON pl.provider_country_id = pc.id
  LEFT JOIN `wego-cloud.place_services.countries` c ON pc.country_id = c.id
  LEFT JOIN `wego-cloud.hotels.provider_property_types` ppt ON ph.provider_property_type_id = ppt.id
  LEFT JOIN `wego-cloud.hotel_services.property_types` pt ON ppt.property_type_id = pt.id
  GROUP BY provider_code, country_name, city_name, property_type
) AS S
ON (T.snapshot_date = S.snapshot_date AND T.provider_code = S.provider_code
    AND T.country_name = S.country_name AND T.city_name = S.city_name
    AND T.property_type = S.property_type)
WHEN MATCHED THEN UPDATE SET
  total_count = S.total_count,
  matched_count = S.matched_count,
  llm_matched_count = S.llm_matched_count
WHEN NOT MATCHED THEN INSERT
  (snapshot_date, provider_code, country_name, city_name, property_type, total_count, matched_count, llm_matched_count)
VALUES
  (S.snapshot_date, S.provider_code, S.country_name, S.city_name, S.property_type, S.total_count, S.matched_count, S.llm_matched_count);
{% endraw %}
