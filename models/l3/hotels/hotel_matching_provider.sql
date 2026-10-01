{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotel_matching_provider_daily_run
-- Destination: analysis.hotel_matching_provider  (unchanged)
-- Schedule   : every day 00:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
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
