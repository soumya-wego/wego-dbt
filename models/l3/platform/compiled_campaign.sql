{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Apple Search Ads Mapping tables flatenning
-- Destination: apple_search_ad.compiled_campaign  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
CREATE OR REPLACE TABLE `wego-cloud.apple_search_ad.compiled_campaign` AS
SELECT * EXCEPT (dedup,_TABLE_SUFFIX) FROM
  (SELECT *,ROW_NUMBER() OVER (PARTITION BY campaignId ORDER BY _TABLE_SUFFIX DESC) AS dedup
  FROM
    (SELECT orgId,orgName,campaignId,campaignName,_TABLE_SUFFIX,
    CASE
    WHEN REGEXP_CONTAINS(campaignName, r'^ASA-App-UA-') THEN
      REGEXP_EXTRACT(campaignName, r'^ASA-App-UA-(\w+)-')
    WHEN REGEXP_CONTAINS(campaignName, r'Today Tab-ASA-(\w+)') THEN
      REGEXP_EXTRACT(campaignName, r'Today Tab-ASA-(\w+)')
    WHEN REGEXP_CONTAINS(campaignName, r'ASA-') THEN
      REGEXP_EXTRACT(campaignName, r'ASA-(\w+)-')
    WHEN REGEXP_CONTAINS(campaignName, r'^LOC-\w+-') THEN
      REGEXP_EXTRACT(campaignName, r'^LOC-(\w+)-')
    ELSE 'Unknown'
  END AS country_code,  
    CASE
    WHEN REGEXP_CONTAINS(campaignName, r'-\d+$') THEN
      REGEXP_EXTRACT(REGEXP_REPLACE(campaignName, r'-\d+$', ''), r'[^-]+$')
    ELSE
      REGEXP_EXTRACT(campaignName, r'[^-]+$')
  END AS category
    FROM `wego-cloud.apple_search_ad.campaign*`
    GROUP BY 1,2,3,4,5,6,7
    )
  )
WHERE dedup=1;

CREATE OR REPLACE TABLE `wego-cloud.apple_search_ad.compiled_adgroup` AS
SELECT * EXCEPT (dedup,_TABLE_SUFFIX) FROM
  (SELECT *,ROW_NUMBER() OVER (PARTITION BY adGroupId ORDER BY _TABLE_SUFFIX DESC) AS dedup
  FROM
    (SELECT adGroupId,adGroupName,_TABLE_SUFFIX
    FROM `wego-cloud.apple_search_ad.adgroup*`
    GROUP BY 1,2,3
    )
  )
WHERE dedup=1;

CREATE OR REPLACE TABLE `wego-cloud.apple_search_ad.compiled_keyword` AS
SELECT * EXCEPT (dedup,_TABLE_SUFFIX) FROM
  (SELECT *,ROW_NUMBER() OVER (PARTITION BY keywordId ORDER BY _TABLE_SUFFIX DESC) AS dedup
  FROM
    (SELECT keywordId,keyword,_TABLE_SUFFIX
    FROM `wego-cloud.apple_search_ad.keyword*`
    GROUP BY 1,2,3
    )
  )
WHERE dedup=1;
{% endraw %}
