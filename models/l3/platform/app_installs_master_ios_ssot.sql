{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : app_installs_master_ios_ssot_daily_run
-- Destination: analysis.app_installs_master_ios_ssot  (unchanged)
-- Schedule   : every day 04:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('external_appsflyer', 'installs') }}
-- depends_on: {{ source('external_appsflyer', 'skan_installs') }}
{% raw %}
SELECT
IFNULL(af.date, skan.date) as date,
IFNULL(af.media_source, skan.media_source) as media_source,
IFNULL(af_installs, 0) AS af_installs,
SUM(af_installs) OVER (PARTITION BY af.date) as date_installs,
skan_installs,
IF(af.media_source='organic', 
    (SUM(af_installs) OVER (PARTITION BY af.date)) - (SUM(IF(af.media_source='organic',0, IFNULL(af_installs,0) + IFNULL(skan_installs,0))) OVER (PARTITION BY IFNULL(af.date, skan.date))), 
    IFNULL(af_installs,0) + IFNULL(skan_installs,0)) as ssot_installs
FROM
    (-- appsflyer dashboard Overview is in UTC+8, install_time in BQ is in UTC
    SELECT
    DATE(install_time) AS date,
    -- restricted could be twitter as well but 99% is Facebook.
    IF(media_source='restricted', 'Facebook Ads', IFNULL(media_source, partner)) AS media_source,
    COUNT(*) AS af_installs
    FROM `wego-cloud.external_appsflyer.installs*`
    WHERE _TABLE_SUFFIX >= '20230117'
    AND DATE(install_time) >= '2023-01-17'
    AND platform='ios'
    GROUP BY date, media_source
    ) AS af

    FULL OUTER JOIN
    (-- appsflyer dashboard SSOT is in UTC
    SELECT 
    DATE(install_date) AS date, 
    media_source, count(*) as skan_installs
    FROM `wego-cloud.external_appsflyer.skan_installs*`
    WHERE _TABLE_SUFFIX >= '20230118'
    AND DATE(install_date) >= '2023-01-17'
    AND af_attribution_flag='false' --only keep installs not attributed by Appsflyer (attributed by SKAN)
    GROUP BY date, media_source
    ) AS skan
    ON af.date = skan.date
    AND af.media_source = skan.media_source
{% endraw %}
