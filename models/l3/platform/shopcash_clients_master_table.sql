{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : shopcash_clients_master_table
-- Destination: shopcash_analytics.shopcash_clients_master_table  (unchanged)
-- Schedule   : every day 01:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('shopcash_analytics', 'clients') }}
-- depends_on: {{ source('shopcash_analytics', 'sessions') }}
-- depends_on: {{ source('shopcash_analytics', 'users') }}
{% raw %}
select
* except(u_client_id, first_client_id)
from
(select
* except(rank,created_at, client_created_at, client_created_at_timezone, device, device_type, device_brand, device_model, marketing_name, user_agent, device_version, app_version)
from
(SELECT
* except(user_hash), ROW_NUMBER() OVER ( PARTITION BY client_id ORDER BY created_at asc ) AS rank
FROM
  `wego-cloud.shopcash_analytics.clients`
WHERE
  DATE(_PARTITIONTIME) > "2021-01-01")
  where rank = 1) c

  left join 
  (select
* except(rank, client_id, created_at), client_id as u_client_id
from
(SELECT
*, ROW_NUMBER() OVER ( PARTITION BY client_id ORDER BY created_at desc ) AS rank
FROM
  `wego-cloud.shopcash_analytics.users`
WHERE
  DATE(created_at) > "2021-01-01")
  where rank = 1) u on c.client_id = u.u_client_id

  left join 
  (select
  client_id as first_client_id,
  date(created_at) as aqcuisition_date,
  advertiser_id as first_advertiser_id,
  crm_id as first_crm_id,
  device as first_device,
  device_type as first_device_type,
  os_type as first_os_type,
  app_version as first_app_version,
  device_version as first_device_version,
  os_version as first_os_version,
  user_country_code as first_user_country_code,
  user_city as first_user_city,
  user_latitude as first_user_latitude,
  user_longitude as first_user_longitude,
  network_type as first_network_type,
  network_carrier_name as first_network_carrier_name,
  landing_url as first_landing_url,
  referrer_url as first_referrer_url,
  site_code as first_site_code,
  locale as first_locale,
  channel as first_channel,
  source as first_source,
  utm_source as first_utm_source,
  utm_medium as first_utm_medium,
  utm_campaign as first_utm_campaign,
  utm_term as first_utm_term,
  utm_content as first_utm_content,
  ts_code as first_ts_code,
  market as first_market,
  app_rt_timestamp as first_app_rt_timestamp,
  app_rt_source as first_app_rt_source,
  app_rt_medium as first_app_rt_medium,
  app_rt_campaign as first_app_rt_campaign,
  app_rt_adgroup as first_app_rt_adgroup,
  app_rt_content as first_app_rt_content
from
(SELECT
*, ROW_NUMBER() OVER ( PARTITION BY client_id ORDER BY created_at asc ) AS rank
FROM
  `wego-cloud.shopcash_analytics.sessions`
WHERE
  DATE(_PARTITIONTIME) > "2021-01-01")
  where rank = 1) s on c.client_id = s.first_client_id
{% endraw %}
