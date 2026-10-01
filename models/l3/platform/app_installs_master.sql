{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : app_installs_master_daily_append_v2
-- Destination: analysis.app_installs_master  (unchanged)
-- Schedule   : every day 04:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table 
-- analysis.app_installs_master
-- PARTITION BY date
-- AS
/*
SELECT
date as date,
media_source as media_source,
campaign as campaign,
country as country,
market as market,
os as os,
app_store as app_store,
channel as channel,
keywords as keywords,
adset_name as adset_name,
city as city,
os_version as os_version,
app_version as app_version,
sdk_version as sdk_version,
referrer_page as referrer_page,
device_category as device_category,
fraud_status,
SUM(installs) as installs,
FROM
(
--appsflyer
     (SELECT
date,
media_source,
campaign,
country,
market,
os,
app_store,
installs,
channel,
keywords,
adset_name,
city,
os_version,
app_version,
sdk_version,
referrer_page,
device_category,
ifnull(fraud_reason, "genuine") as fraud_status
FROM
(SELECT
     date(timestamp_add(install_time, interval 8 hour)) as date,
     ifnull(media_source, partner) as media_source,
     campaign,
     country,
     platform as os,
     install_app_store as app_store,
     1 AS installs,
       channel,
  keywords,
  adset_name,
  city,
  os_version,
  app_version,
  sdk_version,
  http_referrer as referrer_page,
  device_category,
  appsflyer_id
FROM
  `wego-cloud.external_appsflyer.installs*`
     WHERE _TABLE_SUFFIX BETWEEN ('20180701')  and FORMAT_DATE( "%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
     AND event_name = "install") as i

  left join 

(SELECT  
country_code,
market
FROM `wego-cloud.analytics.countries_misc` ) as c
on i.country = c.country_code

  left join 

  (SELECT distinct 
  appsflyer_id as fraud_id,
  first_value(fraud_reason) over( partition by appsflyer_id order by install_time) as fraud_reason
FROM
  `wego-cloud.external_appsflyer.post_attribution_installs*`) f on i.appsflyer_id = f.fraud_id

)

     
 
 UNION ALL 
 --apsalar
     (
      (SELECT
date,
media_source,
campaign,
country,
market,
os,
app_store,
installs,
channel,
keywords,
adset_name,
city,
os_version,
app_version,
sdk_version,
referrer_page,
device_category,
"genuine" as fraud_status
FROM
(SELECT 
date(timestamp_add(install_timestamp, interval 8 hour)) as date,
campaign_source as media_source,
campaign_name as campaign,
country_code as country,
CAST(NULL AS STRING) as os,
CAST(NULL AS STRING) as app_store,
1 as installs,
CAST(NULL AS STRING) as channel,
CAST(NULL AS STRING) as keywords,
CAST(NULL AS STRING) as adset_name,
CAST(NULL AS STRING) as city,
CAST(NULL AS STRING) as os_version,
CAST(NULL AS STRING) as app_version,
CAST(NULL AS STRING) as sdk_version,
CAST(NULL AS STRING) as referrer_page,
CAST(NULL AS STRING) as device_category
     FROM `wego-cloud.apsalar.attribution_consolidated`
     where install_timestamp BETWEEN timestamp('2012-01-01') AND timestamp('2018-06-30')
       and attribution = 'Install') as i

  left join 

(SELECT  
country_code,
market
FROM `wego-cloud.analytics.countries_misc` ) as c
on i.country = c.country_code)

     )
     )

     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
*/

declare start_date date default date(current_date - interval '8' day);
declare end_date date default date(current_date - interval '1' day); 

delete from  `wego-cloud.analysis.app_installs_master` 
where date between start_date and end_date

;

insert into `wego-cloud.analysis.app_installs_master` 

with appsflyer_installs as (
  select
    date(timestamp_add(install_time, interval 8 hour)) as date
  , ifnull(media_source, partner) as media_source
  , campaign
  , country
  , platform as os
  , install_app_store as app_store
  , 1 as installs
  , channel
  , keywords
  , adset_name
  , city
  , os_version
  , app_version
  , sdk_version
  , http_referrer as referrer_page
  , device_category
  , appsflyer_id
  from
    `wego-cloud.external_appsflyer.installs*`
  where _table_suffix between format_date("%Y%m%d", start_date)
    and format_date("%Y%m%d", end_date)
    and event_name = "install"
)

, countries as (
  select
    country_code
  , market
  from
    `wego-cloud.analytics.countries_misc`
)

, appsflyer_fraud as (
  select distinct
    appsflyer_id as fraud_id
  , first_value(fraud_reason) over (
      partition by appsflyer_id
      order by install_time
    ) as fraud_reason
  from
    `wego-cloud.external_appsflyer.post_attribution_installs*`
  where _table_suffix between format_date("%Y%m%d", start_date)
    and format_date("%Y%m%d", end_date)
)

, appsflyer_final as (
  select
    i.date
  , i.media_source
  , i.campaign
  , i.country
  , c.market
  , i.os
  , i.app_store
  , i.installs
  , i.channel
  , i.keywords
  , i.adset_name
  , i.city
  , i.os_version
  , i.app_version
  , i.sdk_version
  , i.referrer_page
  , i.device_category
  , ifnull(f.fraud_reason, "genuine") as fraud_status
  from
    appsflyer_installs i
  left join countries c
    on i.country = c.country_code
  left join appsflyer_fraud f
    on i.appsflyer_id = f.fraud_id
)

select
    date as date
  , media_source as media_source
  , campaign as campaign
  , country as country
  , market as market
  , os as os
  , app_store as app_store
  , channel as channel
  , keywords as keywords
  , adset_name as adset_name
  , city as city
  , os_version as os_version
  , app_version as app_version
  , sdk_version as sdk_version
  , referrer_page as referrer_page
  , device_category as device_category
  , fraud_status
  , sum(installs) as installs
from
  appsflyer_final
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
{% endraw %}
