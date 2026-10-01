{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : campaign_name_recent_name_mapping
-- Destination: analysis.campaign_name_recent_name_mapping  (unchanged)
-- Schedule   : every day 00:45   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('adwords_dts', 'ads_campaign_2851107229') }}
-- depends_on: {{ source('adwords_dts', 'p_campaign_2851107229') }}
-- depends_on: {{ ref('campaign_id_recent_name_mapping') }}
-- depends_on: {{ source('marketing_analytics', 'cost') }}
{% raw %}
with name_date_mapping as
(
SELECT _data_date as date,
CAST(campaign_id AS INT) AS campaign_id, campaign_name
 FROM `adwords_dts.ads_Campaign_2851107229` --data by date, id level
 
  group by 1,2,3),

name_date_mapping_older as 
  (select extract(date from _PARTITIONTIME) as date,cast(CampaignId as int) as campaign_id,CampaignName as campaign_name from adwords_dts.p_Campaign_2851107229 where _PARTITIONTIME >= '2023-01-01'
  group by 1,2,3),

 name_date_mapping_union as 
  (select date,campaign_id,campaign_name from name_date_mapping
  union distinct
  select date,campaign_id,campaign_name from name_date_mapping_older),

  name_mapping as 
  (select campaign_id,campaign_name from name_date_mapping_union group by 1,2),

  name_date_mapping_ranking as
  (select date,campaign_id,campaign_name,
  dense_rank() over(partition by campaign_id order by date desc) as latest_rank
  from name_date_mapping_union
  ),
  name_mapping_recent_name as
  (select campaign_id,campaign_name 
  from name_date_mapping_ranking where latest_rank = 1 
  group by 1,2),
name_mapping_recent_name_final as
(select a.*,b.campaign_name as campaign_name_recent from name_mapping as a 
left join name_mapping_recent_name as b 
on a.campaign_id = b.campaign_id),


--facebook name mapping 
facebook as
(
  

select "Facebook" as source, a.campaign_name,b.campaign_name_recent from 
  (select replace(lower(campaign_name)," ","") as campaign_name,max(campaign_id) as campaign_id from `wego-cloud.marketing_analytics.cost` where data_source = "facebook_adsinsights" group by 1) as a 
  left join  wego-cloud.analysis.campaign_id_recent_name_mapping as b 
  on cast(a.campaign_id as string)  = cast(b.campaign_id as string)



),

google as
(
  

select "Google" as source, a.campaign_name,b.campaign_name_recent from 
  (select replace(lower(campaign_name)," ","") as campaign_name,max(campaign_id) as campaign_id from `wego-cloud.marketing_analytics.cost` where data_source like "%google%" group by 1) as a 
  left join  wego-cloud.analysis.campaign_id_recent_name_mapping as b 
  on cast(a.campaign_id as string)  = cast(b.campaign_id as string)



)



-- select "Google" as Source,replace(lower(campaign_name)," ","") as campaign_name,campaign_name_recent 
-- from name_mapping_recent_name_final group by 1,2,3

select * from google
union all 
select * from facebook
{% endraw %}
