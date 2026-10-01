{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : campaign_id_recent_name_mapping
-- Destination: analysis.campaign_id_recent_name_mapping  (unchanged)
-- Schedule   : every day 00:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
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
on a.campaign_id = b.campaign_id)


--select "Google" as Source,cast(campaign_id as string) as campaign_id,campaign_name_recent from name_mapping_recent_name_final
--union distinct 
select "Google" as source,cast(campaign_id as string) as campaign_id,campaign_name as campaign_name_recent from `wego-cloud.marketing_analytics.cost`
where lower(data_source) like "%google%"
qualify row_number() over(partition by campaign_id order by date desc) = 1

--adding facebook mapping
union all 
(select "Facebook" as source,cast(campaign_id as string) as campaign_id,campaign_name from `wego-cloud.marketing_analytics.cost` where data_source = "facebook_adsinsights"
qualify row_number() over(partition by campaign_id order by date desc) = 1)
{% endraw %}
