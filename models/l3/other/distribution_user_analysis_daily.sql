{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : distribution_user_analysis_daily
-- Destination: analysis.distribution_user_analysis_daily  (unchanged)
-- Schedule   : every day 04:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- insert into `wego-cloud.analysis.distribution_user_analysis_daily`
-- partition by created_at_date as
with user_pool as
(
  SELECT  
    client_id
    , string_agg(distinct case
      when ts_code = '2f2fc' then 'Google Hotels'
      when ts_code = '6be92' and wg_campaign in ('hotels_meta', 'hotels_ads') then 'Skyscanner Hotels'
      when ts_code = '6be92' and wg_campaign in ('flights_meta', 'flights_ads') then 'Skyscanner Flights'
      when ts_code = '9b77c' and wg_campaign in ('hotels_meta', 'hotels_ads') then 'Kayak Hotels'
      when ts_code = '9b77c' and wg_campaign in ('flights_meta', 'flights_ads') then 'Kayak Flights'
      when ts_code = '27a46' and wg_campaign in ('hotels_meta', 'hotels_ads') then 'Vio Hotels'
      else 'Others' end,',') as distribution_type 
  FROM `wego-cloud.analysis.clients_sessions_aggregated_daily_master` 
  WHERE created_at_date = current_date - interval '1' day
  and ts_code in (
    '2f2fc'
    , '6be92'
    , '9b77c'
    , '27a46'
  )
  group by 1--,2,3
)

, sessions as
(
  select
    a.* except(new_existing_session)
    , case when cummulative_total_sessions = 1 and ts_code is null then 'Existing' else new_existing_session end as new_existing_session
    , distribution_type
  from `wego-cloud.analysis.clients_sessions_aggregated_daily_master` a
  join user_pool b
    on a.client_id = b.client_id
  WHERE created_at_date = current_date - interval '1' day
)

select * from sessions
{% endraw %}
