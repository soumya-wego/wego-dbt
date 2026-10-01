{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : shopcash_lifetime_value_ltv_master
-- Destination: analysis.shopcash_lifetime_value_ltv_master  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
with users as 
(SELECT
* except(rn)
FROM
(SELECT 
  created_at as acquisition_date,
  user_hash,
  user_id,
  gender,
  date_of_birth,
  acquired_through_referral,
  ROW_NUMBER() OVER(PARTITION BY user_hash ORDER BY created_at ASC) as rn
FROM
  `wego-cloud.shopcash_analytics.users`
  WHERE user_hash is not null
  and user_hash != ""
order by 2)
WHERE rn = 1),

ltv as
(SELECT
user_hash,
min(transacted_at) as first_transaction_date,
count(distinct coalesce(order_id, genzo_conversion_id, click_id)) as lifetime_orders,
if(sum(order_commission_adjusted) < 0, 0,sum(order_commission_adjusted))  as lifetime_commission,
if(sum(net_revenue_adjusted) < 0, 0,sum(net_revenue_adjusted)) as lifetime_revenue,
if(sum(order_value_adjusted) < 0, 0,sum(order_value_adjusted)) as lifetime_gmv
FROM
  `wego-cloud.shopcash_analytics.transactions`
  where user_hash is not null
  and user_type != "wego"
  group by 1
  order by 4 ),

sessions as
( select
  * except(rn)
  from
  (SELECT 
user_hash,
created_at,
device_type,
os_type,
app_version,
user_country_code,
user_city,
referrer_url,
site_code,
locale,
channel,
utm_source,
utm_medium,
utm_campaign,
utm_content,
ts_code,
source,
market,
app_rt_source,
app_rt_medium,
app_rt_campaign,
app_rt_adgroup,
app_rt_content,
ROW_NUMBER() OVER(PARTITION BY user_hash ORDER BY created_at ASC) as rn
FROM
  `wego-cloud.shopcash_analytics.sessions`
WHERE
  user_hash is not null
  and user_hash != ""
  order by 1)
  where rn = 1
  order by 1),

  clients as 
  (select
* except(rn)
from
(SELECT
  created_at,
  user_hash,
  ua_source,
  ua_medium,
  ua_campaign,
  ua_adgroup,
  ua_content,
  ROW_NUMBER() OVER(PARTITION BY user_hash ORDER BY created_at ASC) as rn
FROM
  `wego-cloud.shopcash_analytics.clients`
  WHERE
  user_hash is not null
  and user_hash != "")
  where rn = 1
  order by user_hash)

select 
users.*,
if(lifetime_orders > 0, "transacting user", "non-transacting user") as user_status,
ltv.* except(user_hash),
sessions.* except(user_hash, created_at),
clients.* except(user_hash, created_at)
from 
users
left join 
ltv on ltv.user_hash = users.user_hash
left join 
sessions on users.user_hash = sessions.user_hash
left join 
clients on users.user_hash = clients.user_hash
{% endraw %}
