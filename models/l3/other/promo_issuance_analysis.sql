{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : promo_issuance_analysis
-- Destination: analysis.promo_issuance_analysis  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
--create table analysis.promo_issuance_analysis as 


WITH RECURSIVE voucher_chain AS (
  SELECT
    p.created_at as created_at,
    p.id,
    pc.code AS promo_code,
    p.promo_value AS voucher_amount,
    p.currency_code,
    p.budget_owner.list AS Team,
    p.name,
    p.booking_ref AS refund_by_booking_ref,
    pc.id AS code_id,
    p.partially_redeemable,
    p.id AS root_campaign_id,
    0 AS level
  FROM `wego-cloud.pandora.promo_campaigns` p
  LEFT JOIN `wego-cloud.pandora.promo_codes` pc 
    ON p.id = pc.campaign_id
  WHERE lower(p.type) = 'giftcard' 
    AND p.enabled = true
    AND p.parent_code_id IS NULL
    AND p.recipient_email NOT IN (
      'faical@wego.com',
      'amjad+14433@wego.com',
      'naga@wego.com',
      'amjad@wego.com'
    )

  UNION ALL

  SELECT
    child_p.created_at as created_at,
    child_p.id,
    child_pc.code,
    child_p.promo_value,
    child_p.currency_code,
    child_p.budget_owner.list,
    child_p.name,
    child_p.booking_ref,
    child_pc.id,
    child_p.partially_redeemable,
    vc.root_campaign_id,
    vc.level + 1
  FROM voucher_chain vc
  JOIN `wego-cloud.pandora.promo_campaigns` child_p 
    ON child_p.parent_code_id = vc.code_id
  LEFT JOIN `wego-cloud.pandora.promo_codes` child_pc 
    ON child_p.id = child_pc.campaign_id
),


promo_code_reservations as 

(
select code_id,client_id,status,expired_at from `wego-cloud.pandora.promo_code_reservations` 
qualify row_number() over(partition by code_id,client_id order by expired_at desc ) = 1
),


final_table as
(select 
base.*, 
pcr.client_id,
pcr.status,expired_at,
dense_rank() over(partition by base.root_campaign_id order by base.level desc) as rank


from voucher_chain as base
left join promo_code_reservations as pcr 
on base.code_id = pcr.code_id ),






final_table_agg as 
(select 
root_campaign_id,
min(created_at) as created_at,
min(partially_redeemable) as partially_redeemable,
sum(voucher_amount) as total_voucher_amount,
min(currency_code) as currency_code , 
max(case when status = "Used" then expired_at end) as redemption_date,
max(case when rank = 1 then status end) as final_status,
MAX(level)  AS chain_depth,
min(refund_by_booking_ref) as refund_by_booking_ref,
coalesce(sum(case when status = "Used" then voucher_amount end),0)  as total_voucher_amount_used,


coalesce(sum(voucher_amount),0) - coalesce(sum(case when status = "Used" then voucher_amount end ),0) as total_voucher_amount_left,
case when coalesce(sum(voucher_amount),0) - coalesce(sum(case when status = "Used" then voucher_amount end ),0) between -1 and 1 then "Used" 
when coalesce(sum(case when status = "Used" then voucher_amount end ),0) = 0 then "Unused" 
else "Partially Used" end as campaign_id_status,


STRING_AGG(
    CONCAT(
      CASE level
        WHEN 0 THEN 'Original'
        ELSE CONCAT('Remainder-', level)
      END,
      ': ',
      promo_code,
      ' (', CAST(voucher_amount AS STRING), ' ', currency_code, ')',
      IFNULL(CONCAT(' [', status, ']'), ' [Unused]')
    ),
    ' → '
    ORDER BY level
  )  AS voucher_chain,



from final_table group by 1
),



needed_rates AS (
  SELECT DISTINCT FORMAT_DATE('%Y%m%d', DATE(created_at)) as created_at_date  , currency_code FROM final_table_agg
),

exchange_rates AS (
  SELECT
    _TABLE_SUFFIX AS rate_date,
    base,
    amount        AS fx_to_usd
  FROM `wego-cloud.analytics.exchange_rates*`
  WHERE
    LENGTH(_TABLE_SUFFIX) = 8
    AND _TABLE_SUFFIX IN (SELECT DISTINCT created_at_date FROM needed_rates)
    AND base       IN (SELECT DISTINCT currency_code  FROM needed_rates)
    AND quote = 'USD'
    AND amount > 0
)

select a.*,


  round(a.total_voucher_amount  * COALESCE(b.fx_to_usd, 1),2) AS total_voucher_amount_usd,
  round(a.total_voucher_amount_used     * COALESCE(b.fx_to_usd, 1),2) AS total_voucher_amount_used_usd,
  round(a.total_voucher_amount_left     * COALESCE(b.fx_to_usd, 1),2) AS total_voucher_amount_usd_left,
  c.site_code
FROM final_table_agg as a 
LEFT JOIN exchange_rates as b 
  ON  cast(b.rate_date as string) =  cast(FORMAT_DATE('%Y%m%d', DATE(a.created_at)) as string)       
  AND b.base      = a.currency_code

left join (select booking_ref,max(site_code) as site_Code from `integrated_bookings_flights.bookings*` where _table_suffix >= "20260101"
group by 1) as c   
on   a.refund_by_booking_ref = c.booking_ref
{% endraw %}
