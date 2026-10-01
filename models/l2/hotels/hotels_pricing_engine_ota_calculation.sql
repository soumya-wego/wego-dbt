{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : hotels_pricing_engine_ota_calculation
-- Destination: pricing_engine.hotels_pricing_engine_ota_calculation  (unchanged)
-- Schedule   : every day 01:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create or replace table `wego-cloud.pricing_engine.hotels_pricing_engine_ota_calculation` 
-- partition by date(created_at) as

with setup as
(
  select
    *
  from `analysis.pricing_engine_ota_setup`
)

, supplier_mapping as
(
  select
    supplier_code
    , supplier_name
  from `wego-cloud.analysis.bowh_supplier_mapping`
)

, bookings as
(
  select
    date(booking_at) as booking_date
    , hotel_id
    , hotel_city
    , promo_discount_amount_usd/wego_total_price_usd as promo_percentage 
    , wego_markup_percentage/100 as markup_percentage
    , click_id
    , booking_id
    , hotel_country_code
    , site_code
    , supplier_code
    , channel_type
    , (total_cost_of_sales_usd-distribution_cost_usd)/wego_total_price_usd as cost_percentage
  from `wego_analytics.hotels_bookings` a
  left join supplier_mapping b
    on lower(a.supplier_name) = lower(b.supplier_name)
  where conversions_tracked = 1
    and date(booking_at) >= current_date - interval '14' day
    and attribution_ts_code is null
    and a.supplier_name not in(
      'Booking'
      , 'Expedia Rapid'
    )
)

, base_cost as
(
  select
    site_code
    , hotel_city
    , hotel_country_code
    , supplier_code
    , channel_type
    , avg(cost_percentage) as avg_cost_percentage
    , approx_quantiles(markup_percentage,100)[50] as p50_markup_percentage
    , approx_quantiles(cost_percentage,100)[50] as p50_cost_percentage
    , approx_quantiles(cost_percentage,100)[60] as p60_cost_percentage
  from bookings
  where hotel_city is not null
    and supplier_code is not null
    and channel_type is not null
  group by 1,2,3,4,5

  union all

  select
    site_code
    , cast(null as string) as hotel_city
    , hotel_country_code
    , supplier_code
    , channel_type
    , avg(cost_percentage) as avg_cost_percentage
    , approx_quantiles(markup_percentage,100)[50] as p50_markup_percentage
    , approx_quantiles(cost_percentage,100)[50] as p50_cost_percentage
    , approx_quantiles(cost_percentage,100)[60] as p60_cost_percentage
  from bookings
  where supplier_code is not null
    and channel_type is not null
  group by 1,2,3,4,5

  union all

  select
    site_code
    , cast(null as string) as hotel_city
    , hotel_country_code
    , cast(null as string) supplier_code
    , channel_type
    , avg(cost_percentage) as avg_cost_percentage
    , approx_quantiles(markup_percentage,100)[50] as p50_markup_percentage
    , approx_quantiles(cost_percentage,100)[50] as p50_cost_percentage
    , approx_quantiles(cost_percentage,100)[60] as p60_cost_percentage
  from bookings
  group by 1,2,3,4,5

  union all

  select
    site_code
    , cast(null as string) as hotel_city
    , hotel_country_code
    , cast(null as string) supplier_code
    , cast(null as string) channel_type
    , avg(cost_percentage) as avg_cost_percentage
    , approx_quantiles(markup_percentage,100)[50] as p50_markup_percentage
    , approx_quantiles(cost_percentage,100)[50] as p50_cost_percentage
    , approx_quantiles(cost_percentage,100)[60] as p60_cost_percentage
  from bookings
  group by 1,2,3,4,5
)

, base_margin as
(
  select
    a.site_code
    , b.code as city_code
    , hotel_country_code
    , supplier_code
    , channel_type
    , round(p60_cost_percentage+coalesce(c.min_net_revenue_target,d.min_net_revenue_target),3) as target_margin
    , round(p50_markup_percentage,3) as p50_markup_percentage
  from base_cost a
  left join `wego-cloud.place_services.locations` b
    on hotel_city = base_name
  left join setup c
    on a.site_code = c.site_code
    and a.hotel_country_code = c.country_code
  left join setup d
    on a.site_code = d.site_code
    and d.country_code = 'ALL'
)


, searches as
(
  select distinct
    date(created_at) as search_date
    , site_code
    , country_code
    , city_code
    , search_id
  from `wego_analytics.hotels_searches`
  where TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= timestamp(DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
  group by 1,2,3,4,5
)

, total_clicks as
(
  select
    search_id
    , click_id
    , date(created_at) as click_date
  from `wego_analytics.hotels_clicks`
  where date(created_at) >= current_date - interval '90' day
  and provider_domain like '%wego%'
  group by 1,2,3
)

, ctr_calc as
(
  select
    search_date
    , site_code
    , country_code
    , city_code
    , count(distinct a.search_id) as total_search
    , count(distinct case when click_id is not null then a.search_id end) as total_search_clicked
    , safe_divide(count(distinct case when click_id is not null then a.search_id end) ,count(distinct a.search_id) ) as ctr_percentage
  from searches a
  left join total_clicks b
    on a.search_date = b.click_date
    and a.search_id = b.search_id
  where city_code is not null
  group by 1,2,3,4

  union all

  select
    search_date
    , site_code
    , country_code
    , null as city_code
    , count(distinct a.search_id) as total_search
    , count(distinct case when click_id is not null then a.search_id end) as total_search_clicked
    , safe_divide(count(distinct case when click_id is not null then a.search_id end) ,count(distinct a.search_id) ) as ctr_percentage
  from searches a
  left join total_clicks b
    on a.search_date = b.click_date
    and a.search_id = b.search_id
  group by 1,2,3
)

, ctr_base as (
  select
    a.search_date
    , a.site_code
    , a.country_code
    , a.city_code
    , a.total_search
    , a.ctr_percentage
    , avg(b.ctr_percentage) as avg_ctr_percentage           
    , stddev_samp(b.ctr_percentage) as std_ctr_percentage
  from ctr_calc a
  left join ctr_calc b
    on coalesce(a.city_code,'null') = coalesce(b.city_code,'null')
   and a.country_code = b.country_code
   and a.site_code = b.site_code
   and b.search_date between date_sub(a.search_date, interval 30 day)
                         and date_sub(a.search_date, interval 2 day)
  where a.search_date = current_date - interval '1' day

  group by 1,2,3,4,5,6
)


, baseline_based as (
  select
    search_date
    , site_code
    , country_code
    , city_code
    , total_search
    , ctr_percentage as p_hat
    , avg_ctr_percentage as p0
    , std_ctr_percentage as sd0
    , sqrt( greatest(avg_ctr_percentage * (1 - avg_ctr_percentage), 1e-12) / nullif(total_search, 0) ) as se0
    , avg_ctr_percentage - 1.96 * sqrt( greatest(avg_ctr_percentage * (1 - avg_ctr_percentage), 1e-12) / nullif(total_search, 0) ) as p0_ci_lower
    , avg_ctr_percentage + 1.96 * sqrt( greatest(avg_ctr_percentage * (1 - avg_ctr_percentage), 1e-12) / nullif(total_search, 0) ) as p0_ci_upper
    , safe_divide(ctr_percentage - avg_ctr_percentage, nullif(std_ctr_percentage, 0.0)) as z_score_hist  -- your original z vs history
  from ctr_base
)

, alerts as (
  select
    search_date
    , site_code
    , country_code
    , city_code
    , total_search
    , p_hat
    , p0
    , p0_ci_lower
    , p0_ci_upper
    , z_score_hist
    , case
        when total_search >= 100
         and p_hat > p0_ci_upper
        then 1 else 0 end as flag_increase_vs_baseline
    , case
        when total_search >= 100
         and p_hat < p0_ci_lower
        then 1 else 0 end as flag_decrease_vs_baseline
    , case
        when total_search >= 100
         and p_hat between p0_ci_lower and p0_ci_upper
        then 1 else 0 end as flag_no_change
    -- optional control limits if you also want variance-only bands (ignores n)
    , p0 - 2.0 * sd0 as lcl_2sd
    , p0 + 2.0 * sd0 as ucl_2sd
  from baseline_based
  -- where searc
)

, markup_final as
(
  select
    a.*
    , p_hat as average_ctr
    , p0_ci_lower
    , p0_ci_upper
    , flag_increase_vs_baseline
    , flag_decrease_vs_baseline
    , flag_no_change
    , case 
        when target_margin < p50_markup_percentage then
        (
          case when flag_increase_vs_baseline = 1 or flag_no_change = 1 then p50_markup_percentage + coalesce(c.incremental_percentage,d.incremental_percentage)
                when flag_decrease_vs_baseline = 1 then greatest(target_margin,p50_markup_percentage) - coalesce(c.incremental_percentage,d.incremental_percentage)
              else p50_markup_percentage end
        )
        else target_margin end as margin_percentage 
  from base_margin a
  left join alerts b
    on a.site_code = b.site_code
    and a.city_code = b.city_code
    and a.hotel_country_code = b.country_code
  left join setup c
    on a.site_code = c.site_code
    and a.hotel_country_code = c.country_code
  left join setup d
    on a.site_code = d.site_code
    and d.country_code = 'ALL'
)

select 
  *
  , current_datetime('Asia/Singapore') as created_at
from markup_final
{% endraw %}
