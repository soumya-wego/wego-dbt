{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : bowh_supplier_availability_aggregated
-- Destination: analysis.bowh_supplier_availability_aggregated  (unchanged)
-- Schedule   : every day 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
insert into `wego-cloud.analysis.bowh_supplier_availability_aggregated`
(date, hotel_id, ts_code, site_code, device_type, hotel_name, supplier_code, supplier_name, supplier_channel, page, hotel_city_name, hotel_country_name, hotel_country_code, hotel_city_code, total_request, total_available)
with dd as
(
  select
    @run_date - interval '1' day as start_date
    , @run_date - interval '1' day as end_date
)

, requests as 
(
  select distinct
    search_id
    , session_id
    , supplier_code
    , supplier_name
    , supplier_channel
    , date(timestamp, 'Asia/Singapore') as date
    , hotel_id
    , timestamp
    , case 
       when session_id like '%preload%' then 'Preload'
       when split(session_id, '~')[offset(9)] = 'F' then 'Feed'
       when split(session_id, '~')[offset(9)] = 'B' then 'Landing' end as page
  from `wego-cloud.ib_hotels_supplier_worker.supplier_search_analytics*`
  left join unnest(requested_hotel_ids) hotel_id
  where _table_suffix between format_date('%Y%m%d', (select start_date from dd)) and format_date('%Y%m%d', (select end_date from dd))
)

, availables as
(
  select distinct
    search_id
    , session_id
    , supplier_code
    , supplier_name
    , supplier_channel
    , date(timestamp, 'Asia/Singapore') as date
    , hotel_id
    , timestamp
   from `wego-cloud.ib_hotels_supplier_worker.supplier_search_analytics*`
  left join unnest(available_hotel_ids) hotel_id
  where _table_suffix between format_date('%Y%m%d', (select start_date from dd)) and format_date('%Y%m%d', (select end_date from dd))
)

, supplier_requests as
(
  select
    a.* 
    , 1 as total_request
    , case when b.hotel_id is not null then 1 else 0 end as total_available
  from requests a
  left join availables b
    using(search_id, session_id, supplier_code, supplier_channel, hotel_id,timestamp)
  qualify row_number() over(partition by a.search_id, a.session_id, a.supplier_code, a.supplier_channel, a.hotel_id order by a.timestamp desc) = 1
)

, hotels_detail as
(
  select distinct
    a.hotel_id
    , b.name_en as hotel_name
    , c.base_name as hotel_city_name
    , d.base_name as hotel_country_name
    , d.code as hotel_country_code
    , c.code as hotel_city_code
  from `wego-cloud.hotels.provider_hotels` a
  left join `wego-cloud.hotel_services.hotels` b
    on a.hotel_id = b.id
  left join `wego-cloud.place_services.locations` c
    on b.city_code = c.code
  left join `wego-cloud.place_services.countries` d
    on c.country_id = d.id
  qualify row_number() over(partition by a.hotel_id order by d.code) = 1
)

, sessions_detail as
(
  select distinct
    search_id
    , device_type
    , ts_code
    , site_code
    , created_at
    , id as session_id
  from  `wego-cloud.ib_hotels.sessions*`
  where _table_suffix between format_date('%Y%m%d', (select start_date from dd)) and format_date('%Y%m%d', (select end_date from dd))
)

-- select * from  `wego-cloud.ib_hotels.sessions*`
--   where _table_suffix between format_date('%Y%m%d', (select start_date from dd)) and format_date('%Y%m%d', (select end_date from dd))
-- and search_id = '2d3113e404cb9091'

, details as
(
  select
    a.*
    , ts_code
    , site_code
    , device_type
    , c.* except(hotel_id)
  from supplier_requests a
  left join sessions_detail b
    on a.search_id = b.search_id
    and a.session_id = b.session_id
  left join hotels_detail c
    on a.hotel_id = cast(c.hotel_id as string)
)

, agg as
(
  select
    date
    , hotel_id
    , ts_code
    , site_code
    , device_type
    , hotel_name
    , supplier_code
    , supplier_name
    , supplier_channel
    , page
    , hotel_city_name
    , hotel_country_name
    , hotel_country_code
    , hotel_city_code
    , sum(total_request) as total_request
    , sum(total_available) as total_available
  from details
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select
  date, hotel_id, ts_code, site_code, device_type, hotel_name, supplier_code, supplier_name, supplier_channel, page, hotel_city_name, hotel_country_name, hotel_country_code, hotel_city_code, total_request, total_available
from agg
{% endraw %}
