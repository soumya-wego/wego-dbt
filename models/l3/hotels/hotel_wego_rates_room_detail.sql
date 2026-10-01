{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : hotel_wego_rates_room_detail_daily_append
-- Destination: analysis.hotel_wego_rates_room_detail  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('ib_hotels_supplier_worker', 'raw_rates') }}
{% raw %}
begin

delete from `wego-cloud.analysis.hotel_wego_rates_room_detail`
where date = date_sub(current_date(), interval 1 day);

insert into `wego-cloud.analysis.hotel_wego_rates_room_detail`
(date, supplier_code, supplier_hotel_id, room_type_id, room_type_name, session_id, occurrence_count)

with date_param as
(
  select
    date_sub(current_date(), interval 1 day) as target_date
    , format_date('%Y%m%d', date_sub(current_date(), interval 1 day)) as target_date2
)

select
    (select target_date from date_param) as date
  , supplier_code
  , supplier_hotel_id
  , room_type_id
  , room_type_name
  , any_value(yorktown_session_id) as session_id
  , count(*) as occurrence_count
from `wego-cloud.ib_hotels_supplier_worker.raw_rates*`
where _table_suffix = (select target_date2 from date_param)
group by 1, 2, 3, 4, 5
;

end;
{% endraw %}
