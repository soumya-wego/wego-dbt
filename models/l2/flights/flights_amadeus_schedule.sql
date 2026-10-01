{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : flights_amadeus_schedule
-- Destination: analysis.flights_amadeus_schedule  (unchanged)
-- Schedule   : 1 of month 06:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
with weekdays_table as 
(select
  id,
  marketing_Airline_code,
  start_date,
  end_date,
  departure_city_Code,
  departure_airport_code,
  arrival_city_Code,
  arrival_airport_Code,
  stops_count,
  "Indirect" AS Flight_type,
  trip_duration as duration,
  stopover_duration,
  weekday
from `wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*` as a,
unnest(schedules) as schedules ,
unnest(schedules.weekdays) as weekday
where _table_suffix = (SELECT max(_table_suffix) FROM `wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*`)),

stopover_codes as
(select id,string_agg(stop_codes,',') as stop_codes from  
`wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*` as a,
unnest(stop_codes) as stop_codes
where _table_suffix = (SELECT max(_table_suffix) FROM `wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*`)
group by 1),

stopover_durations as
(select id,string_agg(cast(stopover_durations as string),',') as stopover_durations from  
`wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*` as a,
unnest(stopover_durations) as stopover_durations
where _table_suffix = (SELECT max(_table_suffix) FROM `wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*`)
group by 1),

segments as 
(select id,
string_agg(cast(segments.duration as string),',') as segments_duration,
string_agg(segments.operating_airline_code,',') as operating_airline_Code
from 
`wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*` as a,
unnest(segments) as segments
where _table_suffix = (SELECT max(_table_suffix) FROM `wego-cloud.wego_amadeus_schedule.connecting_flight_schedule_*`)
group by 1
),

pre_final_table as 

(select 
a.marketing_Airline_code,
a.start_date,
a.end_date,
a.departure_city_Code,
a.departure_airport_code,
a.arrival_city_Code,
a.arrival_airport_Code,
a.stops_count,
a.Flight_type,
a.duration,
a.stopover_duration,
a.weekday,
b.stop_codes,
c.stopover_durations,
d.segments_duration,
d.operating_airline_Code
from weekdays_table as a 
left join stopover_codes as b 
on a.id = b.id
left join stopover_durations as c 
on a.id = c.id
left join segments as d 
on a.id = d.id 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16),

final_table as 

(select 
marketing_Airline_code,
start_date,
end_date,
departure_city_Code,
departure_airport_code,
arrival_city_Code,
arrival_airport_Code,
operating_airline_Code,
stops_count,
Flight_type,
duration,
stopover_duration,
stop_codes,
stopover_durations,
segments_duration,

 STRING_AGG(CAST(weekday AS string), ''
  ORDER BY
    weekday) as weekdays 
from pre_final_table 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),


cte AS (
  SELECT
    *
  FROM
    `wego-cloud.wego_amadeus_schedule.direct_flight_schedule_*`
    where _table_suffix = (SELECT max(_table_suffix) FROM `wego-cloud.wego_amadeus_schedule.direct_flight_schedule_*`)),
  cte_2 AS (
  SELECT
    cte.departure_airport_code,
    departure_city_code,
    arrival_city_code,
    cte.arrival_airport_code,
    marketing_airline_code,
    stopover_duration,
    segments.operating_airline_code,
    segments. stops_count,
    segments.duration,
    schedules.start_date,
    schedules.end_date,
    weekday
  FROM
    cte,
    UNNEST(cte.schedules) AS schedules,
    UNNEST(schedules.weekdays) AS weekday,
    UNNEST(cte.segments) AS segments
  GROUP BY
   1,2,3,4,5,6,7,8,9,10,11,12 ),

 final as   
(SELECT
  marketing_Airline_code,
  start_date,
  end_date,
  departure_city_Code,
  departure_airport_code,
  arrival_city_Code,
  arrival_airport_Code,
  operating_airline_code,
  stops_count,
  "Direct" AS Flight_type,
  duration,
  stopover_duration,
  STRING_AGG(CAST(weekday AS string), ''
  ORDER BY
    weekday) as weekdays
FROM
  cte_2

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12)


select  
marketing_Airline_code,
start_date,
end_date,
departure_city_Code,
departure_airport_code,
arrival_city_Code,
arrival_airport_Code,
operating_airline_code,
stops_count,
"Direct" AS Flight_type,
cast(duration as int) as duration,
stopover_duration,
NULL as stop_codes,
NULL as Stopover_Durations,
NULL as Segment_Durations,
weekdays,
case when weekdays like '%1%' then 1 end as weekday_1,
case when weekdays like '%2%' then 2 end as weekday_2,
case when weekdays like '%3%' then 3 end as weekday_3,
case when weekdays like '%4%' then 4 end as weekday_4,
case when weekdays like '%5%' then 5 end as weekday_5,
case when weekdays like '%6%' then 6 end as weekday_6,
case when weekdays like '%7%' then 7 end as weekday_7,
from final
union all
select
marketing_Airline_code,
start_date,
end_date,
departure_city_Code,
departure_airport_code,
arrival_city_Code,
arrival_airport_Code,
operating_airline_Code,
stops_count,
Flight_type,
duration,
stopover_duration,
stop_codes,
stopover_durations,
segments_duration,
weekdays,
case when weekdays like '%1%' then 1 end as weekday_1,
case when weekdays like '%2%' then 2 end as weekday_2,
case when weekdays like '%3%' then 3 end as weekday_3,
case when weekdays like '%4%' then 4 end as weekday_4,
case when weekdays like '%5%' then 5 end as weekday_5,
case when weekdays like '%6%' then 6 end as weekday_6,
case when weekdays like '%7%' then 7 end as weekday_7,
from final_table;
{% endraw %}
