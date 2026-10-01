{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : wegopro_flights_bookings
-- Destination: analysis.wegopro_flights_bookings  (unchanged)
-- Schedule   : every 24 hours   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- create table analysis.wegopro_flights_bookings 
-- partition by created_at_date as

-- with currency as    
-- (

--   SELECT date(effective) as effective,base,amount
--   FROM `wego-cloud.analytics.exchange_rates*` 
--   where _table_suffix between  "20250101" and "20251209" group by 1,2,3




-- ),



-- country_code as 

-- (select 
-- cou.code as country_code,
-- max(location.code) as city_code 
-- from

-- (SELECT * EXCEPT(dedupe)
--     FROM
--     (SELECT code,id, country_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY active DESC, updated_at DESC, created_at DESC) as dedupe
--     FROM `place_services.locations`)
--     WHERE dedupe = 1) as location 
-- LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id group by 1)

-- SELECT 
-- id,
-- companyId as company_id,
-- costCenterId as cost_center_id,
-- costCenter as cost_center,
-- searchId as search_id,
-- shortId as short_id,
-- userId as user_id,
-- externalId as external_id,
-- travellerIds as travellers_id,
-- company,
-- invoice,
-- date(createdAt) as created_at_date,
-- createdAt as created_at,
-- ticketedAt as ticketed_at,
-- approvedAt as approved_at,
-- failedAt as failed_at,
-- passengers,
-- state,
-- departureDate as departure_date,
-- returnDate as arrival_date,
-- travelClass as cabin_class,
-- tripType as trip_type,
-- deviceType as device_type,
-- app,
-- appVersion as app_version,
-- origin as departure_city_code,
-- h.country_code  as deaprture_country_code,
-- destination as arrival_city_code,
-- i.country_code  as arrival_country_code,
-- concat(origin,"-",destination) as route_cities,

-- providerId as provider_id,
-- groupId as group_id,
-- `group`,










-- concat(h.country_code,"-",i.country_code) as route_countries,

-- round(paymentAmount*b.amount,2) as total_amount_usd,

-- round(providerAmount*c.amount,2) as provider_amount_usd,

-- round(markupAmount*d.amount,2) as markup_amount_usd,

-- round(bookingFeeAmount*e.amount,2) as booking_fee_usd,

-- round(addOnsAmount*f.amount,2) as addons_Amount_usd,

-- round(ancillariesAmount*g.amount,2) as ancillaries_amount_usd,
-- cancelledAt as cancelled_at,
-- round(cancellationCharge*j.amount,2) as cancellation_charge_usd,

-- bookingPCC as booking_ipcc,
-- pcc,
-- policyViolations as policy_violations,
-- tags,
-- imported,


-- paymentId as payment_id,
-- paymentEntityName as payment_entity_name,
-- paymentState as payment_state,
-- paymentSource as payment_source,
-- policyStatus as policy_status


--  FROM `wego-cloud.wegopro.flight-bookings` as a
--  left join currency as b 
--  on a.paymentCurrency = b.base
--  and date(a.createdAt) = b.effective

-- left join currency as c
--  on a.providerAmountCurrency = c.base
--  and date(a.createdAt) = c.effective

--  left join currency as d
--  on a.markupAmountCurrency = d.base
--  and date(a.createdAt) = d.effective


--   left join currency as e
--  on a.bookingFeeCurrency = e.base
--  and date(a.createdAt) = e.effective

--   left join currency as f
--  on a.addOnsAmountCurrency = f.base
--  and date(a.createdAt) = f.effective

--  left join currency as g
--  on a.ancillariesAmountCurrency = g.base
--  and date(a.createdAt) = g.effective


--   left join country_code as h
--  on a.origin = h.city_code 


-- left join country_code as i
--  on a.destination = i.city_code 

--   left join currency as j
--  on a.cancellationCurrency = j.base
--  and date(a.cancelledAt) = j.effective
 


-- where date(createdAt) between "2025-01-01" and current_date
-- -1
--  qualify row_number()over(partition by shortId order by cancelledAt desc) = 1












with currency as    
(

  SELECT date(effective) as effective,base,amount
  FROM `wego-cloud.analytics.exchange_rates*` 
  where _table_suffix = (SELECT format('%s', format_date("%Y%m%d", date_sub(current_date(), interval 1 day)))) group by 1,2,3




),



country_code as 

(select 
cou.code as country_code,
location.code as city_code 
from

(SELECT * EXCEPT(dedupe)
    FROM
    (SELECT code,id, country_id, ROW_NUMBER() OVER (PARTITION BY code ORDER BY active DESC, updated_at DESC, created_at DESC) as dedupe
    FROM `place_services.locations`)
    WHERE dedupe = 1) as location 
LEFT JOIN `place_services.countries` as cou ON cou.id=location.country_id )

SELECT 
id,
companyId as company_id,
costCenterId as cost_center_id,
costCenter as cost_center,
searchId as search_id,
shortId as short_id,
userId as user_id,
externalId as external_id,
travellerIds as travellers_id,
company,
invoice,
date(createdAt) as created_at_date,
createdAt as created_at,
ticketedAt as ticketed_at,
approvedAt as approved_at,
failedAt as failed_at,
passengers,
state,
departureDate as departure_date,
returnDate as arrival_date,
travelClass as cabin_class,
tripType as trip_type,
deviceType as device_type,
app,
appVersion as app_version,
origin as departure_city_code,
h.country_code  as deaprture_country_code,
destination as arrival_city_code,
i.country_code  as arrival_country_code,
concat(origin,"-",destination) as route_cities,

providerId as provider_id,
groupId as group_id,
`group`,










concat(h.country_code,"-",i.country_code) as route_countries,

round(paymentAmount*b.amount,2) as total_amount_usd,

round(providerAmount*c.amount,2) as provider_amount_usd,

round(markupAmount*d.amount,2) as markup_amount_usd,

round(bookingFeeAmount*e.amount,2) as booking_fee_usd,

round(addOnsAmount*f.amount,2) as addons_Amount_usd,

round(ancillariesAmount*g.amount,2) as ancillaries_amount_usd,
cancelledAt as cancelled_at,
round(cancellationCharge*j.amount,2) as cancellation_charge_usd,

bookingPCC as booking_ipcc,
pcc,
policyViolations as policy_violations,
tags,
imported,


paymentId as payment_id,
paymentEntityName as payment_entity_name,
paymentState as payment_state,
paymentSource as payment_source,
policyStatus as policy_status


 FROM `wego-cloud.wegopro.flight-bookings` as a
 left join currency as b 
 on a.paymentCurrency = b.base
 and date(a.createdAt) = b.effective

left join currency as c
 on a.providerAmountCurrency = c.base
 and date(a.createdAt) = c.effective

 left join currency as d
 on a.markupAmountCurrency = d.base
 and date(a.createdAt) = d.effective


  left join currency as e
 on a.bookingFeeCurrency = e.base
 and date(a.createdAt) = e.effective

  left join currency as f
 on a.addOnsAmountCurrency = f.base
 and date(a.createdAt) = f.effective

 left join currency as g
 on a.ancillariesAmountCurrency = g.base
 and date(a.createdAt) = g.effective


  left join country_code as h
 on a.origin = h.city_code 


left join country_code as i
 on a.destination = i.city_code 

  left join currency as j
 on a.cancellationCurrency = j.base
 and date(a.cancelledAt) = j.effective
 


where date(createdAt)= current_date
-1
 qualify row_number()over(partition by shortId order by cancelledAt desc) = 1
{% endraw %}
