{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Amadeus_Schedule-Routes
-- Destination: Flights_Routes.amadeus_schedule-routes  (unchanged)
-- Schedule   : every 24 hours   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
SELECT marketing_Airline_code,departure_airport_code,arrival_airport_Code,departure_city_Code,arrival_city_Code,start_date,end_date FROM `wego-cloud.analysis.flights_amadeus_schedule`
{% endraw %}
