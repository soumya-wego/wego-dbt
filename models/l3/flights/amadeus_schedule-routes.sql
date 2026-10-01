{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Amadeus_Schedule-Routes
-- Destination: Flights_Routes.amadeus_schedule-routes  (unchanged)
-- Schedule   : every 24 hours   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ ref('flights_amadeus_schedule') }}
{% raw %}
SELECT marketing_Airline_code,departure_airport_code,arrival_airport_Code,departure_city_Code,arrival_city_Code,start_date,end_date FROM `wego-cloud.analysis.flights_amadeus_schedule`
{% endraw %}
