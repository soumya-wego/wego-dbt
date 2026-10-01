{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : integrated_bookings_flights_booking_margins_vendor_commissions_dedup2
-- Destination: integrated_bookings_flights.booking_margins  (unchanged)
-- Schedule   : every day 15:10   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('integrated_bookings_flights', 'vendor_commissions') }}
-- depends_on: {{ source('integrated_bookings_flights_staging', 'booking_margins') }}
-- depends_on: {{ source('integrated_bookings_flights_staging', 'vendor_commissions') }}
{% raw %}
-- Capture table changes/snapshots for booking_margins, vendor_commissions tables
-- Deduplicate booking_margins table
CREATE OR REPLACE TABLE integrated_bookings_flights.booking_margins AS
SELECT * EXCEPT (rn)
FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY id, updated_at ORDER BY updated_at)  as rn
FROM `wego-cloud.integrated_bookings_flights.booking_margins` )
WHERE rn = 1
ORDER BY id, updated_at;

CREATE OR REPLACE TABLE integrated_bookings_flights_staging.booking_margins AS
SELECT * EXCEPT (rn)
FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY id, updated_at ORDER BY updated_at)  as rn
FROM `wego-cloud.integrated_bookings_flights_staging.booking_margins` )
WHERE rn = 1
ORDER BY id, updated_at;

-- Deduplicate vendor_commissions table
CREATE OR REPLACE TABLE integrated_bookings_flights.vendor_commissions AS
SELECT * EXCEPT (rn)
FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY id, updated_at ORDER BY updated_at)  as rn
FROM `wego-cloud.integrated_bookings_flights.vendor_commissions` )
WHERE rn = 1
ORDER BY id, updated_at;

CREATE OR REPLACE TABLE integrated_bookings_flights_staging.vendor_commissions AS
SELECT * EXCEPT (rn)
FROM
(SELECT *, ROW_NUMBER() OVER (PARTITION BY id, updated_at ORDER BY updated_at)  as rn
FROM `wego-cloud.integrated_bookings_flights_staging.vendor_commissions` )
WHERE rn = 1
ORDER BY id, updated_at;
{% endraw %}
