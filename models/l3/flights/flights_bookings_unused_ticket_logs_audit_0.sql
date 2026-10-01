{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Flights Bookings Unused Tickets Logs Audit
-- Destination: aaaaa_temporary_export_folder.flights_bookings_unused_ticket_logs_audit_0  (unchanged)
-- Schedule   : every day 01:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- Define a JavaScript UDF to replace all scientific notation numbers with full numeric strings
CREATE TEMP FUNCTION ReplaceScientificNotation(input STRING)
RETURNS STRING
LANGUAGE js AS """
    // Use a regular expression to find all numbers in scientific notation
  return input.replace(/\\d+(?:\\.\\d+)?E\\+\\d+/g, function(match) {
    // Convert each matched number to a Number and format without decimals
    return Number(match).toFixed(0);
  });
""";
CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_unused_ticket_logs_audit_0`
AS
(
  SELECT
    a.created_at,
    CAST(NULL AS TIMESTAMP) AS refunded_on,
    CAST(NULL AS STRING) AS refund_type,
    a.site_code,
    a.ticket_status,
    a.booking_cancellation_reason,
    a.cancellation_date,
    a.pcc,
    a.ipcc_currency_code,
    a.ticket_numbers,
    a.gds_ref,
    a.booking_ref,
    a.supplier_payment_method,
    a.vendor,
    a.gds_refund_amount_usd,
    NULL AS unused_tickets_amount_usd
  FROM
    `wego-cloud.wego_analytics.flights_bookings` a
  WHERE
    unused_tickets_amount_usd > 0
);

CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.flights_bookings_unused_ticket_logs_audit_1`
AS
(
  SELECT
    issue_date AS created_at,
    temp_booking_ref_unused_tickets_2.refunded_on,
    temp_booking_ref_unused_tickets_2.refund_type,
    b.site_code,
    temp_booking_ref_unused_tickets_2.ticket_status,
    b.booking_cancellation_reason,
    b.cancellation_date,
    b.pcc,
    b.ipcc_currency_code,
    temp_booking_ref_unused_tickets_2.ticket_numbers,
    b.gds_ref,
    temp_booking_ref_unused_tickets_2.booking_ref,
    b.supplier_payment_method,
    b.vendor,
    b.gds_refund_amount_usd,
    b.unused_tickets_amount_usd
  FROM
  /* -- temp_booking_ref_unused_tickets_2 --
  booking_ref
	issue_date
	refunded_on
	ticket_status
	refund_type
	ticket_numbers
  */
	(SELECT
  unused_tickets.booking_ref,
  unused_tickets.issue_date,
  unused_tickets.refunded_on,
  unused_tickets.ticket_status,
  unused_tickets.refund_type,
  temp_booking_ref_unused_tickets_1.ticket_numbers,
FROM
`wego-cloud.integrated_bookings_flights.unused_tickets` AS unused_tickets
LEFT JOIN
/* -- temp_booking_ref_unused_tickets_1 --
booking_ref
ticket_numbers
*/
(SELECT
	booking_ref,
	STRING_AGG(num, ',') AS ticket_numbers,
FROM
/* -- temp_booking_ref_unused_tickets_0 --
booking_ref
input_string
num
*/
(/* Please adding ReplaceScientificNotation function creation from main.sql to here.. if you want to run this script independently */
WITH data AS (
  SELECT
    booking_ref,
    ticket_no AS str
  FROM `wego-cloud.integrated_bookings_flights.unused_tickets`
  WHERE ticket_no <> 'Invalid Date' AND booking_ref LIKE 'WF%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_ref, ticket_no) = 1
),
converted_data AS (
  SELECT
    booking_ref,
    ReplaceScientificNotation(str) AS str
  FROM data
),
parts AS (
  -- Split each ticket string on '-' into ordered parts
  SELECT
    booking_ref,
    str,
    part,
    pos,
    LENGTH(part) AS len
  FROM converted_data,
  UNNEST(SPLIT(str, '-')) AS part WITH OFFSET AS pos
),
initial_length AS (
  SELECT
    booking_ref,
    str,
    LENGTH(part) AS initial_length
  FROM parts
  WHERE pos = 0
),
initial AS (
  -- Suffix encoding only shortens (e.g. 1573419831389-390-391), so a part
  -- whose length is >= the first part's length is itself a base number;
  -- shorter parts are suffixes of the most recent base.
  SELECT
    parts.*,
    initial_length.initial_length,
    CASE WHEN LENGTH(part) >= initial_length.initial_length THEN 1 ELSE 0 END AS base_num_change
  FROM parts
  JOIN initial_length USING(booking_ref, str)
),
grouped AS (
  SELECT
    *,
    SUM(base_num_change) OVER (PARTITION BY booking_ref, str ORDER BY pos) AS group_num
  FROM initial
),
with_base AS (
  SELECT
    *,
    FIRST_VALUE(part) OVER (PARTITION BY booking_ref, str, group_num ORDER BY pos) AS base_num
  FROM grouped
),
numbers AS (
  SELECT
    booking_ref,
    str,
    pos,
    CASE
      WHEN base_num_change = 1 THEN base_num
      ELSE CONCAT(
        -- GREATEST clamp guards against malformed inputs slipping past the classifier
        SUBSTR(base_num, 1, GREATEST(LENGTH(base_num) - len, 0)),
        part
      )
    END AS num
  FROM with_base
)
SELECT
  booking_ref,
  str AS input_string,
  CAST(num AS STRING) AS num
FROM numbers
ORDER BY input_string, pos)
GROUP BY booking_ref) AS temp_booking_ref_unused_tickets_1
ON unused_tickets.booking_ref = temp_booking_ref_unused_tickets_1.booking_ref) AS temp_booking_ref_unused_tickets_2
	LEFT JOIN `wego-cloud.wego_analytics.flights_bookings` b ON b.booking_ref = temp_booking_ref_unused_tickets_2.booking_ref
);
{% endraw %}
