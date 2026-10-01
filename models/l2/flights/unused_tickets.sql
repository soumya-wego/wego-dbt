{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : Unused_Tickets_2026_Mapped sync
-- Destination: integrated_bookings_flights.unused_tickets  (unchanged)
-- Schedule   : every day 23:30   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('google_sheets', 'unused_tickets_2026') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ ref('flights_bookings_ticket_level') }}
{% raw %}
MERGE INTO `wego-cloud.integrated_bookings_flights.unused_tickets` T
USING (
  WITH
    unused AS (
      SELECT
        NULLIF(string_field_1, '') AS pnr,
        NULLIF(string_field_2, '') AS airline,
        NULLIF(string_field_3, '') AS ticket_status,
        COUNT(1) AS pax_count,
        NULLIF(string_field_0, '') AS issue_date_string,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', string_field_0) AS issue_date,
        STRING_AGG(DISTINCT NULLIF(string_field_4, ''), ', ') AS ticket_no,
        NULLIF(string_field_7, '') AS refunded_on_string,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d', string_field_7) AS refunded_on,
        CASE
          WHEN NULLIF(string_field_6, '') IS NULL THEN NULL
          WHEN UPPER(TRIM(string_field_6)) LIKE 'REFUND UNUSED TAXES%'
            THEN 'Tax Refund'
          WHEN UPPER(TRIM(string_field_6)) LIKE 'TICKET IS NON-REFUNDABLE%'
            THEN 'Tax Refund'
          WHEN
            UPPER(TRIM(string_field_6)) LIKE 'VOLUNTARY REFUND%'
            OR UPPER(TRIM(string_field_6)) = 'V'
            THEN 'Discarded Refund'
          WHEN UPPER(TRIM(string_field_6)) LIKE 'INVOL%' THEN 'Discarded Refund'
          ELSE string_field_6
          END AS refund_type,
        SUM(SAFE_CAST(string_field_9 AS FLOAT64)) AS net_revenue,
        NULLIF(string_field_8, '') AS ipcc,
        NULLIF(string_field_10, '') AS currency
      FROM `wego-cloud.google_sheets.Unused_Tickets_2026`
      WHERE string_field_4 != 'Tickets No.'
      GROUP BY ALL
    ),
    joined AS (
      SELECT
        b.booking_ref,
        u.pnr,
        u.airline,
        u.ticket_status,
        u.pax_count,
        u.issue_date_string,
        u.issue_date,
        u.ticket_no,
        u.refunded_on_string,
        u.refunded_on,
        u.refund_type,
        u.net_revenue,
        u.ipcc,
        u.currency,
        ROW_NUMBER()
          OVER (
            PARTITION BY u.ticket_no
            ORDER BY
              b.booking_ref IS NULL,
              b.booking_ref  -- prefer a non-null booking_ref
          ) AS rn
      FROM unused u
      LEFT JOIN `wego-cloud.wego_analytics.flights_bookings_ticket_level` t
        ON t.gds_ref = u.pnr
      LEFT JOIN `wego-cloud.wego_analytics.flights_bookings` b
        ON b.booking_id = t.booking_id
    )
  SELECT * EXCEPT (rn)
  FROM joined
  WHERE rn = 1
) S
ON
  COALESCE(T.booking_ref, '') = COALESCE(S.booking_ref, '')
  AND COALESCE(T.pnr, '') = COALESCE(S.pnr, '')
  AND COALESCE(T.ticket_no, '')
    = COALESCE(S.ticket_no, '')
      WHEN MATCHED
        THEN
          UPDATE
SET
  T.airline = S.airline,
  T.ticket_status = S.ticket_status,
  T.pax_count = S.pax_count,
  T.issue_date_string = S.issue_date_string,
  T.issue_date = S.issue_date,
  T.refunded_on_string = S.refunded_on_string,
  T.refunded_on = S.refunded_on,
  T.refund_type = S.refund_type,
  T.net_revenue = S.net_revenue,
  T.ipcc = S.ipcc,
  T.currency = S.currency
    WHEN NOT MATCHED
      THEN
        INSERT(
          booking_ref,
          pnr,
          airline,
          ticket_status,
          pax_count,
          issue_date_string,
          issue_date,
          ticket_no,
          refunded_on_string,
          refunded_on,
          refund_type,
          net_revenue,
          ipcc,
          currency)
          VALUES(
            S.booking_ref,
            S.pnr,
            S.airline,
            S.ticket_status,
            S.pax_count,
            S.issue_date_string,
            S.issue_date,
            S.ticket_no,
            S.refunded_on_string,
            S.refunded_on,
            S.refund_type,
            S.net_revenue,
            S.ipcc,
            S.currency)
{% endraw %}
