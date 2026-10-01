{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Wenrix export sample
-- Destination: aaaaa_temporary_export_folder.wenrix_export_sample_data  (unchanged)
-- Schedule   : every day 02:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.wenrix_export_sample_data` AS
SELECT
  string_field_0  AS segment,
  string_field_1  AS market,
  string_field_2  AS airline,
  string_field_3  AS gds,
  string_field_4  AS branch,
  string_field_5  AS new_ticket_numbers,
  string_field_6  AS post_ticket_issuance_fee,
  string_field_7  AS cancellation_fee,
  string_field_8  AS pnr,
  string_field_9  AS internal_id,
  string_field_10 AS booked_on,
  string_field_11 AS departs_at_local_time,
  int64_field_12  AS pax_num,
  string_field_13 AS pre_tkt_status,
  double_field_14 AS pre_tkt_tracking,
  string_field_15 AS ticketed_on,
  string_field_16 AS post_tkt_status,
  double_field_17 AS post_tkt_tracking,
  string_field_18 AS support_reason,
  double_field_19 AS original_price,
  double_field_20 AS ticketed_price,
  double_field_21 AS pre_tkt_savings,
  double_field_22 AS optivoid_savings,
  double_field_23 AS optirefund_savings,
  double_field_24 AS total_savings
FROM `wego-cloud.aaaaa_temporary_export_folder.wenrix_export_sample`
{% endraw %}
