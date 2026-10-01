{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : Sync booking.com assumed gross margin at 11:00am everyday
-- Destination: aaaaa_temporary_export_folder.booking_com_assumed_gross_margin  (unchanged)
-- Schedule   : every day 03:00   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
{% raw %}
-- Assume marketing fee from AJ inputs
CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.booking_com_assumed_gross_margin` AS (
  SELECT * FROM `wego-cloud.wego_analytics.booking_com_assumed_gross_margin`
  WHERE site_code IS NOT NULL
);

-- Actual marketing fee downloaded from Booking.com portal
CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.booking_com_bow_hotels_revenue_report` AS (
  SELECT
    Booking_date,
    Booking_number,
    Check_in_date,
    Check_out_date,
    Length_of_stay,
    Booking_window,
    Status,
    Your_commission,
    IFNULL(Your_commission_in_your_currency, 0.0) AS Your_commission_in_your_currency,
    Exchange_rate,
    Commission_percentage,
    Total_commission,
    Credit_slip,
    Property_name,
    Property_type,
    Country,
    City,
    UFI,
    Affiliate_ID,
    Label,
    Booker_country,
    Booker_language,
    User_device,
    Travel_purpose,
  FROM `wego-cloud.wego_analytics.booking_com_bow_hotels_revenue_report`
);
{% endraw %}
