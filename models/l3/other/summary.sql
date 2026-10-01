{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : rfm_summary
-- Destination: rfm_analysis.summary  (unchanged)
-- Schedule   : every wed 15:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('wego_analytics', 'flights_bookables') }}
-- depends_on: {{ source('wego_analytics', 'flights_bookings') }}
-- depends_on: {{ source('wego_analytics', 'flights_clicks') }}
-- depends_on: {{ source('wego_analytics', 'flights_insurance') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookables') }}
-- depends_on: {{ source('wego_analytics', 'hotels_bookings') }}
-- depends_on: {{ source('wego_analytics', 'hotels_clicks') }}
{% raw %}
--- Full Run
DECLARE start_date_historical DATE DEFAULT DATE("2023-01-01");
DECLARE end_date_historical DATE DEFAULT DATE("2023-06-01");

-- DECLARE start_date_incremental DATE DEFAULT DATE_ADD(DATE end_date_historical, INTERVAL 1 DAY);
DECLARE start_date_incremental DATE;
DECLARE end_date_incremental DATE;
DECLARE start_date_processing_block DATE;
DECLARE end_date_processing_block DATE;

DECLARE processing_block_range INT64 DEFAULT 120;

-- -------- HISTORICAL RUN --------
-- CREATE OR REPLACE TABLE rfm_analysis.summary
-- PARTITION BY DATE(created_at)
-- CLUSTER BY client_id
-- AS (
-- SELECT 
-- client_id,
-- created_at,
-- site_code,
-- device_type,
-- product,
-- f_conversions_tracked,
-- f_finance_revenue_usd,
-- f_total_booking_value_usd,
-- f_conversions_tracked_cumulative,
-- f_finance_revenue_usd_cumulative,
-- f_total_booking_value_usd_cumulative,
-- f_latest,

-- h_conversions_tracked,
-- h_finance_revenue_usd,
-- h_total_booking_value_usd,
-- h_conversions_tracked_cumulative,
-- h_finance_revenue_usd_cumulative,
-- h_total_booking_value_usd_cumulative,
-- h_latest,

-- fb_conversions_tracked,
-- fb_finance_revenue_usd,
-- fb_total_booking_value_usd,
-- fb_conversions_tracked_cumulative,
-- fb_finance_revenue_usd_cumulative,
-- fb_total_booking_value_usd_cumulative,
-- fb_latest,

-- hb_conversions_tracked,
-- hb_finance_revenue_usd,
-- hb_total_booking_value_usd,
-- hb_conversions_tracked_cumulative,
-- hb_finance_revenue_usd_cumulative,
-- hb_total_booking_value_usd_cumulative,
-- hb_latest,

-- fcau_conversions_tracked,
-- fcau_finance_revenue_usd,
-- fcau_total_booking_value_usd,
-- fcau_conversions_tracked_cumulative,
-- fcau_finance_revenue_usd_cumulative,
-- fcau_total_booking_value_usd_cumulative,
-- fcau_latest,

-- hcau_conversions_tracked,
-- hcau_finance_revenue_usd,
-- hcau_total_booking_value_usd,
-- hcau_conversions_tracked_cumulative,
-- hcau_finance_revenue_usd_cumulative,
-- hcau_total_booking_value_usd_cumulative,
-- hcau_latest,

-- fi_conversions_tracked,
-- fi_finance_revenue_usd,
-- fi_total_booking_value_usd,
-- fi_conversions_tracked_cumulative,
-- fi_finance_revenue_usd_cumulative,
-- fi_total_booking_value_usd_cumulative,
-- fi_latest,
  
-- IFNULL(f_conversions_tracked_cumulative, 0) +
-- IFNULL(h_conversions_tracked_cumulative, 0) +
-- IFNULL(fb_conversions_tracked_cumulative, 0) +
-- IFNULL(hb_conversions_tracked_cumulative, 0) +
-- IFNULL(fcau_conversions_tracked_cumulative, 0) +
-- IFNULL(hcau_conversions_tracked_cumulative, 0) +
-- IFNULL(fi_conversions_tracked_cumulative, 0) AS total_conversions_tracked_cumulative,

-- IFNULL(f_finance_revenue_usd_cumulative, 0) +
-- IFNULL(h_finance_revenue_usd_cumulative, 0) +
-- IFNULL(fb_finance_revenue_usd_cumulative, 0) +
-- IFNULL(hb_finance_revenue_usd_cumulative, 0) +
-- IFNULL(fcau_finance_revenue_usd_cumulative, 0) +
-- IFNULL(hcau_finance_revenue_usd_cumulative, 0) +
-- IFNULL(fi_finance_revenue_usd_cumulative, 0) AS total_finance_revenue_usd_cumulative,

-- IFNULL(f_total_booking_value_usd_cumulative, 0) +
-- IFNULL(h_total_booking_value_usd_cumulative, 0) +
-- IFNULL(fb_total_booking_value_usd_cumulative, 0) +
-- IFNULL(hb_total_booking_value_usd_cumulative, 0) +
-- IFNULL(fcau_total_booking_value_usd_cumulative, 0) +
-- IFNULL(hcau_total_booking_value_usd_cumulative, 0) +
-- IFNULL(fi_total_booking_value_usd_cumulative, 0) AS total_booking_value_usd_cumulative,

-- latest,
-- processing_date,  
-- FROM
--   (SELECT 
--   client_id, 
--   created_at, 
--   site_code,
--   device_type,
--   product,
--   f_conversions_tracked,
--   f_finance_revenue_usd, 
--   f_total_booking_value_usd,
--   SUM(f_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_conversions_tracked_cumulative,
--   SUM(f_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_finance_revenue_usd_cumulative,
--   SUM(f_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_total_booking_value_usd_cumulative,
--   IF(f_created_at = MAX(f_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as f_latest,

--   h_conversions_tracked,
--   h_finance_revenue_usd, 
--   h_total_booking_value_usd,
--   SUM(h_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_conversions_tracked_cumulative,
--   SUM(h_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_finance_revenue_usd_cumulative,
--   SUM(h_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_total_booking_value_usd_cumulative,
--   IF(h_created_at = MAX(h_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as h_latest,

--   fb_conversions_tracked,
--   fb_finance_revenue_usd, 
--   fb_total_booking_value_usd,
--   SUM(fb_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_conversions_tracked_cumulative,
--   SUM(fb_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_finance_revenue_usd_cumulative,
--   SUM(fb_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_total_booking_value_usd_cumulative,
--   IF(fb_created_at = MAX(fb_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fb_latest,

--   hb_conversions_tracked,
--   hb_finance_revenue_usd, 
--   hb_total_booking_value_usd,
--   SUM(hb_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_conversions_tracked_cumulative,
--   SUM(hb_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_finance_revenue_usd_cumulative,
--   SUM(hb_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_total_booking_value_usd_cumulative,
--   IF(hb_created_at = MAX(hb_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as hb_latest,

--   fcau_conversions_tracked,
--   fcau_finance_revenue_usd, 
--   fcau_total_booking_value_usd,
--   SUM(fcau_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_conversions_tracked_cumulative,
--   SUM(fcau_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_finance_revenue_usd_cumulative,
--   SUM(fcau_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_total_booking_value_usd_cumulative,
--   IF(fcau_created_at = MAX(fcau_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fcau_latest,

--   hcau_conversions_tracked,
--   hcau_finance_revenue_usd, 
--   hcau_total_booking_value_usd,
--   SUM(hcau_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_conversions_tracked_cumulative,
--   SUM(hcau_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_finance_revenue_usd_cumulative,
--   SUM(hcau_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_total_booking_value_usd_cumulative,
--   IF(hcau_created_at = MAX(hcau_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as hcau_latest,

--   fi_conversions_tracked,
--   fi_finance_revenue_usd, 
--   fi_total_booking_value_usd,
--   SUM(fi_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_conversions_tracked_cumulative,
--   SUM(fi_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_finance_revenue_usd_cumulative,
--   SUM(fi_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_total_booking_value_usd_cumulative,
--   IF(fi_created_at = MAX(fi_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fi_latest,

--   IF(created_at = MAX(created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as latest,
--   end_date_historical AS processing_date

--   FROM
--   (SELECT client_id, created_at, site_code, device_type, "Flights Meta" as product,
--   conversions_tracked as f_conversions_tracked,
--   finance_revenue_usd as f_finance_revenue_usd, 
--   IF(conversions_tracked > 0, total_price_usd, NULL) as f_total_booking_value_usd,
--   created_at as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,   
--   FROM `wego-cloud.wego_analytics.flights_clicks` 
--   WHERE DATE(_PARTITIONDATE) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL 

--   SELECT client_id, created_at, site_code, device_type, "Hotels Meta" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   conversions_tracked as h_conversions_tracked,
--   finance_revenue_usd as h_finance_revenue_usd, 
--   IF(conversions_tracked > 0, total_price_usd, NULL) as h_total_booking_value_usd,
--   created_at as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.hotels_clicks` 
--   WHERE DATE(_PARTITIONDATE) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL

--   SELECT client_id, created_at, site_code, device_type, "Flights BoW" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   conversions_tracked as fb_conversions_tracked,
--   finance_revenue_usd as fb_finance_revenue_usd, 
--   IF(conversions_tracked > 0, total_price_usd, NULL) as fb_total_booking_value_usd,
--   created_at as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.flights_bookings` 
--   WHERE DATE(created_at) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL 

--   SELECT client_id, created_at, site_code, device_type, "Hotels BoW" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   conversions_tracked as hb_conversions_tracked,
--   finance_revenue_usd as hb_finance_revenue_usd, 
--   IF(conversions_tracked > 0, wego_total_price_usd, NULL) as hb_total_booking_value_usd,
--   created_at as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.hotels_bookings` 
--   WHERE DATE(created_at) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL

--   SELECT client_id, created_at, site_code, device_type, "Flights Cau" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   conversions_adjusted as fcau_conversions_tracked,
--   finance_revenue_usd as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   created_at as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.flights_bookables` 
--   WHERE DATE(_PARTITIONDATE) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL 

--   SELECT client_id, created_at, site_code, device_type, "Hotels Cau" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   conversions_adjusted as hcau_conversions_tracked,
--   finance_revenue_usd as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   created_at as hcau_created_at,
--   NULL as fi_conversions_tracked,
--   NULL as fi_finance_revenue_usd, 
--   NULL as fi_total_booking_value_usd,
--   NULL as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.hotels_bookables` 
--   WHERE DATE(_PARTITIONDATE) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

--   UNION ALL 

--   SELECT client_id, created_at, site_code, device_type, "Flights Insurance" as product,
--   NULL as f_conversions_tracked,
--   NULL as f_finance_revenue_usd, 
--   NULL as f_total_booking_value_usd,
--   NULL as f_created_at,
--   NULL as h_conversions_tracked,
--   NULL as h_finance_revenue_usd, 
--   NULL as h_total_booking_value_usd,
--   NULL as h_created_at,
--   NULL as fb_conversions_tracked,
--   NULL as fb_finance_revenue_usd, 
--   NULL as fb_total_booking_value_usd,
--   NULL as fb_created_at, 
--   NULL as hb_conversions_tracked,
--   NULL as hb_finance_revenue_usd, 
--   NULL as hb_total_booking_value_usd,
--   NULL as hb_created_at,
--   NULL as fcau_conversions_tracked,
--   NULL as fcau_finance_revenue_usd, 
--   NULL as fcau_total_booking_value_usd,
--   NULL as fcau_created_at,
--   NULL as hcau_conversions_tracked,
--   NULL as hcau_finance_revenue_usd, 
--   NULL as hcau_total_booking_value_usd,
--   NULL as hcau_created_at,
--   conversions_adjusted as fi_conversions_tracked,
--   finance_revenue_usd as fi_finance_revenue_usd, 
--   IF(conversions_tracked > 0, total_price_usd, NULL) as fi_total_booking_value_usd,
--   created_at as fi_created_at,  
--   FROM `wego-cloud.wego_analytics.flights_insurance` 
--   WHERE DATE(_PARTITIONDATE) BETWEEN start_date_historical AND end_date_historical
--   AND finance_revenue_usd > 0 
--   -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
--   -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
--   AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")
--   )
-- )

-- );


------ INCREMENTAL RUN --------
EXECUTE IMMEDIATE "SELECT DATE_ADD(MAX(processing_date), INTERVAL 1 DAY) FROM `rfm_analysis.summary`" INTO start_date_incremental;
EXECUTE IMMEDIATE "SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)" INTO end_date_incremental;
-- EXECUTE IMMEDIATE 'SELECT DATE("2023-07-01")' INTO end_date_incremental;

SET start_date_processing_block = start_date_incremental;
LOOP
  IF DATE_DIFF(end_date_incremental, start_date_processing_block, DAY) >= processing_block_range THEN
    SET end_date_processing_block = DATE_ADD(start_date_processing_block, INTERVAL processing_block_range DAY);
  ELSEIF DATE_DIFF(end_date_incremental, start_date_processing_block, DAY) <= 0 THEN
    BREAK;
  ELSEIF DATE_DIFF(end_date_incremental, start_date_processing_block, DAY) < processing_block_range THEN
    SET end_date_processing_block = end_date_incremental;
  END IF;

  -- SELECT start_date_incremental, end_date_incremental, start_date_processing_block, end_date_processing_block;

  CREATE OR REPLACE TABLE rfm_analysis.summary
  PARTITION BY DATE(created_at)
  CLUSTER BY client_id
  AS (
  SELECT 
  client_id,
  created_at,
  site_code,
  device_type,
  product,
  f_conversions_tracked,
  f_finance_revenue_usd,
  f_total_booking_value_usd,
  f_conversions_tracked_cumulative,
  f_finance_revenue_usd_cumulative,
  f_total_booking_value_usd_cumulative,
  f_latest,

  h_conversions_tracked,
  h_finance_revenue_usd,
  h_total_booking_value_usd,
  h_conversions_tracked_cumulative,
  h_finance_revenue_usd_cumulative,
  h_total_booking_value_usd_cumulative,
  h_latest,

  fb_conversions_tracked,
  fb_finance_revenue_usd,
  fb_total_booking_value_usd,
  fb_conversions_tracked_cumulative,
  fb_finance_revenue_usd_cumulative,
  fb_total_booking_value_usd_cumulative,
  fb_latest,

  hb_conversions_tracked,
  hb_finance_revenue_usd,
  hb_total_booking_value_usd,
  hb_conversions_tracked_cumulative,
  hb_finance_revenue_usd_cumulative,
  hb_total_booking_value_usd_cumulative,
  hb_latest,

  fcau_conversions_tracked,
  fcau_finance_revenue_usd,
  fcau_total_booking_value_usd,
  fcau_conversions_tracked_cumulative,
  fcau_finance_revenue_usd_cumulative,
  fcau_total_booking_value_usd_cumulative,
  fcau_latest,

  hcau_conversions_tracked,
  hcau_finance_revenue_usd,
  hcau_total_booking_value_usd,
  hcau_conversions_tracked_cumulative,
  hcau_finance_revenue_usd_cumulative,
  hcau_total_booking_value_usd_cumulative,
  hcau_latest,

  fi_conversions_tracked,
  fi_finance_revenue_usd,
  fi_total_booking_value_usd,
  fi_conversions_tracked_cumulative,
  fi_finance_revenue_usd_cumulative,
  fi_total_booking_value_usd_cumulative,
  fi_latest,
    
  IFNULL(f_conversions_tracked_cumulative, 0) +
  IFNULL(h_conversions_tracked_cumulative, 0) +
  IFNULL(fb_conversions_tracked_cumulative, 0) +
  IFNULL(hb_conversions_tracked_cumulative, 0) +
  IFNULL(fcau_conversions_tracked_cumulative, 0) +
  IFNULL(hcau_conversions_tracked_cumulative, 0) +
  IFNULL(fi_conversions_tracked_cumulative, 0) AS total_conversions_tracked_cumulative,

  IFNULL(f_finance_revenue_usd_cumulative, 0) +
  IFNULL(h_finance_revenue_usd_cumulative, 0) +
  IFNULL(fb_finance_revenue_usd_cumulative, 0) +
  IFNULL(hb_finance_revenue_usd_cumulative, 0) +
  IFNULL(fcau_finance_revenue_usd_cumulative, 0) +
  IFNULL(hcau_finance_revenue_usd_cumulative, 0) +
  IFNULL(fi_finance_revenue_usd_cumulative, 0) AS total_finance_revenue_usd_cumulative,

  IFNULL(f_total_booking_value_usd_cumulative, 0) +
  IFNULL(h_total_booking_value_usd_cumulative, 0) +
  IFNULL(fb_total_booking_value_usd_cumulative, 0) +
  IFNULL(hb_total_booking_value_usd_cumulative, 0) +
  IFNULL(fcau_total_booking_value_usd_cumulative, 0) +
  IFNULL(hcau_total_booking_value_usd_cumulative, 0) +
  IFNULL(fi_total_booking_value_usd_cumulative, 0) AS total_booking_value_usd_cumulative,

  latest,
  processing_date,  
  FROM
    (SELECT 
    client_id,
    created_at,
    site_code, 
    device_type, 
    product,
    f_conversions_tracked,
    f_finance_revenue_usd,
    f_total_booking_value_usd,
    f_conversions_tracked_cumulative,
    f_finance_revenue_usd_cumulative,
    f_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_clicks` 
    WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0),0,f_latest) as f_latest,

    h_conversions_tracked,
    h_finance_revenue_usd,
    h_total_booking_value_usd,
    h_conversions_tracked_cumulative,
    h_finance_revenue_usd_cumulative,
    h_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_clicks` 
    WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 ),0,h_latest) as h_latest,

    fb_conversions_tracked,
    fb_finance_revenue_usd,
    fb_total_booking_value_usd,
    fb_conversions_tracked_cumulative,
    fb_finance_revenue_usd_cumulative,
    fb_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_bookings` 
    WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0),0,fb_latest) as fb_latest,

    hb_conversions_tracked,
    hb_finance_revenue_usd,
    hb_total_booking_value_usd,
    hb_conversions_tracked_cumulative,
    hb_finance_revenue_usd_cumulative,
    hb_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_bookings` 
    WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 ),0,hb_latest) as hb_latest,

    fcau_conversions_tracked,
    fcau_finance_revenue_usd,
    fcau_total_booking_value_usd,
    fcau_conversions_tracked_cumulative,
    fcau_finance_revenue_usd_cumulative,
    fcau_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_bookables` 
    WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0),0,fcau_latest) as fcau_latest,

    hcau_conversions_tracked,
    hcau_finance_revenue_usd,
    hcau_total_booking_value_usd,
    hcau_conversions_tracked_cumulative,
    hcau_finance_revenue_usd_cumulative,
    hcau_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_bookables` 
    WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 ),0,hcau_latest) as hcau_latest,

    fi_conversions_tracked,
    fi_finance_revenue_usd,
    fi_total_booking_value_usd,
    fi_conversions_tracked_cumulative,
    fi_finance_revenue_usd_cumulative,
    fi_total_booking_value_usd_cumulative,
    IF(client_id in (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_insurance` 
    WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 ),0,fi_latest) as fi_latest,

    IF(client_id in (SELECT DISTINCT client_id 
    FROM
      (SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_clicks` 
      WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_clicks` 
      WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_bookings` 
      WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_bookings` 
      WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_bookables` 
      WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.hotels_bookables` 
      WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0 
      UNION ALL
      SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_insurance` 
      WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block AND finance_revenue_usd > 0
      )
    ),0,latest) as latest,
    end_date_processing_block AS processing_date 
    FROM `rfm_analysis.summary`
    UNION ALL
    SELECT a.client_id, a.created_at, a.site_code, a.device_type, a.product, 
    a.f_conversions_tracked, 
    a.f_finance_revenue_usd,
    a.f_total_booking_value_usd,
    IFNULL(a.f_conversions_tracked_cumulative,0) + IFNULL(b.f_conversions_tracked_cumulative,0) as f_conversions_tracked_cumulative, 
    IFNULL(a.f_finance_revenue_usd_cumulative,0) + IFNULL(b.f_finance_revenue_usd_cumulative,0) as f_finance_revenue_usd_cumulative, 
    IFNULL(a.f_total_booking_value_usd_cumulative,0) + IFNULL(b.f_total_booking_value_usd_cumulative,0) as f_total_booking_value_usd_cumulative,
    a.f_latest,

    a.h_conversions_tracked, 
    a.h_finance_revenue_usd,
    a.h_total_booking_value_usd,
    IFNULL(a.h_conversions_tracked_cumulative, 0) + IFNULL(b.h_conversions_tracked_cumulative, 0) as h_conversions_tracked_cumulative, 
    IFNULL(a.h_finance_revenue_usd_cumulative, 0) + IFNULL(b.h_finance_revenue_usd_cumulative, 0) as h_finance_revenue_usd_cumulative,
    IFNULL(a.h_total_booking_value_usd_cumulative, 0) + IFNULL(b.h_total_booking_value_usd_cumulative, 0) as h_total_booking_value_usd_cumulative,
    a.h_latest,

    a.fb_conversions_tracked, 
    a.fb_finance_revenue_usd,
    a.fb_total_booking_value_usd,
    IFNULL(a.fb_conversions_tracked_cumulative,0) + IFNULL(b.fb_conversions_tracked_cumulative,0) as fb_conversions_tracked_cumulative, 
    IFNULL(a.fb_finance_revenue_usd_cumulative,0) + IFNULL(b.fb_finance_revenue_usd_cumulative,0) as fb_finance_revenue_usd_cumulative,
    IFNULL(a.fb_total_booking_value_usd_cumulative,0) + IFNULL(b.fb_total_booking_value_usd_cumulative,0) as fb_total_booking_value_usd_cumulative,
    a.fb_latest,

    a.hb_conversions_tracked, 
    a.hb_finance_revenue_usd,
    a.hb_total_booking_value_usd,
    IFNULL(a.hb_conversions_tracked_cumulative, 0) + IFNULL(b.hb_conversions_tracked_cumulative, 0) as hb_conversions_tracked_cumulative, 
    IFNULL(a.hb_finance_revenue_usd_cumulative, 0) + IFNULL(b.hb_finance_revenue_usd_cumulative, 0) as hb_finance_revenue_usd_cumulative,
    IFNULL(a.hb_total_booking_value_usd_cumulative, 0) + IFNULL(b.hb_total_booking_value_usd_cumulative, 0) as hb_total_booking_value_usd_cumulative,
    a.hb_latest,

    a.fcau_conversions_tracked, 
    a.fcau_finance_revenue_usd,
    a.fcau_total_booking_value_usd,
    IFNULL(a.fcau_conversions_tracked_cumulative,0) + IFNULL(b.fcau_conversions_tracked_cumulative,0) as fcau_conversions_tracked_cumulative, 
    IFNULL(a.fcau_finance_revenue_usd_cumulative,0) + IFNULL(b.fcau_finance_revenue_usd_cumulative,0) as fcau_finance_revenue_usd_cumulative,
    IFNULL(a.fcau_total_booking_value_usd_cumulative,0) + IFNULL(b.fcau_total_booking_value_usd_cumulative,0) as fcau_total_booking_value_usd_cumulative,
    a.fcau_latest,

    a.hcau_conversions_tracked, 
    a.hcau_finance_revenue_usd,
    a.hcau_total_booking_value_usd,
    IFNULL(a.hcau_conversions_tracked_cumulative, 0) + IFNULL(b.hcau_conversions_tracked_cumulative, 0) as hcau_conversions_tracked_cumulative, 
    IFNULL(a.hcau_finance_revenue_usd_cumulative, 0) + IFNULL(b.hcau_finance_revenue_usd_cumulative, 0) as hcau_finance_revenue_usd_cumulative,
    IFNULL(a.hcau_total_booking_value_usd_cumulative, 0) + IFNULL(b.hcau_total_booking_value_usd_cumulative, 0) as hcau_total_booking_value_usd_cumulative,
    a.hcau_latest,

    a.fi_conversions_tracked, 
    a.fi_finance_revenue_usd,
    a.fi_total_booking_value_usd,
    IFNULL(a.fi_conversions_tracked_cumulative, 0) + IFNULL(b.fi_conversions_tracked_cumulative, 0) as fi_conversions_tracked_cumulative, 
    IFNULL(a.fi_finance_revenue_usd_cumulative, 0) + IFNULL(b.fi_finance_revenue_usd_cumulative, 0) as fi_finance_revenue_usd_cumulative,
    IFNULL(a.fi_total_booking_value_usd_cumulative, 0) + IFNULL(b.fi_total_booking_value_usd_cumulative, 0) as fi_total_booking_value_usd_cumulative,
    a.fi_latest,

    a.latest,
    a.processing_date 
    FROM
      (SELECT 
      client_id, 
      created_at,
      site_code,
      device_type, 
      product,
      f_conversions_tracked,
      f_finance_revenue_usd, 
      f_total_booking_value_usd,
      SUM(f_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_conversions_tracked_cumulative,
      SUM(f_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_finance_revenue_usd_cumulative,
      SUM(f_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as f_total_booking_value_usd_cumulative,
      IF(f_created_at = MAX(f_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as f_latest,

      h_conversions_tracked,
      h_finance_revenue_usd, 
      h_total_booking_value_usd,
      SUM(h_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_conversions_tracked_cumulative,
      SUM(h_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_finance_revenue_usd_cumulative,
      SUM(h_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as h_total_booking_value_usd_cumulative,
      IF(h_created_at = MAX(h_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as h_latest,

      fb_conversions_tracked,
      fb_finance_revenue_usd, 
      fb_total_booking_value_usd,
      SUM(fb_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_conversions_tracked_cumulative,
      SUM(fb_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_finance_revenue_usd_cumulative,
      SUM(fb_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fb_total_booking_value_usd_cumulative,
      IF(fb_created_at = MAX(fb_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fb_latest,

      hb_conversions_tracked,
      hb_finance_revenue_usd, 
      hb_total_booking_value_usd,
      SUM(hb_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_conversions_tracked_cumulative,
      SUM(hb_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_finance_revenue_usd_cumulative,
      SUM(hb_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hb_total_booking_value_usd_cumulative,
      IF(hb_created_at = MAX(hb_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as hb_latest,

      fcau_conversions_tracked,
      fcau_finance_revenue_usd, 
      fcau_total_booking_value_usd,
      SUM(fcau_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_conversions_tracked_cumulative,
      SUM(fcau_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_finance_revenue_usd_cumulative,
      SUM(fcau_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fcau_total_booking_value_usd_cumulative,
      IF(fcau_created_at = MAX(fcau_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fcau_latest,

      hcau_conversions_tracked,
      hcau_finance_revenue_usd, 
      hcau_total_booking_value_usd,
      SUM(hcau_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_conversions_tracked_cumulative,
      SUM(hcau_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_finance_revenue_usd_cumulative,
      SUM(hcau_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hcau_total_booking_value_usd_cumulative,
      IF(hcau_created_at = MAX(hcau_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as hcau_latest,

      fi_conversions_tracked,
      fi_finance_revenue_usd, 
      fi_total_booking_value_usd,
      SUM(fi_conversions_tracked) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_conversions_tracked_cumulative,
      SUM(fi_finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_finance_revenue_usd_cumulative,
      SUM(fi_total_booking_value_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as fi_total_booking_value_usd_cumulative,
      IF(fi_created_at = MAX(fi_created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as fi_latest,

      IF(created_at = MAX(created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as latest,
      end_date_processing_block AS processing_date

      FROM
        (SELECT client_id, created_at, site_code, device_type, "Flights Meta" as product,
        conversions_tracked as f_conversions_tracked,
        finance_revenue_usd as f_finance_revenue_usd, 
        IF(conversions_tracked > 0, total_price_usd, NULL) as f_total_booking_value_usd,
        created_at as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,   
        FROM `wego-cloud.wego_analytics.flights_clicks` 
        WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL 

        SELECT client_id, created_at, site_code, device_type, "Hotels Meta" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        conversions_tracked as h_conversions_tracked,
        finance_revenue_usd as h_finance_revenue_usd, 
        IF(conversions_tracked > 0, total_price_usd, NULL) as h_total_booking_value_usd,
        created_at as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,  
        FROM `wego-cloud.wego_analytics.hotels_clicks` 
        WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL

        SELECT client_id, created_at, site_code, device_type, "Flights BoW" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        conversions_tracked as fb_conversions_tracked,
        finance_revenue_usd as fb_finance_revenue_usd, 
        IF(conversions_tracked > 0, total_price_usd, NULL) as fb_total_booking_value_usd,
        created_at as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,  
        FROM `wego-cloud.wego_analytics.flights_bookings` 
        WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL 

        SELECT client_id, created_at, site_code, device_type, "Hotels BoW" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        conversions_tracked as hb_conversions_tracked,
        finance_revenue_usd as hb_finance_revenue_usd, 
        IF(conversions_tracked > 0, wego_total_price_usd, NULL) as hb_total_booking_value_usd,
        created_at as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,  
        FROM `wego-cloud.wego_analytics.hotels_bookings` 
        WHERE DATE(created_at) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL

        SELECT client_id, created_at, site_code, device_type, "Flights Cau" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        conversions_adjusted as fcau_conversions_tracked,
        finance_revenue_usd as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        created_at as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,  
        FROM `wego-cloud.wego_analytics.flights_bookables` 
        WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL 

        SELECT client_id, created_at, site_code, device_type, "Hotels Cau" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        conversions_adjusted as hcau_conversions_tracked,
        finance_revenue_usd as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        created_at as hcau_created_at,
        NULL as fi_conversions_tracked,
        NULL as fi_finance_revenue_usd, 
        NULL as fi_total_booking_value_usd,
        NULL as fi_created_at,  
        FROM `wego-cloud.wego_analytics.hotels_bookables` 
        WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")

        UNION ALL 

        SELECT client_id, created_at, site_code, device_type, "Flights Insurance" as product,
        NULL as f_conversions_tracked,
        NULL as f_finance_revenue_usd, 
        NULL as f_total_booking_value_usd,
        NULL as f_created_at,
        NULL as h_conversions_tracked,
        NULL as h_finance_revenue_usd, 
        NULL as h_total_booking_value_usd,
        NULL as h_created_at,
        NULL as fb_conversions_tracked,
        NULL as fb_finance_revenue_usd, 
        NULL as fb_total_booking_value_usd,
        NULL as fb_created_at, 
        NULL as hb_conversions_tracked,
        NULL as hb_finance_revenue_usd, 
        NULL as hb_total_booking_value_usd,
        NULL as hb_created_at,
        NULL as fcau_conversions_tracked,
        NULL as fcau_finance_revenue_usd, 
        NULL as fcau_total_booking_value_usd,
        NULL as fcau_created_at,
        NULL as hcau_conversions_tracked,
        NULL as hcau_finance_revenue_usd, 
        NULL as hcau_total_booking_value_usd,
        NULL as hcau_created_at,
        conversions_adjusted as fi_conversions_tracked,
        finance_revenue_usd as fi_finance_revenue_usd, 
        IF(conversions_tracked > 0, total_price_usd, NULL) as fi_total_booking_value_usd,
        created_at as fi_created_at,  
        FROM `wego-cloud.wego_analytics.flights_insurance` 
        WHERE DATE(_PARTITIONDATE) BETWEEN start_date_processing_block AND end_date_processing_block
        AND finance_revenue_usd > 0 
        -- AND client_id in (SELECT client_id FROM `wego-cloud.wego_analytics.flights_bookings` GROUP BY 1 HAVING SUM(1) > 1)
        -- AND client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
        AND client_id NOT IN ("00000000-0000-0000-0000-000000000000", "")
          
        )
      ) as a

      LEFT JOIN

      (SELECT *
      FROM `rfm_analysis.summary`
      WHERE latest = 1) as b
      ON a.client_id = b.client_id)
  );
  
  SET start_date_processing_block = DATE_ADD(end_date_processing_block, INTERVAL 1 DAY);

END LOOP;


-- SELECT * EXCEPT (report) 
-- FROM 
--   (SELECT *, IF(created_at = MAX(created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),1,0) as report 
--   FROM `rfm_analysis.summary`
--   WHERE DATE(created_at) <= "2022-12-01" 
--   AND client_id = "403e38ab-facb-4149-9fd6-cdeb464b1b41"
--   )
-- WHERE report = 1
-- ORDER BY created_at

-------- ALTERNATIVE OPTION --------
-- INSERT INTO `rfm_analysis.summary` (client_id, created_at, finance_revenue_usd, finance_revenue_usd_cumulative, latest)
-- SELECT a.client_id, a.created_at, a.finance_revenue_usd,
-- IFNULL(a.finance_revenue_usd_cumulative,0) + b.finance_revenue_usd_cumulative as finance_revenue_usd_cumulative, a.latest 
-- FROM
--   (SELECT client_id, created_at, finance_revenue_usd, 
--   SUM(finance_revenue_usd) OVER (PARTITION BY client_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as finance_revenue_usd_cumulative,
--   IF(created_at = MAX(created_at) OVER (PARTITION BY client_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),-1,0) as latest
--   FROM `wego-cloud.wego_analytics.flights_bookings`
--   WHERE DATE(created_at) BETWEEN "2022-09-02" AND "2023-05-01"
--   AND client_id = "00002CC2-6CDC-4E53-84EF-EA1227F26B91") as a

--   LEFT JOIN

--   (SELECT *
--   FROM `rfm_analysis.summary`
--   WHERE latest = 1) as b
--   ON a.client_id = b.client_id;


-- UPDATE `rfm_analysis.summary`
-- SET latest = 0
-- WHERE client_id in (SELECT DISTINCT client_id 
-- FROM `wego-cloud.wego_analytics.flights_bookings` 
-- WHERE DATE(created_at) BETWEEN "2022-09-01" AND "2023-05-01" AND latest = 1);


-- UPDATE `rfm_analysis.summary`
-- SET latest = 1 WHERE latest = -1;

-------- INVESTIGATIONS --------
-- SELECT DISTINCT client_id FROM `wego-cloud.wego_analytics.flights_bookings` WHERE DATE(created_at) BETWEEN start_date_incremental AND end_date_incremental AND client_id in (SELECT distinct client_id from rfm_analysis.summary)

-- SELECT *
-- FROM `rfm_analysis.summary`
-- WHERE  client_id = "1D91F3EA-95EC-4002-96B8-791EEB97CC2E"
{% endraw %}
