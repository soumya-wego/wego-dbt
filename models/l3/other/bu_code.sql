{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : ota_data_upload_template_working_30Oct24
-- Destination: ota_data_upload_template_working_30oct24.bu_code  (unchanged)
-- Schedule   : every day 16:15   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('netsuite_metrics_v1', 'bu_code') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'chart_of_accounts') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'currency_exchanges_rates') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'departments') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'market_segment') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'products') }}
-- depends_on: {{ source('netsuite_metrics_v1', 'subsidiary') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'bucode') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'chartofaccounts') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'customer') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'department') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'entitylookup') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'marketsegment') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'product') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'subsidiary') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'taxcodes') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'vccprovider') }}
-- depends_on: {{ source('ota_data_upload_template_working_30oct24', 'vendors') }}
{% raw %}
CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.bu_code` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.BUCode` -- Ready to migrate
  SELECT
    CAST(Internal_ID AS STRING) AS InternalID,
    BU_Code_Name AS BUCodeName
  FROM
    `wego-cloud.netsuite_metrics_v1.bu_code`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.chart_of_accounts` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.ChartofAccounts` -- Ready to migrate
  SELECT
    * EXCEPT(updated_at)
  FROM
    `wego-cloud.netsuite_metrics_v1.chart_of_accounts`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.customers` AS (
  SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.Customer`
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.departments` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.Department` -- Ready to migrate
  
  -- SELECT
  --   * EXCEPT(updated_at)
  -- FROM
  --   `wego-cloud.netsuite_metrics_v1.departments`
  -- QUALIFY
  --   ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1

  -- Departments not in the map (e.g. new ones) fall back to the NetSuite name.
  WITH legacy_names AS (
    SELECT * FROM UNNEST([
      STRUCT(2 AS Internal_ID, 'OH : Human Resource' AS Name),
      (4, 'OH : Management (CEO Office)'),
      (5, 'COD : Software Engineering'),
      (6, 'COS : Sales/Commercial/Customer Success'),
      (8, 'COS : Media Solutions'),
      (9, 'COD : Customer Service'),
      (10, 'COD : Product Management'),
      (11, 'COD : Data Engineering and Analytics'),
      (18, 'COS : Marketing-Brand'),
      (21, 'COS : Marketing-Performance'),
      (22, 'COD : Design Marketing'),
      (23, 'COD : Design UX'),
      (24, 'COD : System Operations'),
      (25, 'OH : Corporate Services'),
      (26, 'OH : Finance'),
      (27, 'OH : Admin & IT Support (Office Infra/General)'),
      (28, 'OH : Common')
    ])
  )
  SELECT
    CAST(d.Internal_ID AS STRING) AS InternalID,
    COALESCE(l.Name, d.Departments_Name) AS Name
  FROM
    `wego-cloud.netsuite_metrics_v1.departments` d
  LEFT JOIN
    legacy_names l
    USING (Internal_ID)
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY d.Internal_ID ORDER BY d.updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.entity_lookup` AS (
  SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.EntityLookup`
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.market_segment` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.MarketSegment` -- Ready to migrate
  SELECT
    CAST(Internal_ID AS STRING) AS InternalID,
    Market_Segment_Name AS MarketSegmentName
  FROM
    `wego-cloud.netsuite_metrics_v1.market_segment`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.products` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.Product` -- Ready to migrate
  SELECT
    CAST(Internal_ID AS STRING) AS InternalID,
    Products_Name AS ProductName
  FROM
    `wego-cloud.netsuite_metrics_v1.products`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.subsidiaries` AS (
  -- SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.Subsidiary` -- Ready to migrate
  SELECT
    * EXCEPT(updated_at)
  FROM
    `wego-cloud.netsuite_metrics_v1.subsidiary`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY Internal_ID ORDER BY updated_at DESC) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.currency_exchange_rates` AS (
  SELECT
    *
  FROM
    `wego-cloud.netsuite_metrics_v1.currency_exchanges_rates`
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        Base_Currency,
        Source_Currency,
        DATE(Effective_Date)
      ORDER BY
        Rate_Updated_Date DESC,
        Exchange_Rate DESC
    ) = 1
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.tax_codes` AS (
  SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.TaxCodes`
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.vendor` AS (
  SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.Vendors`
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.vcc_provider` AS (
  SELECT * FROM `wego-cloud.ota_data_upload_template_working_30Oct24.VccProvider`
);

CREATE OR REPLACE TABLE `wego-cloud.ota_data_upload_template_working_30Oct24.wegopro_entity_lookup` AS (
  SELECT
    string_field_0 AS EntityRef,
    string_field_1 AS SubsidiaryName,
    int64_field_2 AS SubsidiaryInternalID,
    string_field_3 AS SubsidiaryFunctionalCurrency,
    CAST(int64_field_4 AS STRING) AS COA_Number,
    CAST(int64_field_5 AS STRING) AS COA_InternalID
  FROM
    wego-cloud.`ota_data_upload_template_working_30Oct24.WegoProEntityLookup`
);
{% endraw %}
