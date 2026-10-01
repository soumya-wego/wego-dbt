{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : KSA_EG_VAT_Reporting
-- Destination: aaaaa_temporary_export_folder.ksa_eg_vat_reporting  (unchanged)
-- Schedule   : 1,7,14,21,28 of month 01:00   State: FAILED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('payments', 'tax_vendor_details') }}
-- depends_on: {{ source('payments', 'taxes') }}
{% raw %}
CREATE OR REPLACE TABLE `wego-cloud.aaaaa_temporary_export_folder.KSA_EG_VAT_Reporting` as (
	/*Docs: https://docs.google.com/spreadsheets/d/1O31Ne908FkhW-cZJla7FOGhtdCvXHA_l/edit?gid=581413784#gid=581413784*/
	WITH taxes_source AS (
	  SELECT
	    id,
	    site_code,
	    user_country_code,
	    document_date,
	    created_at,
	    updated_at,
	    deleted_at,
	    tax_ref,
	    booking_ref,
	    invoice_ref,
	    status,
	    document_type,
	    user_name,
	    original_currency_code,
	    original_amount,
	    tax_currency_code,
	    tax_base_amount,
	    tax_amount,
	    tax_total_amount,
	    tax_rate,
	    tax_code,
	    conversion_rate,
	    invoice_currency_code,
	    invoice_base_amount,
	    invoice_discount_total_amount,
	    invoice_taxable_amount_excl_vat,
	    invoice_vat_amount,
	    invoice_total_amount,
	    invoice_tax_rate,
	    invoice_gov_tax_currency_code,
	    invoice_gov_tax_amount,
	    invoice_gross_total_incl_vat,
	    invoice_document_level_charge,
	    invoice_prepaid_amount,
	    invoice_xml_file_s3_key,
	    invoice_pdf_file_s3_key,
	    client_id,
	    credit_note_billing_ref,
	    credit_note_reason,
	    tax_type,
	    retry_count,
	    exchange_rates,
	    delivery_date,
	    original_country_code,
	    destination_country_code,
	    credit_note_tax_ref,
	    error_detail,
	    CAST(NULL AS NUMERIC) AS vendor_cost,
	    CAST(NULL AS STRING) AS vendor_currency_code,
	    CAST(NULL AS STRING) AS vendor_tax_code,
	    CAST(NULL AS STRING) AS invoice_desc,
      invoice_issued_date,
	    1 AS _in_group_order,
	    IF(document_type = 'Credit Note', -1, 1) AS _is_credit_note
	  FROM `payments.taxes`
	  WHERE deleted_at IS NULL
	  ORDER BY document_date DESC
	)
	, tax_vendor_details AS (
	  SELECT
	    taxes_source.id,
	    taxes_source.site_code,
	    taxes_source.user_country_code,
	    taxes_source.document_date,
	    taxes_source.created_at,
	    taxes_source.updated_at,
	    taxes_source.deleted_at,
	    taxes_source.tax_ref,
	    taxes_source.booking_ref,
	    taxes_source.invoice_ref,
	    taxes_source.status,
	    taxes_source.document_type,
	    taxes_source.user_name,
	    CAST(NULL AS STRING) AS original_currency_code, --Blank
	    CAST(NULL AS NUMERIC) AS original_amount, --Blank
	    CAST(NULL AS STRING) AS tax_currency_code, --Blank
	    CAST(NULL AS NUMERIC) AS tax_base_amount, --Blank
	    CAST(NULL AS NUMERIC) AS tax_amount, --Blank
	    CAST(NULL AS NUMERIC) AS tax_total_amount, --Blank
	    CAST(NULL AS NUMERIC) AS tax_rate, --Blank
	    CAST(NULL AS STRING) AS tax_code, --Blank
	    taxes_source.conversion_rate,
	    tax_vendor_details.invoice_currency_code,
	    tax_vendor_details.invoice_base_amount,
	    tax_vendor_details.invoice_discount_total_amount,
	    tax_vendor_details.invoice_taxable_amount_excl_vat,
	    tax_vendor_details.invoice_vat_amount,
	    tax_vendor_details.invoice_total_amount,
	    tax_vendor_details.invoice_tax_rate,
	    tax_vendor_details.invoice_gov_tax_currency_code,
	    tax_vendor_details.invoice_gov_tax_amount,
	    tax_vendor_details.invoice_gross_total_incl_vat,
	    CAST(NULL AS NUMERIC) AS invoice_document_level_charge, --Blank
	    CAST(NULL AS NUMERIC) AS invoice_prepaid_amount, --Blank
	    CAST(NULL AS STRING) AS invoice_xml_file_s3_key, --Blank
	    CAST(NULL AS STRING) AS invoice_pdf_file_s3_key, --Blank
	    CAST(NULL AS INT64) AS client_id, --Blank
	    taxes_source.credit_note_billing_ref,
	    taxes_source.credit_note_reason,
	    taxes_source.tax_type,
	    taxes_source.retry_count,
	    taxes_source.exchange_rates,
	    taxes_source.delivery_date,
	    taxes_source.original_country_code,
	    taxes_source.destination_country_code,
	    taxes_source.credit_note_tax_ref,
	    taxes_source.error_detail,
	    tax_vendor_details.vendor_total_amount AS vendor_cost,
	    tax_vendor_details.vendor_currency_code,
	    tax_vendor_details.vendor_tax_code,
	    tax_vendor_details.invoice_desc,
      taxes_source.invoice_issued_date,
	    2 AS _in_group_order,
	    IF(document_type = 'Credit Note', -1, 1) AS _is_credit_note
	  FROM taxes_source
	  LEFT JOIN `payments.tax_vendor_details` AS tax_vendor_details
	  ON taxes_source.id = tax_vendor_details.tax_id
	  WHERE tax_vendor_details.deleted_at IS NULL
	)
	, prefinal AS (
	  SELECT id,site_code,user_country_code,document_date,created_at,updated_at,deleted_at,tax_ref,booking_ref,invoice_ref,
	         status,document_type,user_name,original_currency_code,original_amount,tax_currency_code,tax_base_amount,tax_amount,
	         tax_total_amount,tax_rate,tax_code,invoice_currency_code,invoice_base_amount,invoice_discount_total_amount,
	         invoice_taxable_amount_excl_vat,invoice_vat_amount,invoice_total_amount,invoice_tax_rate,invoice_gov_tax_currency_code,
	         invoice_gov_tax_amount,invoice_gross_total_incl_vat,invoice_document_level_charge,invoice_prepaid_amount,invoice_xml_file_s3_key,
	         invoice_pdf_file_s3_key,client_id,credit_note_billing_ref,credit_note_reason,tax_type,retry_count,exchange_rates,delivery_date,
	         original_country_code,destination_country_code,credit_note_tax_ref,error_detail,vendor_cost,vendor_currency_code,vendor_tax_code,
	         invoice_desc,invoice_issued_date,_in_group_order,_is_credit_note,
	         IF(site_code = 'SA' AND original_amount <= 0, 1, conversion_rate) AS conversion_rate, /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754987206551759?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
	         IF(site_code = 'SA' AND invoice_taxable_amount_excl_vat = 5.22 AND invoice_vat_amount = 0.78 AND invoice_total_amount = 6, TRUE, FALSE) AS _using_market_value
	  FROM taxes_source
	  UNION ALL
	  SELECT id,site_code,user_country_code,document_date,created_at,updated_at,deleted_at,tax_ref,booking_ref,invoice_ref,
	         status,document_type,user_name,original_currency_code,original_amount,tax_currency_code,tax_base_amount,tax_amount,
	         tax_total_amount,tax_rate,tax_code,invoice_currency_code,invoice_base_amount,invoice_discount_total_amount,
	         invoice_taxable_amount_excl_vat,invoice_vat_amount,invoice_total_amount,invoice_tax_rate,invoice_gov_tax_currency_code,
	         invoice_gov_tax_amount,invoice_gross_total_incl_vat,invoice_document_level_charge,invoice_prepaid_amount,invoice_xml_file_s3_key,
	         invoice_pdf_file_s3_key,client_id,credit_note_billing_ref,credit_note_reason,tax_type,retry_count,exchange_rates,delivery_date,
	         original_country_code,destination_country_code,credit_note_tax_ref,error_detail,vendor_cost,vendor_currency_code,vendor_tax_code,
	         invoice_desc,invoice_issued_date,_in_group_order,_is_credit_note,
	         IF(site_code = 'SA' AND original_amount <= 0, 1, conversion_rate) AS conversion_rate, /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754987206551759?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
	         IF(site_code = 'SA' AND invoice_taxable_amount_excl_vat = 5.22 AND invoice_vat_amount = 0.78 AND invoice_total_amount = 6, TRUE, FALSE) AS _using_market_value
	  FROM tax_vendor_details
	)
	, final AS (
		/*Docs: https://docs.google.com/spreadsheets/d/1JTkumvW4cRvN4yDaF62Gi3wDBE0wcTVBnhHK--cNUbM/edit?gid=268234455#gid=268234455*/
		SELECT
			site_code AS `Site Code`,
			user_country_code AS `User Country Code`,
			status AS `Status`,
			document_type AS `Document Type`,
			document_date AS `Transaction Date`,
			invoice_issued_date AS `Reporting Date`,
			booking_ref AS `Booking Reference`,
			CASE
			  WHEN site_code = 'SA' THEN invoice_ref
			  WHEN site_code = 'EG' THEN tax_ref
			  ELSE NULL
			END AS `Transaction No`,
			user_name AS `Customer Name`,
			CASE
				WHEN tax_code IS NULL AND vendor_tax_code IS NOT NULL THEN vendor_tax_code
				WHEN tax_code IS NOT NULL AND vendor_tax_code IS NULL THEN tax_code
				WHEN tax_code IS NOT NULL AND vendor_tax_code IS NOT NULL THEN tax_code || ' & ' || vendor_tax_code
				ELSE ''
			END AS `Tax Code`,
			invoice_tax_rate AS `Invoice Tax Rate`,
			CASE
				WHEN _in_group_order = 1 THEN invoice_currency_code
				WHEN _in_group_order = 2 THEN vendor_currency_code
				ELSE ''
			END AS `Invoice Currency`,
			invoice_base_amount * _is_credit_note AS `Invoice Base Amount`, /*Ref: https://docs.google.com/spreadsheets/d/1JTkumvW4cRvN4yDaF62Gi3wDBE0wcTVBnhHK--cNUbM/edit?gid=268234455#gid=268234455&range=E15*/
			invoice_discount_total_amount AS `Invoice Discount`,
			invoice_taxable_amount_excl_vat * _is_credit_note AS `Invoice Amount excl VAT`,
			invoice_vat_amount * _is_credit_note AS `Invoice VAT Amount`,
			invoice_total_amount * _is_credit_note AS `Invoice Total Amount`,
			CASE
				WHEN site_code = 'EG' THEN 1
        WHEN site_code = 'SA' AND tax_total_amount = 6 AND tax_currency_code = 'SAR' THEN 1 /*Slack Ref: https://wego.slack.com/archives/C09AHGY5WJV/p1766565741833459?thread_ts=1766466839.806709&cid=C09AHGY5WJV*/
				ELSE conversion_rate
			END AS `Tax FX Rate`, /*Slack Ref: https://wego.slack.com/archives/C09A46W6ZN1/p1757344135046439?thread_ts=1756951673.890049&cid=C09A46W6ZN1*/
			invoice_gov_tax_currency_code AS `Local Currency`,
			CASE
				WHEN site_code = 'SA' THEN invoice_taxable_amount_excl_vat * conversion_rate * _is_credit_note /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1753964974706019?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
				WHEN site_code = 'EG' THEN invoice_taxable_amount_excl_vat * _is_credit_note /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754296648208019?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
				ELSE NULL
			END AS `Local Amount Before Tax`, /*Ref: https://docs.google.com/spreadsheets/d/1JTkumvW4cRvN4yDaF62Gi3wDBE0wcTVBnhHK--cNUbM/edit?gid=268234455#gid=268234455&range=E22*/
			invoice_gov_tax_amount * _is_credit_note AS `Local Tax Amount`,
			CASE
				WHEN site_code = 'SA' THEN invoice_total_amount * conversion_rate * _is_credit_note
				ELSE invoice_gross_total_incl_vat * _is_credit_note
			END AS `Local Total Amount After Tax`,
			invoice_desc AS `Description`,
			CASE
			  WHEN site_code = 'SA' THEN credit_note_billing_ref
			  WHEN site_code = 'EG' THEN credit_note_tax_ref
			  ELSE NULL
			END AS `Credit Note Reference`,
			CASE
			  WHEN site_code = 'SA' THEN credit_note_reason
			  WHEN site_code = 'EG' THEN error_detail
			  ELSE NULL
			END AS `Credit Note Reason`,
			id,
			_in_group_order,
			_using_market_value
		FROM prefinal
	)
	SELECT
		`Site Code`,
		`User Country Code`,
		`Status`,
		`Document Type`,
		`Transaction Date`,
		`Reporting Date`,
		`Booking Reference`,
		`Transaction No`,
		`Customer Name`,
		`Tax Code`,
		`Invoice Tax Rate`,
		`Invoice Currency`,
		`Invoice Base Amount`,
		`Invoice Discount`,
		CASE
			WHEN _using_market_value THEN `Local Amount Before Tax` /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754458996175929?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
			ELSE `Invoice Amount excl VAT`
		END AS `Invoice Amount excl VAT`,
		CASE
			WHEN _using_market_value THEN `Local Tax Amount` /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754458996175929?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
			ELSE `Invoice VAT Amount`
		END AS `Invoice VAT Amount`,
		CASE
			WHEN _using_market_value THEN `Local Total Amount After Tax` /*Slack Ref: https://wego.slack.com/archives/C04U4KATYUV/p1754458996175929?thread_ts=1751531863.203429&cid=C04U4KATYUV*/
			ELSE `Invoice Total Amount`
		END AS `Invoice Total Amount`,
		`Tax FX Rate`,
		`Local Currency`,
		`Local Amount Before Tax`,
		`Local Tax Amount`,
		`Local Total Amount After Tax`,
		`Description`,
		`Credit Note Reference`,
		`Credit Note Reason`,
	FROM final
	ORDER BY id DESC, _in_group_order ASC
)
{% endraw %}
