{{ config(materialized='ephemeral') }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model.
-- Ephemeral: dbt and Cosmos never run or bill this; it exists so the full
-- dependency DAG is visible in dbt docs. Do NOT ref() it from a real model
-- until it is properly migrated.
-- Query      : provider_code_domain_mapping
-- Destination: analysis.provider_domain_code_mapping  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, set materialized, add the parity test.
-- Edges below are declared from the parsed legacy SQL; the raw block keeps
-- jinja from interpreting any braces in it.
-- depends_on: {{ source('services_tpa', 'provider_domains') }}
{% raw %}
SELECT
  p.partner_id,
  p.domain  AS parent_domain,
  c.domain  AS child_domain
FROM `services_tpa.provider_domains` AS p
LEFT JOIN `services_tpa.provider_domains` AS c
  ON  c.partner_id = p.partner_id
  AND c.parent = FALSE
  AND c.deleted_at IS NULL
WHERE p.parent = TRUE
  AND p.deleted_at IS NULL
ORDER BY p.partner_id, c.domain;
{% endraw %}
