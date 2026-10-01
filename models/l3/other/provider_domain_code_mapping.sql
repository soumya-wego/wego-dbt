{{ config(enabled=false) }}
-- LEGACY SCHEDULED QUERY -- verbatim SQL, NOT yet a dbt model. Do not enable.
-- Query      : provider_code_domain_mapping
-- Destination: analysis.provider_domain_code_mapping  (unchanged)
-- Schedule   : every 24 hours   State: SUCCEEDED
-- Migrate via /migrate-table: rewrite reads as source()/ref(), make it
-- incremental+date-parameterized, remove enabled=false, add the parity test.
-- The raw block below keeps jinja from interpreting any braces in legacy SQL.
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
