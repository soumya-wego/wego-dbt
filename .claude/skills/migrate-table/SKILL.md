---
name: migrate-table
description: >
  Generate (or REGENERATE, overwriting the existing file) the parity test for a
  migrated dbt model: fetch the legacy BigQuery table's columns via
  INFORMATION_SCHEMA, ask the user for the comparison window (days or an
  inclusive date range) and the % tolerance, classify each column into a
  fingerprint (sum / count distinct / null-coverage), and write
  tests/parity__<model>.sql with the user's window and tolerance baked in as
  defaults — so a plain `uv run dbt test --select <model>` runs their gate.
  Use whenever the user says /migrate-table, "generate the parity test",
  "regenerate the test for <model>", or migrates a scheduled query to dbt.
---

# /migrate-table — parity test generator

Usage: `/migrate-table <model_name> <legacy_project.dataset.table> <date_column>`
Ask for any missing argument. The command only CREATES THE TEST FILE — the user
runs the test themselves afterwards. Do not run dbt test, backfills, or git
operations as part of this command.

## Steps

### 1. Fetch the legacy table's columns (free, metadata-only)

```
bq query --use_legacy_sql=false --format=csv "select column_name, data_type from <dataset>.INFORMATION_SCHEMA.COLUMNS where table_name = '<table>' order by ordinal_position"
```

The legacy table is the schema contract — the model is supposed to match it,
so classify from the legacy side alone (the model's table may not even be
built yet, which is fine). The legacy table must be declared as a source in
`models/sources.yml`; add it if missing — never hardcode table names in tests.

### 2. Ask the user two questions

- **Window**: a number of complete days, or an inclusive date range
  (start and end dates).
- **% tolerance**: suggest 1% for backfill comparisons (recomputed history
  sees late-arriving data that legacy's frozen snapshots missed — validated
  on the pilot: ~+0.3% rows, new side higher, while the same-day-computed day
  matched exactly). Tighter catches more; looser tolerates snapshot noise.
  Convert % to a fraction (1% -> 0.01).

### 3. Classify each column into a fingerprint

Apply the heuristics from the global `bq-parity-test` skill
(`~/.claude/skills/bq-parity-test/references/column-heuristics.md`). Summary:
- the date column -> the `group by` grain, not a metric
- id-like (`*_id`, `*_code`, `*_key`) -> `count(distinct x)`, even when numeric
- money/measures (FLOAT64/INT64 with fare/price/amount/margin/_usd/count-ish
  names) -> `round(sum(x), 2)`, compared with the relative tolerance
- join-produced nullables (e.g. `variant`) -> `countif(x is not null)`
- low-cardinality dimensions (site_code, trip_type) -> `count(distinct x)`
- free text / high-cardinality noise (trip_* blobs, names, urls) -> SKIP,
  listed in the file's header comment
Budget: 6-12 fingerprints — row_count + the key measures + main join keys +
coverage checks. Present the classification table (column -> fingerprint ->
reason) and let the user trim before writing.

### 4. Write the test file — REPLACING any existing one

Write `tests/parity__<model_name>.sql`, overwriting the current file if it
exists (regeneration is the point of re-running the command; the file is
git-tracked, so the overwrite shows up as a reviewable diff — nothing is
silently lost). Follow the structure of the pilot's validated template
(`~/.claude/skills/bq-parity-test/assets/parity_test_template.sql`):
- two identically-aggregated CTEs (model via `ref()`, legacy via `source()`),
  grouped by the date column
- the window clause from the user's answer: `>= date_sub(current_date,
  interval N day)` for days, or `between '<start>' and '<end>'` for a range —
  as the DEFAULT of a `compare_days`/`compare_start`/`compare_end` var
- ALL comparisons relative, sharing one `tolerance` var whose DEFAULT is the
  user's chosen fraction
- full outer join on the grain; violations-only select; each row carries a
  `failed_check` label and both sides' values

Because window and tolerance are baked in as var defaults, the user's gate is
simply:

```
uv run dbt test --select <model_name> --profiles-dir .
```

(overridable per-run with `--vars` without editing the file).

### 5. Stop

Report: the file path, the fingerprints chosen, the skipped columns, and the
one-line test command above. The user takes it from there — running the gate,
judging the result, and committing the file through a branch + PR.
