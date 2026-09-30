---
name: migrate-table
description: >
  One command per migrated table: given a dbt model, its legacy BigQuery table
  and the date column, generate the named parity test (tests/parity__<model>.sql),
  price the backfill with a dry run, run the 7-day backfill + full test gate via
  scripts/backfill_compare.ps1, and report whether the model is PR-ready with
  evidence to paste. Use whenever the user says /migrate-table, "onboard this
  model", "run the migration steps for <model>", or has just written a migrated
  model and wants the backfill/parity gate executed.
---

# /migrate-table — per-table migration runner

Usage: `/migrate-table <model_name> <legacy_project.dataset.table> <date_column> [days]`
(days defaults to 7). Ask for any missing argument before starting.

This command exists because the steps below are identical for every one of the
~96 tables in the scheduled-queries migration, and forgetting one (usually the
dry run) costs real money. Run them in order; stop at any red gate.

## Steps

### 1. Preflight (free)
- The model file exists in `models/` and compiles: `uv run dbt parse --profiles-dir .`
- The legacy table is declared in `models/sources.yml` — if not, add it (never
  hardcode table names in tests).
- The model is incremental by day partition and honors `--vars run_date`
  (grep for `var("run_date"` / `var('run_date'`). If it doesn't, stop: the
  model isn't migration-conventional yet — fix that first.
- If the destination table ALREADY exists (earlier dev runs), verify it is
  actually partitioned — incremental models never retrofit config changes, so
  a table created before `partition_by` was added stays unpartitioned and
  breaks insert_overwrite silently:
  `select partition_id, total_rows from <dataset>.INFORMATION_SCHEMA.PARTITIONS where table_name = '<model>'`
  One NULL partition holding everything = unpartitioned → the first backfill
  day must run with `--full-refresh` (drops and recreates; same cost as a
  normal day-run; get user approval since it discards the existing rows).

### 2. Generate the named parity test (free)
Invoke the `bq-parity-test` skill with the model, legacy table, and date
column. It introspects INFORMATION_SCHEMA on both sides, reports the schema
diff, proposes per-column fingerprints, and writes
`tests/parity__<model_name>.sql`. Present its classification table and let the
user trim before continuing. If a parity test for this model already exists,
skip generation and say so.

### 3. Price the backfill (free)
Dry-run one representative day and report the bill before spending it:

```
uv run dbt run --select <model> --vars "{run_date: '<recent date>'}" --profiles-dir . --empty
```

If `--empty` is not supported by the installed dbt version, compile the model
and dry-run the compiled SQL via `bq query --dry_run`. Report:
`<GB> scanned/day x <days> days = ~$<total> (at $6.25/TB)`. Also check which
of the target days already exist in the destination table
(`select distinct <date_column> ... where <date_column> >= ...` — metadata-cheap)
and subtract them. **Wait for explicit user approval of the cost.**

### 4. TRIAL PARITY — compare before materializing (the cheap gate)
Ask the user for the window: a number of complete days, or an inclusive date
range. Then compare the MODEL'S QUERY directly against legacy in one
aggregate query — no table is built, and the model's fixed upstream scans
are paid once instead of once per day (~5x cheaper than build-then-test,
and each fix-retry iteration stays cheap):

1. Compile the model for the range:
   `uv run dbt compile --select <model> --vars "{run_date_start: '<start>', run_date_end: '<end>'}" --profiles-dir .`
2. Build a trial query in the scratchpad (never committed): take the
   compiled SELECT from `target/compiled/.../<model>.sql` as a CTE named
   `new_side_raw`, then reuse the fingerprint structure from
   `tests/parity__<model>.sql` — aggregate new_side_raw and the legacy table
   per day over the same range, full outer join, violations-only select,
   tolerance 0.01 (backfill semantics: recomputed history sees late-arriving
   data legacy's frozen snapshots missed).
3. Dry-run the trial query, report the price, get approval, run it via bq.
4. Zero rows → parity holds; show the summary. Violation rows → diagnose
   (see step 6's signatures), fix the model, re-trial — iterations cost ~one
   fixed scan each, so loop freely BEFORE spending on materialization.

### 5. Materialize + full gate (the paid step, only after trial is green)
```
.\scripts\backfill_compare.ps1 -Model <model_name> -Days <days>
```
Builds one day per run (oldest first, stops on first failure), then runs ALL
the model's tests — the committed parity test is selected automatically
because it ref()s the model. This confirms the materialized table (incremental
config, partitioning, insert_overwrite) behaves like the trial query did.
(Cheaper variant for wide windows: one range run —
`dbt run --select <model> --vars "{run_date_start: ..., run_date_end: ...}"` —
insert_overwrite replaces all partitions in the result in a single pass;
verify partition counts afterwards.)

### 6. Report the gate
- **Green**: print the PASS summary and the PR checklist — branch, commit the
  model + `tests/parity__<model>.sql` together, paste the test output into the
  PR description, request review.
- **Red**: show the failing day/metric rows (suggest `--store-failures` for a
  queryable audit table) and diagnose BEFORE touching anything. Known
  signature — late-arriving data (expected on every backfill): new side
  uniformly ~0.1–1% HIGHER on counts/sums, gap similar across old days, and
  the most recent day (computed the same morning as legacy) PASSING. That is
  snapshot timing, not a model bug: backfill mode already runs at 1% tolerance
  (script default); same-day nightly runs stay strict. Any other pattern —
  new side LOWER, huge gaps, missing days, variant coverage broken — is a real
  model bug: fix the model, re-run only the broken day
  (`-Days 1 -EndDate <that day>`), re-test. Never widen tolerance to make red
  green without the user saying so explicitly.

## Cost discipline (why the order is what it is)
Steps 1–3 are free; step 4 is the only one that scans. The dry run comes
before the loop because a mis-parameterized model that full-scans on every
"daily" run turns a $3 backfill into a $200 one. Per the house rule: always
dry run before executing.
