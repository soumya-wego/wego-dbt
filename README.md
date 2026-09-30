# wego-dbt

dbt project for the Wego scheduled-queries → dbt migration.
Pilot model: `autopricing_ab_test` (replaces the legacy scheduled query writing
`analysis.autopricing_ab_test`).

## Local setup (once)

```bash
uv sync                                   # exact dbt version from uv.lock
gcloud auth application-default login     # your own BigQuery access — no keys in this repo
```

## Daily workflow

```bash
uv run dbt run  --select <model> --profiles-dir .           # build yesterday
uv run dbt run  --select <model> --vars "{run_date: '2026-09-23'}" --profiles-dir .   # build one day
uv run dbt test --select <model> --profiles-dir .           # run its tests (incl. parity vs legacy)
.\scripts\backfill_compare.ps1 -Model <model> -Days 7       # backfill a week + full test gate
```

## Migrating a new table

One command drives the whole per-table sequence (generate named parity test,
dry-run the cost, backfill, gate): in Claude Code, run

```
/migrate-table <model_name> <legacy_project.dataset.table> <date_column>
```

Conventions every migrated model follows:
- incremental by day partition, date-parameterized via `--vars run_date`
  (defaults to yesterday)
- legacy table declared in `models/sources.yml`
- parity test named `tests/parity__<model_name>.sql`, referencing the model
  via `ref()` so `dbt test --select <model>` picks it up automatically

Models write to `wego-cloud.dbt_learning` (dev target). Always dry-run new
models — one full run of the pilot model scans ~560 GB (~$3.50).

## How changes ship

1. Branch → PR. CI runs `dbt parse` + DAG syntax check (free, no BigQuery).
2. Paste the parity test PASS output into the PR (gate: green on 7 days).
3. Merge to `main` → deploy the project to the Composer DAGs bucket
   (manual `gsutil rsync` during the pilot; CI later).
4. Cosmos re-renders the `wego_dbt` DAG from the project on the next parse —
   no Airflow code changes, ever. See `dags/wego_dbt_cosmos.py`.

## Layout

```
models/    the migrated models + sources + column tests
tests/     singular tests (parity vs legacy tables)
dags/      the ONE Cosmos DAG file (env-aware: local Docker + Composer)
```
