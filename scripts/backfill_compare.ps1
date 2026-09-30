# Backfill any day-partitioned model for the last N days, then run ALL its
# tests (yml tests + any parity test that ref()s it — dbt selects tests
# attached to the selected model automatically, so no test names in here).
#
# Works for every model that follows the migration conventions:
#   * incremental by day partition
#   * date-parameterized via --vars run_date (defaults to yesterday)
#   * parity test in tests/ referencing the model via ref()
#
# Usage:
#   .\scripts\backfill_compare.ps1 -Model autopricing_ab_test              # 7 days ending yesterday
#   .\scripts\backfill_compare.ps1 -Model autopricing_ab_test -Days 3
#   .\scripts\backfill_compare.ps1 -Model some_model -Days 7 -EndDate 2026-09-20   # historic window
#
# Interim tooling: once a model's DAG is scheduled on Composer, backfills move
# to Airflow (`airflow dags backfill`) and this script retires for that model.

param(
    [Parameter(Mandatory = $true)][string]$Model,
    [int]$Days = 7,
    [string]$EndDate   # yyyy-MM-dd; default: yesterday
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)   # repo root, wherever called from

$end = if ($EndDate) { [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null) }
       else          { (Get-Date).AddDays(-1) }

for ($i = $Days - 1; $i -ge 0; $i--) {
    $d = $end.AddDays(-$i).ToString("yyyy-MM-dd")
    Write-Host "=== building $Model for $d ===" -ForegroundColor Cyan
    uv run dbt run --select $Model --vars "{run_date: '$d'}" --profiles-dir .
    if ($LASTEXITCODE -ne 0) { throw "build failed for $Model on $d" }
}

Write-Host "=== testing $Model (parity window: $Days days) ===" -ForegroundColor Cyan
uv run dbt test --select $Model --vars "{compare_days: $Days}" --profiles-dir .
if ($LASTEXITCODE -ne 0) { throw "tests failed for $Model" }

Write-Host "=== $Model : backfill + tests green ===" -ForegroundColor Green
