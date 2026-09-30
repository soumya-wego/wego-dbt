# Backfill the dbt model for the last N days, then run the parity test vs legacy.
# Usage:  .\backfill_compare.ps1            (7 days)
#         .\backfill_compare.ps1 -Days 3
param([int]$Days = 7)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

for ($i = $Days; $i -ge 1; $i--) {
    $d = (Get-Date).AddDays(-$i).ToString("yyyy-MM-dd")
    Write-Host "=== building $d ===" -ForegroundColor Cyan
    uv run dbt run --select autopricing_ab_test --vars "{run_date: '$d'}" --profiles-dir .
    if ($LASTEXITCODE -ne 0) { throw "build failed for $d" }
}

Write-Host "=== running parity test (last $Days days) ===" -ForegroundColor Cyan
uv run dbt test --select assert_autopricing_matches_legacy --vars "{compare_days: $Days}" --profiles-dir .
