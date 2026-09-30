-- PARITY TEST: dbt model vs legacy production table, last N days.
-- Zero rows returned = pass. Any row returned = the test FAILS and the row
-- tells you which day broke and on which metric.
--
-- ALL checks are RELATIVE with a shared tolerance, because exact parity vs
-- legacy is only possible when both pipelines computed the same day on the
-- same morning. Historical backfills recompute old days from TODAY's sources,
-- which have received late-arriving events the legacy snapshot froze out
-- (observed: ~+0.3% rows, new side higher, uniformly) — so:
--
--   nightly / same-day runs : default tolerance 0.01% (effectively strict)
--   historical backfills    : pass tolerance 1% explicitly
--
-- Parameterized:
--   compare_days : lookback window, default 7
--   tolerance    : allowed relative diff on all metrics, default 0.0001 (0.01%)
--
--   uv run dbt test --select autopricing_ab_test --profiles-dir .                                # strict (nightly semantics)
--   uv run dbt test --select autopricing_ab_test --vars '{compare_days: 7, tolerance: 0.01}' --profiles-dir .   # backfill mode

{% set days = var('compare_days', 7) %}
{% set tol  = var('tolerance', 0.0001) %}

with new_side as (
    select
        created_at,
        count(*)                              as row_count,
        count(distinct search_id)             as distinct_searches,
        round(sum(bow_original_total_fare_usd), 2) as fare_usd_sum,
        countif(variant is not null)          as rows_with_variant
    from {{ ref('autopricing_ab_test') }}
    where created_at >= date_sub(current_date, interval {{ days }} day)
    group by 1
),

legacy_side as (
    select
        created_at,
        count(*)                              as row_count,
        count(distinct search_id)             as distinct_searches,
        round(sum(bow_original_total_fare_usd), 2) as fare_usd_sum,
        countif(variant is not null)          as rows_with_variant
    from {{ source('analysis', 'autopricing_ab_test_legacy') }}
    where created_at >= date_sub(current_date, interval {{ days }} day)
    group by 1
)

select
    coalesce(n.created_at, l.created_at) as day,
    case
        when n.created_at is null then 'day missing in dbt model'
        when l.created_at is null then 'day missing in legacy table'
        when abs(n.row_count - l.row_count) / nullif(abs(l.row_count), 0) > {{ tol }} then 'row count mismatch'
        when abs(n.distinct_searches - l.distinct_searches) / nullif(abs(l.distinct_searches), 0) > {{ tol }} then 'distinct searches mismatch'
        when abs(n.fare_usd_sum - l.fare_usd_sum) / nullif(abs(l.fare_usd_sum), 0) > {{ tol }} then 'fare sum mismatch'
        when abs(n.rows_with_variant - l.rows_with_variant) / nullif(abs(l.rows_with_variant), 0) > {{ tol }} then 'variant coverage mismatch'
    end as failed_check,
    n.row_count  as new_rows,   l.row_count  as legacy_rows,
    n.fare_usd_sum as new_fare, l.fare_usd_sum as legacy_fare,
    n.distinct_searches as new_searches, l.distinct_searches as legacy_searches
from new_side n
full outer join legacy_side l using (created_at)
where n.created_at is null
   or l.created_at is null
   or abs(n.row_count - l.row_count) / nullif(abs(l.row_count), 0) > {{ tol }}
   or abs(n.distinct_searches - l.distinct_searches) / nullif(abs(l.distinct_searches), 0) > {{ tol }}
   or abs(n.fare_usd_sum - l.fare_usd_sum) / nullif(abs(l.fare_usd_sum), 0) > {{ tol }}
   or abs(n.rows_with_variant - l.rows_with_variant) / nullif(abs(l.rows_with_variant), 0) > {{ tol }}
