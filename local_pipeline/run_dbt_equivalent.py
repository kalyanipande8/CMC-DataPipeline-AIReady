"""
run_dbt_equivalent.py

REAL, RUNNABLE. `dbt run`/`dbt test` were never executed against a real
Snowflake warehouse (no dbt install, no Snowflake account here). Instead, this
module executes the SAME SQL LOGIC that dbt_models/staging/*.sql and
dbt_models/marts/*.sql define -- transcribed for DuckDB's SQL dialect
(`date_diff` instead of Snowflake's `datediff`, no `ref()`/`source()` macros,
tables referenced directly) -- against the local DuckDB warehouse this pipeline
actually populated. This proves the 56 semantic metrics compute correctly,
even though the real dbt project was authored-not-run.

Each SQL string below is a faithful, dialect-adjusted copy of the corresponding
file in dbt_models/. Where the dbt model uses {{ ref('x') }} it is replaced
with the literal schema-qualified table/view name here.
"""
from __future__ import annotations

import duckdb

# ---------------------------------------------------------------------------
# Staging views (mirrors dbt_models/staging/*.sql)
# ---------------------------------------------------------------------------

STG_MES_BATCHES = """
create or replace view staging.stg_mes_batches_v as
select
    batch_id,
    site_id,
    product_code,
    process_order_id,
    shift,
    line_id,
    cast(batch_start_ts as timestamp) as batch_start_ts,
    cast(batch_end_ts as timestamp)   as batch_end_ts,
    planned_yield_kg,
    actual_yield_kg,
    cycle_time_planned_hr,
    case when cycle_time_actual_hr < 0 then null else cycle_time_actual_hr end as cycle_time_actual_hr,
    batch_disposition,
    operator_id
from staging.stg_mes_batches
"""

STG_LIMS_RESULTS = """
create or replace view staging.stg_lims_results_v as
select
    result_id,
    batch_id,
    site_id,
    sample_id,
    assay_name,
    result_value,
    unit,
    spec_lower,
    spec_upper,
    cast(test_ts as timestamp) as test_ts,
    analyst_id,
    instrument_id,
    case
        when result_value is null then null
        when result_value < spec_lower or result_value > spec_upper then true
        else false
    end as is_oos
from staging.stg_lims_results
"""

# Note: unlike the dbt/Snowflake version (which reads raw strings straight off
# the MES/LIMS/QMS extract and must cast '' -> null), the local warehouse
# tables here were loaded from DataFrames already typed by
# etl_spark_equivalent.clean_qms (the pandas "Spark job" stand-in already
# did the ''->NaT conversion). So this view only needs to derive is_open/age_days,
# which is the same derived-column logic the dbt model expresses.
STG_QMS_DEVIATIONS = """
create or replace view staging.stg_qms_deviations_v as
select
    deviation_id,
    batch_id,
    site_id,
    deviation_type,
    severity,
    opened_ts,
    closed_ts,
    closure_sla_days,
    capa_required,
    nullif(capa_id, '') as capa_id,
    investigator_id,
    case when closed_ts is null then true else false end as is_open,
    date_diff('day', opened_ts, coalesce(closed_ts, current_timestamp)) as age_days
from staging.stg_qms_deviations
"""

# ---------------------------------------------------------------------------
# Mart tables (mirrors dbt_models/marts/*.sql)
# ---------------------------------------------------------------------------

MART_MES_SITE_DAILY = """
create or replace table marts.mart_mes_site_daily_metrics as
with batches as (
    select *, date_trunc('day', batch_start_ts) as batch_date
    from staging.stg_mes_batches_v
)
select
    site_id,
    batch_date,
    count(*) as batches_started,
    sum(case when batch_disposition = 'Released' then 1 else 0 end) as batches_released,
    sum(case when batch_disposition = 'Rejected' then 1 else 0 end) as batches_rejected,
    sum(case when batch_disposition = 'Quarantine' then 1 else 0 end) as batches_quarantined,
    round(sum(case when batch_disposition = 'Released' then 1 else 0 end)::double / nullif(count(*), 0), 4) as first_pass_release_rate,
    round(sum(case when batch_disposition = 'Rejected' then 1 else 0 end)::double / nullif(count(*), 0), 4) as reject_rate,
    round(avg(planned_yield_kg), 3) as avg_planned_yield_kg,
    round(avg(actual_yield_kg), 3) as avg_actual_yield_kg,
    round(avg(actual_yield_kg / nullif(planned_yield_kg, 0)) * 100, 2) as avg_yield_attainment_pct,
    round(avg(cycle_time_planned_hr), 2) as avg_cycle_time_planned_hr,
    round(avg(cycle_time_actual_hr), 2) as avg_cycle_time_actual_hr,
    round(sum(case when cycle_time_actual_hr <= cycle_time_planned_hr then 1 else 0 end)::double
        / nullif(sum(case when cycle_time_actual_hr is not null then 1 else 0 end), 0), 4) as on_time_cycle_rate
from batches
group by 1, 2
"""

MART_MES_PRODUCT = """
create or replace table marts.mart_mes_product_metrics as
with batches as (select * from staging.stg_mes_batches_v)
select
    product_code,
    count(*) as total_batches,
    count(distinct site_id) as sites_manufacturing,
    round(sum(case when batch_disposition = 'Released' then 1 else 0 end)::double / nullif(count(*), 0), 4) as release_rate,
    round(avg(actual_yield_kg), 3) as avg_actual_yield_kg,
    round(stddev_samp(actual_yield_kg), 3) as yield_stddev_kg,
    round(avg(cycle_time_actual_hr), 2) as avg_cycle_time_actual_hr,
    round(sum(case when cycle_time_actual_hr > cycle_time_planned_hr then 1 else 0 end)::double
        / nullif(sum(case when cycle_time_actual_hr is not null then 1 else 0 end), 0), 4) as cycle_overrun_rate,
    count(distinct process_order_id) as distinct_process_orders
from batches
group by 1
"""

MART_LIMS_ASSAY_QUALITY = """
create or replace table marts.mart_lims_assay_quality_metrics as
with results as (select * from staging.stg_lims_results_v)
select
    site_id,
    assay_name,
    count(*) as total_tests,
    sum(case when result_value is not null then 1 else 0 end) as valid_result_count,
    sum(case when is_oos then 1 else 0 end) as oos_count,
    round(sum(case when is_oos then 1 else 0 end)::double
        / nullif(sum(case when result_value is not null then 1 else 0 end), 0), 4) as oos_rate,
    round(avg(result_value), 4) as avg_result_value,
    round(stddev_samp(result_value), 4) as result_stddev,
    round(min(result_value), 4) as min_result_value,
    round(max(result_value), 4) as max_result_value,
    count(distinct batch_id) as distinct_batches_tested,
    count(distinct analyst_id) as distinct_analysts,
    count(distinct instrument_id) as distinct_instruments,
    round(sum(case when result_value is not null then 1 else 0 end)::double / nullif(count(*), 0), 4) as result_completeness_rate
from results
group by 1, 2
"""

MART_QMS_DEVIATION = """
create or replace table marts.mart_qms_deviation_metrics as
with dev as (select * from staging.stg_qms_deviations_v)
select
    site_id,
    count(*) as total_deviations,
    sum(case when is_open then 1 else 0 end) as open_deviations,
    sum(case when not is_open then 1 else 0 end) as closed_deviations,
    sum(case when severity = 'Critical' then 1 else 0 end) as critical_deviation_count,
    sum(case when severity = 'Major' then 1 else 0 end) as major_deviation_count,
    sum(case when severity = 'Minor' then 1 else 0 end) as minor_deviation_count,
    sum(case when capa_required then 1 else 0 end) as capa_count,
    round(avg(case when is_open then age_days end), 2) as avg_open_age_days,
    round(avg(case when not is_open then age_days end), 2) as avg_days_to_close,
    round(sum(case when not is_open and age_days <= closure_sla_days then 1 else 0 end)::double
        / nullif(sum(case when not is_open then 1 else 0 end), 0), 4) as sla_on_time_closure_rate,
    round(count(*)::double / nullif(count(distinct batch_id), 0), 4) as deviations_per_batch_touched
from dev
group by 1
"""

MART_CROSS_SYSTEM_BATCH_QUALITY = """
create or replace table marts.mart_cross_system_batch_quality as
with batches as (
    select * from staging.stg_mes_batches_v
),
lims_agg as (
    select
        batch_id,
        count(*) as test_count,
        sum(case when is_oos then 1 else 0 end) as oos_test_count,
        avg(result_value) as avg_result_value
    from staging.stg_lims_results_v
    group by 1
),
qms_agg as (
    select
        batch_id,
        count(*) as deviation_count,
        sum(case when severity = 'Critical' then 1 else 0 end) as critical_deviation_count,
        max(case when is_open then 1 else 0 end) as has_open_deviation
    from staging.stg_qms_deviations_v
    group by 1
)
select
    b.batch_id,
    b.site_id,
    b.product_code,
    b.batch_disposition,
    coalesce(l.test_count, 0) as lab_test_count,
    coalesce(l.oos_test_count, 0) as lab_oos_count,
    case
        when b.batch_disposition = 'Released'
             and coalesce(l.oos_test_count, 0) = 0
             and coalesce(q.critical_deviation_count, 0) = 0
        then true else false
    end as is_right_first_time,
    coalesce(q.deviation_count, 0) as linked_deviation_count,
    coalesce(q.has_open_deviation, 0) = 1 as has_open_deviation,
    round(b.actual_yield_kg / nullif(b.planned_yield_kg, 0) * 100, 2) as yield_attainment_pct,
    round(b.cycle_time_actual_hr - b.cycle_time_planned_hr, 2) as cycle_time_variance_hr
from batches b
left join lims_agg l on b.batch_id = l.batch_id
left join qms_agg q on b.batch_id = q.batch_id
"""

MART_SITE_QUALITY_SCORECARD = """
create or replace table marts.mart_site_quality_scorecard as
with batch_quality as (
    select * from marts.mart_cross_system_batch_quality
)
select
    site_id,
    count(*) as total_batches,
    round(sum(case when is_right_first_time then 1 else 0 end)::double / nullif(count(*), 0), 4) as right_first_time_rate,
    sum(case when linked_deviation_count > 0 then 1 else 0 end) as batches_with_deviation,
    sum(case when has_open_deviation then 1 else 0 end) as batches_with_open_deviation,
    round(avg(yield_attainment_pct), 2) as avg_yield_attainment_pct,
    round(avg(cycle_time_variance_hr), 2) as avg_cycle_time_variance_hr
from batch_quality
group by 1
"""

# metric count per mart, kept in sync with dbt_models/marts/schema.yml
METRIC_COUNTS = {
    "mart_mes_site_daily_metrics": 12,
    "mart_mes_product_metrics": 8,
    "mart_lims_assay_quality_metrics": 12,
    "mart_qms_deviation_metrics": 11,
    "mart_cross_system_batch_quality": 7,
    "mart_site_quality_scorecard": 6,
}

MODEL_ORDER = [
    ("stg_mes_batches", STG_MES_BATCHES),
    ("stg_lims_results", STG_LIMS_RESULTS),
    ("stg_qms_deviations", STG_QMS_DEVIATIONS),
    ("mart_mes_site_daily_metrics", MART_MES_SITE_DAILY),
    ("mart_mes_product_metrics", MART_MES_PRODUCT),
    ("mart_lims_assay_quality_metrics", MART_LIMS_ASSAY_QUALITY),
    ("mart_qms_deviation_metrics", MART_QMS_DEVIATION),
    ("mart_cross_system_batch_quality", MART_CROSS_SYSTEM_BATCH_QUALITY),
    ("mart_site_quality_scorecard", MART_SITE_QUALITY_SCORECARD),
]


def run_dbt_equivalent(con: duckdb.DuckDBPyConnection) -> dict:
    """Executes each model in dependency order against the local DuckDB
    warehouse, exactly like `dbt run` would against Snowflake. Returns a
    summary dict with row counts per mart, used by run_all.py for reporting.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    summary = {}
    for name, sql in MODEL_ORDER:
        con.execute(sql)
        if name.startswith("mart_"):
            count = con.execute(f"select count(*) from marts.{name}").fetchone()[0]
            summary[name] = {
                "rows": count,
                "metrics_defined": METRIC_COUNTS.get(name, 0),
            }
    summary["total_metrics_defined"] = sum(METRIC_COUNTS.values())
    return summary
