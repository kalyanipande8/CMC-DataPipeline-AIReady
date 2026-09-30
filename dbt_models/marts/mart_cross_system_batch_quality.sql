-- mart_cross_system_batch_quality.sql
-- Semantic metrics 44-50: batch-grain metrics joining MES + LIMS + QMS,
-- the genuinely cross-system view that MES-only or LIMS-only marts can't give.

with batches as (
    select * from {{ ref('stg_mes_batches') }}
),

lims_agg as (
    select
        batch_id,
        count(*)                                            as test_count,
        sum(case when is_oos then 1 else 0 end)             as oos_test_count,
        avg(result_value)                                   as avg_result_value
    from {{ ref('stg_lims_results') }}
    group by 1
),

qms_agg as (
    select
        batch_id,
        count(*)                                            as deviation_count,
        sum(case when severity = 'Critical' then 1 else 0 end) as critical_deviation_count,
        max(case when is_open then 1 else 0 end)            as has_open_deviation
    from {{ ref('stg_qms_deviations') }}
    group by 1
)

select
    b.batch_id,
    b.site_id,
    b.product_code,
    b.batch_disposition,

    -- metric 44: lab tests performed for this batch
    coalesce(l.test_count, 0)                                          as lab_test_count,
    -- metric 45: OOS test count for this batch
    coalesce(l.oos_test_count, 0)                                      as lab_oos_count,
    -- metric 46: batch-level "clean" flag (released, no OOS, no critical deviation)
    case
        when b.batch_disposition = 'Released'
             and coalesce(l.oos_test_count, 0) = 0
             and coalesce(q.critical_deviation_count, 0) = 0
        then true else false
    end                                                                 as is_right_first_time,
    -- metric 47: deviations linked to this batch
    coalesce(q.deviation_count, 0)                                     as linked_deviation_count,
    -- metric 48: has an unresolved (open) deviation
    coalesce(q.has_open_deviation, 0) = 1                              as has_open_deviation,
    -- metric 49: yield attainment for this batch (%)
    round(b.actual_yield_kg / nullif(b.planned_yield_kg, 0) * 100, 2)  as yield_attainment_pct,
    -- metric 50: cycle time variance vs. plan (hr)
    round(b.cycle_time_actual_hr - b.cycle_time_planned_hr, 2)         as cycle_time_variance_hr

from batches b
left join lims_agg l on b.batch_id = l.batch_id
left join qms_agg q on b.batch_id = q.batch_id
