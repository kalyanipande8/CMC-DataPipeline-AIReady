-- mart_mes_site_daily_metrics.sql
-- Semantic metrics 1-12: manufacturing execution metrics, grouped by site + day.
-- Batch date is truncated from batch_start_ts.

with batches as (

    select *, date_trunc('day', batch_start_ts) as batch_date
    from {{ ref('stg_mes_batches') }}

)

select
    site_id,
    batch_date,

    -- metric 1: batches started
    count(*)                                                            as batches_started,
    -- metric 2: batches released
    sum(case when batch_disposition = 'Released' then 1 else 0 end)     as batches_released,
    -- metric 3: batches rejected
    sum(case when batch_disposition = 'Rejected' then 1 else 0 end)     as batches_rejected,
    -- metric 4: batches quarantined
    sum(case when batch_disposition = 'Quarantine' then 1 else 0 end)   as batches_quarantined,
    -- metric 5: first-pass release rate
    round(
        sum(case when batch_disposition = 'Released' then 1 else 0 end)::float
        / nullif(count(*), 0), 4
    )                                                                    as first_pass_release_rate,
    -- metric 6: reject rate
    round(
        sum(case when batch_disposition = 'Rejected' then 1 else 0 end)::float
        / nullif(count(*), 0), 4
    )                                                                    as reject_rate,
    -- metric 7: average planned yield (kg)
    round(avg(planned_yield_kg), 3)                                     as avg_planned_yield_kg,
    -- metric 8: average actual yield (kg)
    round(avg(actual_yield_kg), 3)                                      as avg_actual_yield_kg,
    -- metric 9: average yield attainment (%)
    round(avg(actual_yield_kg / nullif(planned_yield_kg, 0)) * 100, 2)  as avg_yield_attainment_pct,
    -- metric 10: average planned cycle time (hr)
    round(avg(cycle_time_planned_hr), 2)                                as avg_cycle_time_planned_hr,
    -- metric 11: average actual cycle time (hr)
    round(avg(cycle_time_actual_hr), 2)                                 as avg_cycle_time_actual_hr,
    -- metric 12: on-time cycle rate (actual <= planned)
    round(
        sum(case when cycle_time_actual_hr <= cycle_time_planned_hr then 1 else 0 end)::float
        / nullif(sum(case when cycle_time_actual_hr is not null then 1 else 0 end), 0), 4
    )                                                                    as on_time_cycle_rate

from batches
group by 1, 2
