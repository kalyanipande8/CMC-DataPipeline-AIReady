-- mart_qms_deviation_metrics.sql
-- Semantic metrics 33-43: quality-events (deviation/CAPA) metrics by site.

with dev as (
    select * from {{ ref('stg_qms_deviations') }}
)

select
    site_id,

    -- metric 33: total deviations
    count(*)                                                            as total_deviations,
    -- metric 34: open deviations
    sum(case when is_open then 1 else 0 end)                           as open_deviations,
    -- metric 35: closed deviations
    sum(case when not is_open then 1 else 0 end)                       as closed_deviations,
    -- metric 36: critical deviation count
    sum(case when severity = 'Critical' then 1 else 0 end)             as critical_deviation_count,
    -- metric 37: major deviation count
    sum(case when severity = 'Major' then 1 else 0 end)                as major_deviation_count,
    -- metric 38: minor deviation count
    sum(case when severity = 'Minor' then 1 else 0 end)                as minor_deviation_count,
    -- metric 39: CAPA count
    sum(case when capa_required then 1 else 0 end)                     as capa_count,
    -- metric 40: average age of open deviations (days)
    round(avg(case when is_open then age_days end), 2)                 as avg_open_age_days,
    -- metric 41: average time to close (days), closed only
    round(avg(case when not is_open then age_days end), 2)             as avg_days_to_close,
    -- metric 42: on-time closure rate vs. closure_sla_days
    round(
        sum(case when not is_open and age_days <= closure_sla_days then 1 else 0 end)::float
        / nullif(sum(case when not is_open then 1 else 0 end), 0), 4
    )                                                                    as sla_on_time_closure_rate,
    -- metric 43: deviations per 100 batches touched (approx via distinct batch_id)
    round(count(*)::float / nullif(count(distinct batch_id), 0), 4)     as deviations_per_batch_touched

from dev
group by 1
