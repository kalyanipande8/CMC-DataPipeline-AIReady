-- mart_site_quality_scorecard.sql
-- Semantic metrics 51-56: a single top-line scorecard per site, rolling up
-- the batch-grain cross-system mart -- the kind of metric a site-quality
-- dashboard would actually surface.

with batch_quality as (
    select * from {{ ref('mart_cross_system_batch_quality') }}
)

select
    site_id,

    -- metric 51: total batches
    count(*)                                                            as total_batches,
    -- metric 52: right-first-time rate (RFT) -- the headline CMC quality KPI
    round(
        sum(case when is_right_first_time then 1 else 0 end)::float
        / nullif(count(*), 0), 4
    )                                                                    as right_first_time_rate,
    -- metric 53: batches with any linked deviation
    sum(case when linked_deviation_count > 0 then 1 else 0 end)        as batches_with_deviation,
    -- metric 54: batches with an open (unresolved) deviation
    sum(case when has_open_deviation then 1 else 0 end)                as batches_with_open_deviation,
    -- metric 55: average yield attainment across all batches at this site
    round(avg(yield_attainment_pct), 2)                                 as avg_yield_attainment_pct,
    -- metric 56: average cycle time variance across all batches at this site
    round(avg(cycle_time_variance_hr), 2)                               as avg_cycle_time_variance_hr

from batch_quality
group by 1
