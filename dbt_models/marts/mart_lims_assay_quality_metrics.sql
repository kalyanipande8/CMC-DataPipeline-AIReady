-- mart_lims_assay_quality_metrics.sql
-- Semantic metrics 21-32: lab-result quality metrics grouped by site + assay.

with results as (
    select * from {{ ref('stg_lims_results') }}
)

select
    site_id,
    assay_name,

    -- metric 21: total tests performed
    count(*)                                                          as total_tests,
    -- metric 22: tests with a valid (non-null) result
    sum(case when result_value is not null then 1 else 0 end)        as valid_result_count,
    -- metric 23: OOS count
    sum(case when is_oos then 1 else 0 end)                          as oos_count,
    -- metric 24: OOS rate
    round(
        sum(case when is_oos then 1 else 0 end)::float
        / nullif(sum(case when result_value is not null then 1 else 0 end), 0), 4
    )                                                                  as oos_rate,
    -- metric 25: average result value
    round(avg(result_value), 4)                                       as avg_result_value,
    -- metric 26: result std dev (assay precision indicator)
    round(stddev_samp(result_value), 4)                               as result_stddev,
    -- metric 27: min result value observed
    round(min(result_value), 4)                                       as min_result_value,
    -- metric 28: max result value observed
    round(max(result_value), 4)                                       as max_result_value,
    -- metric 29: distinct batches tested
    count(distinct batch_id)                                          as distinct_batches_tested,
    -- metric 30: distinct analysts performing this assay
    count(distinct analyst_id)                                        as distinct_analysts,
    -- metric 31: distinct instruments used
    count(distinct instrument_id)                                     as distinct_instruments,
    -- metric 32: result completeness rate (non-null / total)
    round(
        sum(case when result_value is not null then 1 else 0 end)::float
        / nullif(count(*), 0), 4
    )                                                                  as result_completeness_rate

from results
group by 1, 2
