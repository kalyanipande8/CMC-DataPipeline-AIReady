-- mart_mes_product_metrics.sql
-- Semantic metrics 13-20: manufacturing metrics grouped by product across all sites.

with batches as (
    select * from {{ ref('stg_mes_batches') }}
)

select
    product_code,

    -- metric 13: total batches
    count(*)                                                          as total_batches,
    -- metric 14: distinct sites manufacturing this product
    count(distinct site_id)                                           as sites_manufacturing,
    -- metric 15: release rate by product
    round(
        sum(case when batch_disposition = 'Released' then 1 else 0 end)::float
        / nullif(count(*), 0), 4
    )                                                                  as release_rate,
    -- metric 16: avg actual yield by product
    round(avg(actual_yield_kg), 3)                                    as avg_actual_yield_kg,
    -- metric 17: yield std dev by product (process consistency indicator)
    round(stddev_samp(actual_yield_kg), 3)                            as yield_stddev_kg,
    -- metric 18: avg cycle time by product
    round(avg(cycle_time_actual_hr), 2)                               as avg_cycle_time_actual_hr,
    -- metric 19: cycle time overrun rate by product
    round(
        sum(case when cycle_time_actual_hr > cycle_time_planned_hr then 1 else 0 end)::float
        / nullif(sum(case when cycle_time_actual_hr is not null then 1 else 0 end), 0), 4
    )                                                                  as cycle_overrun_rate,
    -- metric 20: distinct process orders
    count(distinct process_order_id)                                  as distinct_process_orders

from batches
group by 1
