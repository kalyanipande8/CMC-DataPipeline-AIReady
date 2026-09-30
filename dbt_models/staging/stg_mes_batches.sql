-- stg_mes_batches.sql
-- Staging model: light cleaning/typing of the raw MES batch-record extract.
-- AUTHORED for Snowflake via dbt `source()`; logic mirrored for real against
-- DuckDB in local_pipeline/run_dbt_equivalent.py (see stg_mes_batches there).

with source as (

    select * from {{ source('cmc_raw', 'mes_batch_records') }}

),

renamed as (

    select
        batch_id,
        site_id,
        product_code,
        process_order_id,
        shift,
        line_id,
        cast(batch_start_ts as timestamp)      as batch_start_ts,
        cast(batch_end_ts as timestamp)        as batch_end_ts,
        planned_yield_kg,
        actual_yield_kg,
        cycle_time_planned_hr,
        -- negative cycle times are a known data-quality defect from the MES
        -- extract; null them out here rather than in every downstream model
        case when cycle_time_actual_hr < 0 then null else cycle_time_actual_hr end as cycle_time_actual_hr,
        batch_disposition,
        operator_id
    from source

)

select * from renamed
