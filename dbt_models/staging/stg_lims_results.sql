-- stg_lims_results.sql
-- Staging model for raw LIMS test-result extract: types columns and derives
-- an in-spec/out-of-spec (OOS) flag used by many downstream quality metrics.

with source as (

    select * from {{ source('cmc_raw', 'lims_test_results') }}

),

renamed as (

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
    from source

)

select * from renamed
