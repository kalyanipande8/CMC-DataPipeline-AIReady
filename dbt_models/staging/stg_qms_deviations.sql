-- stg_qms_deviations.sql
-- Staging model for raw QMS deviation/CAPA extract: types timestamps and
-- derives closure/aging fields shared by several downstream quality metrics.

with source as (

    select * from {{ source('cmc_raw', 'qms_deviations') }}

),

renamed as (

    select
        deviation_id,
        batch_id,
        site_id,
        deviation_type,
        severity,
        cast(opened_ts as timestamp) as opened_ts,
        case when closed_ts = '' then null else cast(closed_ts as timestamp) end as closed_ts,
        closure_sla_days,
        capa_required,
        nullif(capa_id, '') as capa_id,
        investigator_id
    from source

)

select
    *,
    case when closed_ts is null then true else false end as is_open,
    datediff('day', opened_ts, coalesce(closed_ts, current_timestamp)) as age_days
from renamed
