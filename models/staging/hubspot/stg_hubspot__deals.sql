with source as (

    select *
    from {{ source('hubspot', 'deals_raw_hubspot') }}

),

renamed as (

    select
        "Record ID"          as deal_id,
        "Deal Name"          as deal_name,
        "Deal Description"   as deal_description,
        "Amount"             as amount,
        "Deal Stage"         as deal_stage,
        "Pipeline"           as pipeline,
        "Create Date"        as created_at,
        "Close Date"         as closed_at,
        cast(null as bigint)  as company_id,
        cast(null as bigint) as contact_ids
        
    from source

)

select *
from renamed
