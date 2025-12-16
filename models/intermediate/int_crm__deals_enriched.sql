with deals as (

    -- capa de staging
    select *
    from {{ ref('stg_hubspot__deals') }}

),

typed as (

    select
        deal_id,
        deal_name,
        deal_description,
        deal_stage,
        pipeline,

        -- parseamos las fechas que vienen como texto: '10/12/2025 11:55'
        try_strptime(created_at, '%d/%m/%Y %H:%M') as created_at_ts,
        try_strptime(closed_at,  '%d/%m/%Y %H:%M') as close_date_ts,

        -- importes: usamos amount como base de todo
        try_cast(amount as double) as amount_original,
        try_cast(amount as double) as amount_company_currency,
        try_cast(amount as double) as annual_contract_value,
        try_cast(amount as double) as total_contract_value,

        -- days_to_close "de fuente" (simplemente la diferencia en días)
        case
            when created_at is not null
             and closed_at  is not null
            then datediff(
                'day',
                try_strptime(created_at, '%d/%m/%Y %H:%M'),
                try_strptime(closed_at,  '%d/%m/%Y %H:%M')
            )
            else null
        end as days_to_close_source,

        -- flags derivados del Deal Stage
        case
            when lower(deal_stage) like '%closed won%' then 1
            else 0
        end as is_closed_won,

        case
            when lower(deal_stage) like '%closed lost%' then 1
            else 0
        end as is_closed_lost,

        case
            when lower(deal_stage) like '%closed%' then 1
            else 0
        end as is_deal_closed,

        -- placeholder: no lo tenemos en el CSV
        cast(null as varchar) as forecast_category,

        -- ownership (no vienen en el CSV, los dejamos nulos)
        cast(null as varchar) as hubspot_team,
        cast(null as bigint)  as created_by_user_id,
        cast(null as bigint)  as updated_by_user_id

    from deals

),

derived as (

    select
        *,

        -- estado lógico del deal
        case
            when is_closed_won = 1 then 'won'
            when is_closed_lost = 1 then 'lost'
            when is_deal_closed = 1 then 'closed_other'
            else 'open'
        end as deal_status,

        -- fecha de booking (solo para won)
        case
            when is_closed_won = 1 and close_date_ts is not null
                then cast(close_date_ts as date)
            else null
        end as booking_date,

        -- sales cycle recalculado por nosotros
        case
            when is_closed_won = 1
             and created_at_ts is not null
             and close_date_ts is not null
            then datediff('day', created_at_ts, close_date_ts)
            else null
        end as sales_cycle_days,

        -- montos derivados
        case
            when is_closed_won = 1 then amount_company_currency
            else null
        end as won_amount_company_currency,

        case
            when is_closed_lost = 1 then amount_company_currency
            else null
        end as lost_amount_company_currency

    from typed

),

final as (

    select
        deal_id,
        deal_name,
        deal_description,
        deal_stage,
        pipeline,
        deal_status,

        created_at_ts as created_at,
        close_date_ts as close_date,
        booking_date,
        sales_cycle_days,
        days_to_close_source,

        is_deal_closed,
        is_closed_won,
        is_closed_lost,
        forecast_category,

        amount_original,
        amount_company_currency,
        annual_contract_value,
        total_contract_value,
        won_amount_company_currency,
        lost_amount_company_currency,

        hubspot_team,
        created_by_user_id,
        updated_by_user_id

    from derived

)

select *
from final
