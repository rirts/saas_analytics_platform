with source as (

    select *
    from {{ source('stripe', 'subscriptions_raw_stripe') }}

),

renamed as (

    select
        id       as subscription_id,
        customer as customer_id,
        status   as subscription_status,
        price_id,
        
        current_period_start as current_period_start_unix,
        current_period_end   as current_period_end_unix,
        
        timestamp '1970-01-01' + current_period_start * INTERVAL 1 SECOND as current_period_start_at,
        timestamp '1970-01-01' + current_period_end   * INTERVAL 1 SECOND as current_period_end_at
        
    from source

),

typed as (

    select
        subscription_id,
        customer_id,
        price_id,
        subscription_status,
        
        current_period_start_unix,
        current_period_end_unix,
        current_period_start_at,
        current_period_end_at,
        
        -- flags for convenience
        case when subscription_status = 'active'   then 1 else 0 end as is_active,
        case when subscription_status = 'canceled' then 1 else 0 end as is_canceled,
        case when subscription_status = 'trialing' then 1 else 0 end as is_trialing,
        
        -- Normalize plan type from price_id
        case 
            when price_id = 'price_monthly' then 'monthly'
            when price_id = 'price_annual'  then 'annual'
            else 'unknown'
        end as billing_period,
        
        -- Synthetic pricing logic
        case
            when price_id = 'price_monthly' then 50.0
            when price_id = 'price_annual'  then 300.0 / 12.0
        end as mrr_usd,
        
        case
            when price_id = 'price_monthly' then 50.0 * 12.0
            when price_id = 'price_annual'  then 300.0
        end as arr_usd
        
    from renamed
    
)

select *
from typed
