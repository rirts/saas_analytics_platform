with source as (
	select * 
    from {{ source('stripe', 'customers_raw_stripe') }}
    
),

renamed as (
	select
		id as customer_id,
        email as customer_email,
        name as customer_name,
        currency as customer_currency,
        -- unix timestamp
        created as customer_created_unix,
        -- convert unix timestamp to datetime
        timestamp '1970-01-01' + created * INTERVAL 1 SECOND as customer_created_at,
        
	from source
    
)

select *
from renamed
