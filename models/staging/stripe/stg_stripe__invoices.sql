with source as (

	select *
    from {{ source('stripe', 'invoices_raw_stripe') }}
    
),

renamed as (

	select
		id as invoice_id,
        customer as customer_id,
        subscription as subscription_id,
        
        amount_due,
        amount_paid,
        status as invoice_status,
        currency,
        
        created as invoice_created_unix,
        timestamp '1970-01-01' + created * INTERVAL 1 SECOND as invoice_created_at,
        
        cast(paid as bit) as is_paid
        
	from source

)

select *
from renamed
