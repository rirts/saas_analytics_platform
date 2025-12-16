with subscriptions as (

	select *
    from {{ ref('stg_stripe__subscriptions') }}
    
),

customers as (

	select *
    from {{ ref('stg_stripe__customers') }}
    
),

invoices as (

	select *
    from {{ ref('stg_stripe__invoices') }}
    
),

invoices_agg as (

	select
		subscription_id,
        
        count(*) as invoices_count,
        
        -- amounts are in cents in the raw data
        sum(amount_due) / 100.0 as total_invoiced_usd,
        sum(amount_paid) / 100.0 as total_paid_usd,
        
        max(invoice_created_at) as last_invoice_at,
        max(
			case
				when invoice_status = 'paid' then 1
                else 0
			end
		) as has_any_paid_invoice
        
	from invoices
    group by subscription_id
    
),

joined as (

	select
		s.subscription_id,
        s.customer_id,
        
        c.customer_name,
        c.customer_email,
        c.customer_currency,
        
        s.subscription_status,
        s.billing_period,
        s.price_id,
        
        s.current_period_start_at,
        s.current_period_end_at,
        
        s.is_active,
        s.is_canceled,
        s.is_trialing,
        
        s.mrr_usd,
        s.arr_usd,
        
        ia.invoices_count,
        ia.total_invoiced_usd,
        ia.total_paid_usd,
        ia.last_invoice_at,
        ia.has_any_paid_invoice
        
	from subscriptions s
    left join customers c
		on s.customer_id = c.customer_id
	left join invoices_agg ia
		on s.subscription_id = ia.subscription_id
        
)

select *
from joined
