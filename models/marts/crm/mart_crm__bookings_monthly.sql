with deals as (
	
    select *
    from {{ ref('int_crm__deals_enriched') }}
    where deal_status = 'won'
	and booking_date is not null
    
),

aggregated as (

	select 
		cast(date_trunc('month', booking_date) as date) as booking_month,
        
        count(distinct deal_id) as won_deals_count,
        sum(won_amount_company_currency) as won_amount_company_currency,
        
        sum(amount_company_currency) as total_amount_company_currency,
        
        avg(sales_cycle_days) as avg_sales_cycle_days
        
	from deals
    group by
		cast(date_trunc('month', booking_date) as date)
        
)

select *
from aggregated