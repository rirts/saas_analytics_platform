with deals as (

	select *
    from {{ ref('int_crm__deals_enriched') }}

),

agg_by_stage as (

	select
		deal_stage,
        
        -- counts by status
        count(*) as deals_count,
        sum(case when deal_status = 'open' then 1 else 0 end) as open_deals_count,
        sum(case when deal_status = 'won' then 1 else 0 end) as won_deals_count,
        sum(case when deal_status = 'lost' then 1 else 0 end) as lost_deals_count,
        
        -- amount by status
        sum(case when deal_status = 'open'
				 then amount_company_currency else 0 end) as open_amount_total,
		sum(case when deal_status = 'won'
				 then won_amount_company_currency else 0 end) as won_amount_total,
		sum(case when deal_status = 'lost'
				 then lost_amount_company_currency else 0 end) as lost_amount_total
	from deals
    group by deal_stage
    
),

final as (

	select
		deal_stage,
        deals_count,
        open_deals_count,
        won_deals_count,
        lost_deals_count,
        open_amount_total,
        won_amount_total,
        lost_amount_total
	from agg_by_stage
    
)

select *
from final
        