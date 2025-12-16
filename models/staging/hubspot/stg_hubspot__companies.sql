with companies_raw as (

	select *
    from {{ source('hubspot', 'companies_raw_hubspot') }}
    
),

renamed as (

	select
		-- identifiers
        cast("Record ID" as bigint) 		as company_id,
        
        -- core attributes
        "Company name" as company_name,
        lower("Company Domain Name") 		as company_domain,
        
        -- custom SaaS industry field
        "Saas Industry" 					as saas_industry,
        
        -- geography
        "Country/Region" 					as country,
        
        -- timestamps
        "Create Date" 						as created_at_raw
	
    from companies_raw
)

select *
from renamed

    