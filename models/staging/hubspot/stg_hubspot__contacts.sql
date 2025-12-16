with contacts_raw as (

	select *
    from {{ source('hubspot','contacts_raw_hubspot') }}
    
),

renamed as (

	select 
		-- identifiers
        cast("Record ID" as bigint) as contact_id,
        
        -- basic attributes
        lower("Email") 				as email,
        "First Name" 				as first_name,
        "Last Name" 				as last_name,
        "Job Title" 				as job_title,
        
        -- lifecycle
        "Lifecycle Stage" 			as lifecycle_stage,
        
        -- associated company (there's no ID in the CSV)
        "Company Name" 				as associated_company_name,
        
        -- geography
        "Country/Region" 			as country,
        
        -- timestamps
        "Create Date" 				as created_at_raw,
        "Last Activity Date" 		as last_activity_at_raw
        
	from contacts_raw
)

select *
from renamed
        