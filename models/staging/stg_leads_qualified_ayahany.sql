{{ config(materialized='view') }}

SELECT 
    mql_id, 
    cast(first_contact_date as date) as first_contact_date, 
    landing_page_id, 
    origin,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 

FROM 
    project_landing.leads_qualified_ayahany



 