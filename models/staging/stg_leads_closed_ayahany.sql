{{ config(materialized='view') }}

SELECT 
    mql_id, 
    seller_id, 
    cast(won_date as date) as won_date, 
    business_segment, 
    lead_type, 
    declared_monthly_revenue,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 

FROM 
    project_landing.leads_closed_ayahany
