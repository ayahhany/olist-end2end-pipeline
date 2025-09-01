{{ config(materialized='view') }}

SELECT 
    mql_id, 
    seller_id, 
    cast(won_date as date) as won_date, 
    business_segment, 
    lead_type, 
    declared_monthly_revenue 
FROM 
    project_landing.leads_closed_ayahany
