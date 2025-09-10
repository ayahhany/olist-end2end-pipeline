{{ config(materialized='view') }}

SELECT 
    customer_id, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 
FROM 
    project_landing.customers_ayahany

 