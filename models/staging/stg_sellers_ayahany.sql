{{ config(materialized='view') }}

SELECT 
    seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state 
FROM 
    project_landing.sellers_ayahany



 