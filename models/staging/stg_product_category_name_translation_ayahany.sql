{{ config(materialized='view') }}

SELECT 
    product_category_name,
    product_category_name_english,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 
FROM 
    project_landing.product_category_name_translation_ayahany
