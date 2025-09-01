{{ config(materialized='view') }}

SELECT 
    product_category_name,
    product_category_name_english       
FROM 
    project_landing.product_category_name_translation_ayahany
