{{ config(materialized='view') }}

SELECT 
    product_id, 
    product_category_name, 
    product_name_lenght as product_name_length, 
    product_description_lenght as product_description_length, 
    product_photos_qty, 
    cast(product_weight_g as numeric) as product_weight_g, 
    cast(product_length_cm as numeric) as product_length_cm, 
    cast(product_height_cm as numeric) as product_height_cm, 
    cast(product_width_cm as numeric) as product_width_cm
FROM 
    project_landing.products_ayahany



 