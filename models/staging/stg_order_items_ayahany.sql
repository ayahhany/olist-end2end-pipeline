{{ config(materialized='view') }}

SELECT 
    order_id, 
    order_item_id, 
    product_id, 
    seller_id, 
    cast(shipping_limit_date as timestamp) as shipping_limit_date, 
    cast(price as numeric) as price, 
    cast(freight_value as numeric) as freight_value,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 

FROM 
    project_landing.order_items_ayahany



 