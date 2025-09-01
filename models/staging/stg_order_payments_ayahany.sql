{{ config(materialized='view') }}

SELECT 
    order_id, 
    payment_sequential, 
    payment_type, 
    cast(payment_installments as int) as payment_installments, 
    cast(payment_value as numeric) as payment_value 
FROM 
    project_landing.order_payments_ayahany



 