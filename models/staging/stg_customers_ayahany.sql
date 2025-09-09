{{ config(materialized='view') }}

<<<<<<< HEAD
with source as ( 
    select * from {{ source('project_landing', 'customers_ayahany') }} 
), 

renamed as ( 
    select 
        customer_id, 
        customer_unique_id, 
        customer_zip_code_prefix, 
        customer_city, 
        customer_state 
    from source 
) 

select * from renamed; 
=======
SELECT 
    customer_id, 
    customer_unique_id, 
    customer_zip_code_prefix, 
    customer_city, 
    customer_state,
    cast(updated_at_timestamp as timestamp) as updated_at_timestamp 
FROM 
    project_landing.customers_ayahany


>>>>>>> b905eb28d4708165c553b19e025c4ac1a67343fd

 