{{ config(
    materialized='incremental',
    incremental_strategy = 'merge',
    unique_key = 'customer_id'
    ) }}

SELECT 
customer_id, customer_unique_id, customer_city, customer_state 
from project_landing.customers_ayahany