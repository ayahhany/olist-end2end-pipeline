{{ config(
    materialized='incremental',
    incremental_strategy = 'merge',
    unique_key = 'seller_id'
    ) }}

SELECT 
seller_id, seller_city, seller_state 
from project_landing.sellers_ayahany