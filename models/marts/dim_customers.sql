{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('int_customer_geolocation_ayahany') }}
),

final AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        latitude,
        longitude,
        updated_at_timestamp
    FROM customers
)

SELECT * FROM final
