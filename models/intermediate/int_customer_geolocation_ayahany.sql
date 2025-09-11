{{ config(
    materialized='view'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers_ayahany') }}
),

geolocation AS (
    SELECT * FROM {{ ref('int_geolocation_deduplicated_ayahany') }}
),

final AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        g.latitude,
        g.longitude,
        g.city,
        g.state,
        c.updated_at_timestamp

    FROM customers c
    LEFT JOIN geolocation g
        ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
)

SELECT * FROM final
