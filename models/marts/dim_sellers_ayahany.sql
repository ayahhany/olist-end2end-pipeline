{{ config(
    materialized='table',
    unique_key='seller_id'
) }}

WITH sellers AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ ref('stg_sellers_ayahany') }}
),

geolocation AS (
    SELECT * FROM {{ ref('stg_geolocation_ayahany') }}
),

final AS (
    SELECT
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,
        g.latitude,
        g.longitude
    FROM sellers s
    LEFT JOIN geolocation g
        ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
)

SELECT * FROM final
