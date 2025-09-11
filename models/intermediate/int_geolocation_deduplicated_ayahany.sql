{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        geolocation_zip_code_prefix,
        MAX(latitude) as latitude,
        MAX(longitude) as longitude,
        MAX(city) as city,
        MAX(state) as state
    FROM {{ ref('stg_geolocation_ayahany') }}
    GROUP BY
        geolocation_zip_code_prefix
)

SELECT * FROM source
