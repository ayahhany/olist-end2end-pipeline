{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT
        geolocation_zip_code_prefix,
        MAX(city) as city,
        MAX(state) as state
    FROM {{ ref('stg_geolocation_ayahany') }}
    GROUP BY
        geolocation_zip_code_prefix
)

SELECT * FROM source
