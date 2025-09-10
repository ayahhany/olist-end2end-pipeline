{{ config(
    materialized='table',
    unique_key='product_id'
) }}

WITH products AS (
    SELECT * FROM {{ ref('int_product_with_translation_ayahany') }}
),

final AS (
    SELECT
        product_id,
        product_category_name,
        product_category_name_english,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        updated_at_timestamp
    FROM products
)

SELECT * FROM final
