{{ config(
    materialized='view'
) }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products_ayahany') }}
),

translations AS (
    SELECT * FROM {{ ref('stg_product_category_name_translation_ayahany') }}
),

final AS (
    SELECT
        p.product_id,
        p.product_category_name,
        t.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.updated_at_timestamp

    FROM products p
    LEFT JOIN translations t
        ON p.product_category_name = t.product_category_name
)

SELECT * FROM final
