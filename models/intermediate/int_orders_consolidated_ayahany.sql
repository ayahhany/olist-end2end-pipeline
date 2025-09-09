{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders_ayahany') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items_ayahany') }}
),

order_payments AS (
    SELECT * FROM {{ ref('stg_order_payments_ayahany') }}
),

order_reviews AS (
    SELECT * FROM {{ ref('stg_order_reviews_ayahany') }}
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.updated_at_timestamp,
        
        -- Aggregate order item data
        SUM(oi.price) AS total_item_value,
        SUM(oi.freight_value) AS total_freight_value,
        COUNT(oi.order_item_id) AS total_items,
        COUNT(DISTINCT oi.product_id) AS distinct_products,

        -- Aggregate payment data
        SUM(op.payment_value) AS total_payment_value,
        COUNT(op.payment_sequential) AS total_payments,
        MAX(op.payment_type) AS primary_payment_type,

        -- Aggregate review data
        AVG(orv.review_score) AS average_review_score

    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN order_payments op ON o.order_id = op.order_id
    LEFT JOIN order_reviews orv ON o.order_id = orv.order_id
    GROUP BY
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.updated_at_timestamp
)

SELECT * FROM final
