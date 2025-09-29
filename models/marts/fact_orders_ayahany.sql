{{ config(
    materialized='view',
    unique_key='order_id',
) }}

WITH consolidated_orders AS (
    SELECT * FROM {{ ref('int_orders_consolidated_ayahany') }}
),

orders_and_customers AS (
    SELECT
        co.*,
        c.customer_unique_id
    FROM consolidated_orders co
    LEFT JOIN {{ ref('stg_customers_ayahany') }} c
        ON co.customer_id = c.customer_id
),

final AS (
    SELECT
        order_id,
        customer_id,
        customer_unique_id,
        order_status,
        DATE_DIFF(DATE(order_delivered_customer_date), DATE(order_purchase_timestamp), DAY) AS delivery_days,
        CASE
            WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
                 AND DATE(order_delivered_customer_date) <= DATE(order_estimated_delivery_date) THEN 'On-time'
            WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
                 AND DATE(order_delivered_customer_date) > DATE(order_estimated_delivery_date) THEN 'Late'
            ELSE 'Not Delivered'
        END AS delivery_status,

        DATE(order_purchase_timestamp) AS order_purchase_date,
        order_purchase_timestamp,

        total_item_value,
        total_freight_value,
        total_payment_value,
        total_items,
        distinct_products,
        average_review_score,
        updated_at_timestamp
        
    FROM orders_and_customers
    WHERE total_payment_value IS NOT NULL
)

SELECT * FROM final
