{{ config(
    unique_key=['order_id', 'order_item_id'],
    materialized='incremental',
    partition_by={
        'field': 'order_purchase_date',
        'data_type': 'date'
    },
    cluster_by=['order_id', 'seller_id', 'product_id']
) }}

WITH consolidated_orders AS (
    SELECT * FROM {{ ref('int_orders_consolidated_ayahany') }}
    {% if is_incremental() %}
        WHERE updated_at_timestamp >= (SELECT MAX(updated_at_timestamp) FROM {{ this }})
    {% endif %}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items_ayahany') }}
),

all_joins AS (
    SELECT
        co.order_id,
        co.customer_id, 
        co.order_status,
        co.order_purchase_timestamp,
        co.order_delivered_customer_date,
        co.order_estimated_delivery_date,
        co.average_review_score,
        co.updated_at_timestamp,
        c.customer_unique_id,
        oi.seller_id,
        oi.product_id,
        oi.order_item_id,
        oi.price AS item_price,
        oi.freight_value AS item_freight_value
        
    FROM consolidated_orders co
    LEFT JOIN {{ ref('stg_customers_ayahany') }} c
        ON co.customer_id = c.customer_id
    INNER JOIN order_items oi
        ON co.order_id = oi.order_id
),

final AS (
    SELECT
        order_id,
        order_item_id,
        customer_id, 
        customer_unique_id,
        seller_id,
        product_id, 
        order_status,
        
        -- Date and Delivery Calculations
        DATE(order_purchase_timestamp) AS order_purchase_date,
        DATE_DIFF(DATE(order_delivered_customer_date), DATE(order_purchase_timestamp), DAY) AS delivery_days,
        CASE
            WHEN order_delivered_customer_date IS NOT NULL 
                 AND DATE(order_delivered_customer_date) <= DATE(order_estimated_delivery_date) THEN 'On-time'
            WHEN order_delivered_customer_date IS NOT NULL 
                 AND DATE(order_delivered_customer_date) > DATE(order_estimated_delivery_date) THEN 'Late'
            ELSE 'Not Delivered'
        END AS delivery_status,
        
        -- Financial Metrics
        item_price, 
        item_freight_value, 
        (item_price + item_freight_value) AS item_total_revenue,
        1 AS quantity_sold, -- Each row is one item line
        
        -- Quality Metrics
        average_review_score,
        
        updated_at_timestamp
        
    FROM all_joins
    WHERE item_price IS NOT NULL 
)

SELECT * FROM final
