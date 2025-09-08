{{ config(
    materialized='incremental',
    incremental_strategy = 'merge',
    unique_key = 'order_id'
    ) 
}}

with orders as (
    select * from {{ ref('stg_orders_ayahany') }}
),
order_items as (
    select * from {{ ref('stg_order_items_ayahany') }}
),
payments as (
    select * from {{ ref('stg_order_payments_ayahany') }}
),
reviews as (
    select * from {{ ref('stg_order_reviews_ayahany') }}
),
delivery as (
    select
        order_id,
        date_diff(
            cast(order_delivered_customer_date as date),
            cast(order_purchase_timestamp as date),
            day
        ) as delivery_days,
        date_diff(
            cast(order_estimated_delivery_date as date),
            cast(order_delivered_customer_date as date),
            day
        ) as delay_days,
        case
            when order_delivered_customer_date <= order_estimated_delivery_date then 'on_time'
            else 'delayed'
        end as delivery_status
    from {{ ref('stg_orders_ayahany') }}
    where order_delivered_customer_date is not null
)

select
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_status,

    -- Aggregated order item metrics
    sum(oi.price) as total_item_value,
    sum(oi.freight_value) as total_freight_value,
    count(oi.order_item_id) as total_items,
    count(distinct oi.product_id) as distinct_products,
    count(distinct oi.seller_id) as distinct_sellers,

    -- Payment
    max(p.payment_type) as primary_payment_type,
    sum(p.payment_value) as total_payment_value,

    -- Review
    avg(r.review_score) as avg_review_score,

    -- Delivery
    d.delivery_days,
    d.delay_days,
    d.delivery_status

from orders o
left join order_items oi using(order_id)
left join payments p using(order_id)
left join reviews r using(order_id)
left join delivery d using(order_id)
group by
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_status,
    d.delivery_days,
    d.delay_days,
    d.delivery_status