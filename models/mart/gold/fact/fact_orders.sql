WITH order_payments AS (
    SELECT
        order_id,
        SUM(amount) AS total_payment_amount,
        MAX(installments) AS max_installments,
        COUNT(DISTINCT payment_method) AS payment_method_count
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

order_items AS (
    SELECT
        order_id,
        COUNT(*) AS items_count,
        SUM(price) AS total_items_amount,
        SUM(shipping_fee) AS total_shipping_fee
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        delivery_phase,
        purchase_at,
        approved_at,
        shipped_at,
        delivered_at,
        estimated_delivery_at,

        DATE_PART('day', shipped_at - approved_at) AS shipping_duration_days,
        DATE_PART('day', delivered_at - shipped_at) AS delivery_duration_days,
        DATE_PART('day', delivered_at - estimated_delivery_at) AS delay_days
    FROM {{ ref('stg_orders') }}
)

SELECT
    o.order_id,
    o.customer_id,

    o.order_status,
    o.delivery_phase,

    o.purchase_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,

    o.shipping_duration_days,
    o.delivery_duration_days,
    o.delay_days,

    oi.items_count,
    oi.total_items_amount,
    oi.total_shipping_fee,
    (COALESCE(oi.total_items_amount, 0) + COALESCE(oi.total_shipping_fee, 0)) AS total_amount,

    op.total_payment_amount,
    op.max_installments,
    op.payment_method_count,

    (o.delivered_at IS NOT NULL) AS is_delivered,
    (o.delay_days > 0) AS is_late_delivery

FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN order_payments op ON o.order_id = op.order_id