with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

shipments as (
    select * from {{ ref('stg_shipments') }}
),

order_items_agg as (
    select
        order_id,
        count(*)                                               as total_items,
        sum(line_total)                                        as items_total
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.currency,
    o.total_amount,
    o.created_at,

    -- payment info
    p.payment_method,
    p.payment_status,
    coalesce(p.payment_amount, 0)                              as payment_amount,

    -- shipment info
    s.carrier,
    s.shipping_method,
    coalesce(s.shipping_cost, 0)                              as shipping_cost,
    s.shipment_status,

    -- items
    coalesce(i.total_items, 0)                                as total_items,

    -- flags
    (o.order_status = 'completed')                            as is_completed,
    (o.order_status = 'cancelled')                            as is_cancelled,
    (o.order_status = 'refunded')                             as is_refunded

from orders o
left join payments p      on o.order_id = p.order_id
left join shipments s     on o.order_id = s.order_id
left join order_items_agg i on o.order_id = i.order_id
