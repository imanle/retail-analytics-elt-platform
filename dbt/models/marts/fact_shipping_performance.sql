with shipments as (
    select * from {{ ref('stg_shipments') }}
),

orders as (
    select order_id, order_date, order_status
    from {{ ref('stg_orders') }}
)

select
    s.shipment_id,
    s.order_id,
    o.order_date,
    s.carrier,
    s.shipping_method,
    s.shipping_cost,
    s.shipped_at,
    s.delivered_at,
    s.shipment_status,
    s.delivery_days,

    -- a shipment is late if it took more than the expected days by method
    case
        when s.shipping_method = 'overnight'  and s.delivery_days > 1   then true
        when s.shipping_method = 'express'    and s.delivery_days > 3   then true
        when s.shipping_method = 'standard'   and s.delivery_days > 7   then true
        else false
    end                                                        as is_late

from shipments s
left join orders o on s.order_id = o.order_id
