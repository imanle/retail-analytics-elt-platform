with orders as (
    select * from {{ ref('fact_orders') }}
)

select
    order_date,
    count(*)                                                   as total_orders,
    count(*) filter (where is_completed)                       as completed_orders,
    count(*) filter (where is_cancelled)                       as cancelled_orders,
    count(*) filter (where is_refunded)                        as refunded_orders,
    sum(total_amount) filter (where is_completed)              as total_revenue,
    sum(total_items)                                           as total_items_sold,
    round(
        avg(total_amount) filter (where is_completed), 2
    )                                                          as average_order_value,
    sum(shipping_cost)                                         as total_shipping_cost

from orders
group by order_date
order by order_date
