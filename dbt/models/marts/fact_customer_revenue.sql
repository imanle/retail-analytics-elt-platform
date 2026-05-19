with orders as (
    select * from {{ ref('fact_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
)

select
    o.customer_id,
    c.full_name,
    c.email,
    c.country,
    c.city,
    c.customer_segment,
    count(*)                                                   as total_orders,
    count(*) filter (where o.is_completed)                     as completed_orders,
    sum(o.total_amount) filter (where o.is_completed)          as total_revenue,
    round(
        avg(o.total_amount) filter (where o.is_completed), 2
    )                                                          as avg_order_value,
    min(o.order_date)                                          as first_order_date,
    max(o.order_date)                                          as last_order_date

from orders o
left join customers c on o.customer_id = c.customer_id
group by
    o.customer_id,
    c.full_name,
    c.email,
    c.country,
    c.city,
    c.customer_segment
