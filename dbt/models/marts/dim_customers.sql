with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_stats as (
    select
        customer_id,
        min(order_date)                                        as first_order_date,
        max(order_date)                                        as last_order_date,
        count(*)                                               as total_orders,
        sum(total_amount)                                      as total_revenue
    from orders
    where order_status = 'completed'
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.country,
    c.city,
    c.created_at                                               as customer_created_at,
    o.first_order_date,
    o.last_order_date,
    coalesce(o.total_orders, 0)                                as total_orders,
    coalesce(o.total_revenue, 0)                               as total_revenue,
    case
        when o.customer_id is null                             then 'inactive'
        when o.total_orders = 1                                then 'new'
        when o.total_revenue >= 2000                           then 'high_value'
        else 'returning'
    end                                                        as customer_segment

from customers c
left join order_stats o on c.customer_id = o.customer_id
