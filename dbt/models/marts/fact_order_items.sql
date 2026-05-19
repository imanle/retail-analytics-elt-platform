with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_amount,
    oi.tax_amount,
    oi.line_total,

    -- cost & profit estimate based on product cost_price
    round(p.cost_price * oi.quantity, 2)                      as estimated_cost,
    round(oi.line_total - (p.cost_price * oi.quantity), 2)    as estimated_profit

from order_items oi
left join products p on oi.product_id = p.product_id
