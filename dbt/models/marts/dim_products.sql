with products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    cost_price,
    sale_price,
    is_active,
    gross_margin_pct

from products
