with source as (
    select * from raw.products
),

cleaned as (
    select
        product_id,
        trim(product_name)                                     as product_name,
        initcap(trim(category))                                as category,
        initcap(trim(subcategory))                             as subcategory,
        initcap(trim(brand))                                   as brand,
        cost_price::numeric(12, 2)                             as cost_price,
        sale_price::numeric(12, 2)                             as sale_price,
        case
            when lower(trim(is_active)) in ('true', '1', 'yes') then true
            else false
        end                                                    as is_active,
        _source_file_name,
        _loaded_at::timestamp                                  as _loaded_at,

        row_number() over (
            partition by product_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where product_id is not null
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
    round(
        (sale_price - cost_price) / nullif(sale_price, 0) * 100,
        2
    )                                                          as gross_margin_pct,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
