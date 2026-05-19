with source as (
    select * from raw.order_items
),

cleaned as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity::integer                                       as quantity,
        unit_price::numeric(12, 2)                             as unit_price,
        discount_amount::numeric(12, 2)                        as discount_amount,
        tax_amount::numeric(12, 2)                             as tax_amount,
        line_total::numeric(12, 2)                             as line_total,
        _source_file_name,
        _loaded_at::timestamp                                  as _loaded_at,

        row_number() over (
            partition by order_item_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where order_item_id is not null
)

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    tax_amount,
    line_total,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
