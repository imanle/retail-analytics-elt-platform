with source as (
    select * from raw.orders
),

cleaned as (
    select
        order_id,
        customer_id,
        order_date::date                                        as order_date,
        lower(trim(order_status))                               as order_status,
        upper(trim(currency))                                   as currency,
        total_amount::numeric(12, 2)                            as total_amount,
        created_at::timestamp                                   as created_at,
        updated_at::timestamp                                   as updated_at,
        _source_file_name,
        _loaded_at::timestamp                                   as _loaded_at,

        row_number() over (
            partition by order_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where order_id is not null
)

select
    order_id,
    customer_id,
    order_date,
    order_status,
    currency,
    total_amount,
    created_at,
    updated_at,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
