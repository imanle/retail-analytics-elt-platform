with source as (
    select * from raw.payments
),

cleaned as (
    select
        payment_id,
        order_id,
        lower(trim(payment_method))                            as payment_method,
        lower(trim(payment_status))                            as payment_status,
        payment_amount::numeric(12, 2)                         as payment_amount,
        paid_at::timestamp                                     as paid_at,
        _source_file_name,
        _loaded_at::timestamp                                  as _loaded_at,

        row_number() over (
            partition by payment_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where payment_id is not null
)

select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    payment_amount,
    paid_at,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
