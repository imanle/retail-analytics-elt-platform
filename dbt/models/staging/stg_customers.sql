with source as (
    select * from raw.customers
),

cleaned as (
    select
        customer_id,
        initcap(trim(first_name))                              as first_name,
        initcap(trim(last_name))                               as last_name,
        lower(trim(email))                                     as email,
        initcap(trim(country))                                 as country,
        initcap(trim(city))                                    as city,
        created_at::timestamp                                  as created_at,
        _source_file_name,
        _loaded_at::timestamp                                  as _loaded_at,

        row_number() over (
            partition by customer_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where customer_id is not null
)

select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name                             as full_name,
    email,
    country,
    city,
    created_at,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
