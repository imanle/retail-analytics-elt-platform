with source as (
    select * from raw.shipments
),

cleaned as (
    select
        shipment_id,
        order_id,
        initcap(trim(carrier))                                 as carrier,
        lower(trim(shipping_method))                           as shipping_method,
        shipping_cost::numeric(12, 2)                          as shipping_cost,
        shipped_at::timestamp                                  as shipped_at,
        case
            when delivered_at is not null
             and trim(delivered_at) not in ('', 'None', 'null', 'NaT')
            then delivered_at::timestamp
            else null
        end                                                    as delivered_at,
        lower(trim(shipment_status))                           as shipment_status,
        _source_file_name,
        _loaded_at::timestamp                                  as _loaded_at,

        row_number() over (
            partition by shipment_id
            order by _loaded_at desc
        ) as _row_num

    from source
    where shipment_id is not null
)

select
    shipment_id,
    order_id,
    carrier,
    shipping_method,
    shipping_cost,
    shipped_at,
    delivered_at,
    shipment_status,
    case
        when delivered_at is not null
        then extract(epoch from (delivered_at - shipped_at)) / 86400.0
        else null
    end                                                        as delivery_days,
    _source_file_name,
    _loaded_at

from cleaned
where _row_num = 1
