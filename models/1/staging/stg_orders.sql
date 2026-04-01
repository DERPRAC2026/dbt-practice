with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

staged as (

    select
        order_id                                    as order_id,
        customer_id                                 as customer_id,
        cast(ordered_at as timestamp)               as ordered_at,
        coalesce(cast(order_amount as float), 0)    as order_amount,
        lower(status)                               as status

    from source
    where order_id is not null

)

select * from staged