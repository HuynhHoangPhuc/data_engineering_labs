with source as (
    select * from {{ source('olist', 'order_items') }}
)
select
    order_id || '-' || order_item_id                as order_item_key,   -- single-column key for tests
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date                             as shipping_limit_at,
    price::numeric(12,2)                            as price,
    freight_value::numeric(12,2)                    as freight_value,
    updated_at
from source
