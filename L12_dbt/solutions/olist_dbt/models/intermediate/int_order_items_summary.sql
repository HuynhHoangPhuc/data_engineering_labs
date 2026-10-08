-- One row per order: what was bought (items) - grain change from order line -> order
select
    order_id,
    count(*)                        as item_count,
    count(distinct seller_id)       as seller_count,
    sum(price)                      as items_value,
    sum(freight_value)              as freight_value
from {{ ref('stg_olist__order_items') }}
group by order_id
