-- Grain: ONE ROW PER ORDER LINE (order_id, order_item_id).
-- This is the fact that connects to dim_products and dim_sellers.
select
    oi.order_item_key,
    oi.order_id,
    oi.order_item_id,
    o.customer_id                                   as customer_key,
    oi.product_id                                   as product_key,
    oi.seller_id                                    as seller_key,
    to_char(o.purchased_at, 'YYYYMMDD')::int        as order_date_key,
    o.order_status,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                     as line_total
from {{ ref('stg_olist__order_items') }} oi
join {{ ref('stg_olist__orders') }} o on o.order_id = oi.order_id
