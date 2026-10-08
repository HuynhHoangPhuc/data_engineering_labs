with source as (
    select * from {{ source('olist', 'orders') }}
)
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp                        as purchased_at,
    order_approved_at                               as approved_at,
    order_delivered_carrier_date                    as delivered_to_carrier_at,
    order_delivered_customer_date                   as delivered_to_customer_at,
    order_estimated_delivery_date                   as estimated_delivery_at,
    updated_at
from source
