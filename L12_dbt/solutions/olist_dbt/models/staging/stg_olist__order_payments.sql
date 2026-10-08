with source as (
    select * from {{ source('olist', 'order_payments') }}
)
select
    order_id || '-' || payment_sequential           as order_payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value::numeric(12,2)                    as payment_value,
    updated_at
from source
