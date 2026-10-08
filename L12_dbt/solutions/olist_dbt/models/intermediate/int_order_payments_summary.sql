-- One row per order: how it was paid
select
    order_id,
    count(*)                                            as payment_count,
    sum(payment_value)                                  as payment_value,
    max(payment_installments)                           as max_installments,
    -- the payment type that covered most of the value
    (array_agg(payment_type order by payment_value desc))[1] as main_payment_type
from {{ ref('stg_olist__order_payments') }}
group by order_id
