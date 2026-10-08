-- Grain: ONE ROW PER ORDER.
-- Measures come from two different child tables (items, payments) -> aggregate each to the
-- order grain FIRST (intermediate models), then join. Joining items and payments directly would
-- multiply rows ("fan-out") and double-count revenue.
with orders as (
    select * from {{ ref('stg_olist__orders') }}
),
items as (
    select * from {{ ref('int_order_items_summary') }}
),
payments as (
    select * from {{ ref('int_order_payments_summary') }}
)
select
    o.order_id,
    o.customer_id                                           as customer_key,
    to_char(o.purchased_at, 'YYYYMMDD')::int                as order_date_key,
    o.order_status,
    o.purchased_at,
    o.approved_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    coalesce(i.item_count, 0)                               as item_count,
    coalesce(i.seller_count, 0)                             as seller_count,
    coalesce(i.items_value, 0)                              as items_value,
    coalesce(i.freight_value, 0)                            as freight_value,
    coalesce(p.payment_value, 0)                            as payment_value,
    p.payment_count,
    p.main_payment_type,
    p.max_installments,
    case when o.delivered_to_customer_at is not null
         then extract(epoch from o.delivered_to_customer_at - o.purchased_at) / 86400.0
    end::numeric(8,2)                                       as delivery_days,
    case when o.delivered_to_customer_at is null then null
         else o.delivered_to_customer_at::date
              > o.estimated_delivery_at::date + {{ var('late_threshold_days') }}
    end                                                     as is_late
from orders o
left join items i    on i.order_id = o.order_id
left join payments p on p.order_id = o.order_id
