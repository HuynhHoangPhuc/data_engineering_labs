-- TODO (step 5): one row per order_id from stg_olist__order_payments with
--   payment_count, payment_value (sum), max_installments, main_payment_type
--   Hint (Postgres): (array_agg(payment_type order by payment_value desc))[1]
select 1 as todo
