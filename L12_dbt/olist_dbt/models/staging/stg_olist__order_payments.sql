-- TODO (step 4): staging model for order_payments
--   * key: order_id || '-' || payment_sequential   as order_payment_key
--   * cast payment_value to numeric(12,2)
--   * keep order_id, payment_sequential, payment_type, payment_installments, updated_at
select 1 as todo
