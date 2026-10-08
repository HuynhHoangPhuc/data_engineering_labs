-- Custom SINGULAR test: for orders that have items, the sum of payments must equal
-- items + freight (within 5 cents). Returns the offending orders (test fails if any row).
-- The real Kaggle data has a handful of mismatches -> warn instead of error below 50 rows.
{{ config(severity='error', warn_if='>0', error_if='>50') }}

select order_id, items_value, freight_value, payment_value,
       payment_value - (items_value + freight_value) as diff
from {{ ref('fact_orders') }}
where item_count > 0
  and abs(payment_value - (items_value + freight_value)) > 0.05
