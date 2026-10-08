TODO (step 7): add a SINGULAR test `assert_payments_match_order_value.sql` here:
a SELECT over ref('fact_orders') returning orders WITH items whose payment_value differs from
items_value + freight_value by more than 0.05. Configure it to warn if > 0 rows and error if > 50 rows.
