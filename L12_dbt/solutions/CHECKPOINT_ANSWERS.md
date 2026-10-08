# L12 - Checkpoint answers

1. `fact_orders`: one row per **order** (order-level measures: payment value, delivery days, lateness).
   `fact_order_items`: one row per **order line** (product- and seller-level measures: price, freight). Product and seller
   are attributes of a line, not of an order (an order can contain several products from several sellers), so questions
   like "revenue by category/seller" need the line grain, while "late orders by state" or "payments by type" need the order grain.

2. `orders` 1:N `order_items` and `orders` 1:N `order_payments`. Joining both directly produces items x payments rows per
   order (fan-out), so an order with 2 items and 3 payments appears 6 times: `sum(price)` is tripled and `sum(payment_value)`
   doubled. Aggregating each child to the order grain first (intermediate models) makes every join 1:1.

3. Staging = thin renames -> views are free to create and always current. Intermediate = building blocks only used by other
   models -> ephemeral CTEs avoid clutter in the database. Marts are queried by BI tools many times -> tables (precomputed,
   fast). Make a mart **incremental** when a full rebuild becomes too slow/expensive (large facts with append/update patterns),
   processing only new/changed rows (`is_incremental()` + `unique_key`).

4. `ref()` resolves the right schema/database for the current target (dev vs prod), and - most importantly - tells dbt about
   the **dependency**, so dbt builds models in the right order, can select `+model`/`model+`, skips children of failed
   parents, and draws lineage in the docs.

5. `customer_unique_id` identifies the person. For a person-level dimension, build `dim_shoppers` grouped by
   `customer_unique_id` (e.g. latest city/state, first order date, number of orders), and add `customer_unique_id` to the
   fact (or map `customer_id -> shopper_key` in an intermediate model).

6. Not necessarily: customers only change when someone registers, so a 7-day threshold is wrong for that table, while
   orders must be fresh. Configure freshness **per table** (e.g. `error_after: 30 days` or `freshness: null` for customers,
   products, sellers) and use a loader timestamp (`_loaded_at`) rather than a business timestamp when possible.

7. A **generic** test is a parameterized macro (`{% test name(model, column_name, ...) %}`) applied to many
   models/columns from YAML (`positive_value`). A **singular** test is a one-off SQL file returning failing rows.
   "Payments = items + freight" involves several columns of one specific model with a tolerance, so a singular test
   (`assert_payments_match_order_value.sql`) is the natural fit.

8. It keeps **history** of a mutable source table (slowly changing dimension type 2): every time a customer row changes,
   the old version is closed and a new one inserted. dbt adds `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`,
   `dbt_valid_to` (NULL = current version). Facts can then be joined to the version valid at the order date.
