-- =====================================================================
-- Replace the synthetic seed with the REAL Kaggle Olist data (~99k orders)
-- =====================================================================
-- Prerequisite: download https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
-- (free Kaggle login), unzip, then run  ./load_kaggle.sh <postgres-container> <unzipped-dir>
-- which copies the CSVs to /tmp/kaggle inside the container and runs this file.
-- Expects: olist_customers_dataset.csv, olist_sellers_dataset.csv,
--          olist_products_dataset.csv, olist_orders_dataset.csv,
--          olist_order_items_dataset.csv, olist_order_payments_dataset.csv
-- (geolocation / reviews / category-translation files are not used.)
\set ON_ERROR_STOP on
\timing on
BEGIN;
TRUNCATE customers, sellers, products, orders, order_items, order_payments RESTART IDENTITY CASCADE;

\copy customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) FROM '/tmp/kaggle/olist_customers_dataset.csv' WITH (FORMAT csv, HEADER true)
\copy sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state) FROM '/tmp/kaggle/olist_sellers_dataset.csv' WITH (FORMAT csv, HEADER true)
\copy products (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) FROM '/tmp/kaggle/olist_products_dataset.csv' WITH (FORMAT csv, HEADER true)
-- orders go through a staging table so that order_seq (identity) is assigned in purchase-time order
CREATE TEMP TABLE stg_orders (order_id varchar(32), customer_id varchar(32), order_status varchar(16),
    order_purchase_timestamp timestamp, order_approved_at timestamp, order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp, order_estimated_delivery_date timestamp);
\copy stg_orders FROM '/tmp/kaggle/olist_orders_dataset.csv' WITH (FORMAT csv, HEADER true)
INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
                    order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
SELECT * FROM stg_orders ORDER BY order_purchase_timestamp, order_id;
\copy order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) FROM '/tmp/kaggle/olist_order_items_dataset.csv' WITH (FORMAT csv, HEADER true)
\copy order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) FROM '/tmp/kaggle/olist_order_payments_dataset.csv' WITH (FORMAT csv, HEADER true)

-- Give updated_at a historical, meaningful value (COPY filled it with now()).
-- Setting it explicitly means the set_updated_at() trigger leaves it alone.
UPDATE orders SET updated_at = GREATEST(order_purchase_timestamp, order_approved_at,
                                        order_delivered_carrier_date, order_delivered_customer_date);
UPDATE customers c SET updated_at = o.order_purchase_timestamp
  FROM orders o WHERE o.customer_id = c.customer_id;
UPDATE order_items i SET updated_at = COALESCE(o.order_approved_at, o.order_purchase_timestamp)
  FROM orders o WHERE o.order_id = i.order_id;
UPDATE order_payments p SET updated_at = COALESCE(o.order_approved_at, o.order_purchase_timestamp)
  FROM orders o WHERE o.order_id = p.order_id;
UPDATE sellers  SET updated_at = timestamp '2016-09-01 00:00:00';
UPDATE products SET updated_at = timestamp '2016-09-01 00:00:00';
COMMIT;
ANALYZE;

SELECT 'customers' AS table_name, count(*) FROM customers
UNION ALL SELECT 'sellers', count(*) FROM sellers
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'order_items', count(*) FROM order_items
UNION ALL SELECT 'order_payments', count(*) FROM order_payments;
