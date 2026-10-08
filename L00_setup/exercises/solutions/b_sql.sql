-- L00 exercise (b) - SQL on the Olist OLTP database - SOLUTION
-- Run:
--   docker compose exec -T postgres psql -U olist < solutions/b_sql.sql
\pset footer off
\timing off

\echo '== B1. rows per table (UNION ALL)'
SELECT 'customers' AS table_name, count(*) AS row_count FROM customers
UNION ALL SELECT 'sellers', count(*) FROM sellers
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'order_items', count(*) FROM order_items
UNION ALL SELECT 'order_payments', count(*) FROM order_payments;

\echo '== B2. JOIN + GROUP BY: delivered orders per customer state, top 5'
SELECT c.customer_state, count(*) AS delivered_orders
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY delivered_orders DESC
LIMIT 5;

\echo '== B3. GROUP BY + HAVING: categories with more than 300 items sold, by revenue'
SELECT p.product_category_name,
       count(*)                    AS items_sold,
       round(avg(oi.price), 2)     AS avg_price,
       sum(oi.price)               AS revenue
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_category_name
HAVING count(*) > 300
ORDER BY revenue DESC;

\echo '== B4. LEFT JOIN anti-join: orders without any item (per status), and products never sold'
SELECT o.order_status, count(*) AS orders_without_items
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.order_id
WHERE oi.order_id IS NULL
GROUP BY o.order_status
ORDER BY 2 DESC;

SELECT count(*) AS products_never_sold
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.product_id);   -- same idea with NOT EXISTS

\echo '== B5. CTE + window LAG: monthly revenue of 2018 and month-over-month growth'
WITH order_revenue AS (            -- step 1: one row per order (payments can have several rows!)
    SELECT order_id, sum(payment_value) AS revenue
    FROM order_payments
    GROUP BY order_id
),
monthly AS (                       -- step 2: one row per month
    SELECT date_trunc('month', o.order_purchase_timestamp)::date AS month,
           count(*)          AS orders,
           sum(r.revenue)    AS revenue
    FROM orders o
    JOIN order_revenue r ON r.order_id = o.order_id
    WHERE o.order_purchase_timestamp >= '2018-01-01' AND o.order_purchase_timestamp < '2019-01-01'
    GROUP BY 1
)
SELECT month, orders, revenue,
       round(100.0 * (revenue - lag(revenue) OVER (ORDER BY month)) / lag(revenue) OVER (ORDER BY month), 1)
           AS growth_pct
FROM monthly
ORDER BY month;

\echo '== B6. window RANK: top 2 sellers by revenue in each of the 3 biggest seller states'
WITH seller_revenue AS (
    SELECT s.seller_state, s.seller_id, sum(oi.price) AS revenue
    FROM order_items oi
    JOIN sellers s ON s.seller_id = oi.seller_id
    GROUP BY s.seller_state, s.seller_id
),
ranked AS (
    SELECT seller_state, left(seller_id, 8) AS seller, revenue,
           rank() OVER (PARTITION BY seller_state ORDER BY revenue DESC) AS rnk,
           count(*) OVER (PARTITION BY seller_state)                     AS sellers_in_state
    FROM seller_revenue
)
SELECT * FROM ranked
WHERE rnk <= 2 AND seller_state IN (SELECT seller_state FROM sellers GROUP BY 1 ORDER BY count(*) DESC LIMIT 3)
ORDER BY sellers_in_state DESC, rnk;

\echo '== B7. window running total: cumulative revenue per day, first week of March 2018'
WITH daily AS (
    SELECT o.order_purchase_timestamp::date AS day, sum(op.payment_value) AS revenue
    FROM orders o JOIN order_payments op ON op.order_id = o.order_id
    WHERE o.order_purchase_timestamp >= '2018-03-01' AND o.order_purchase_timestamp < '2018-03-08'
    GROUP BY 1
)
SELECT day, revenue,
       sum(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       round(avg(revenue) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3d
FROM daily
ORDER BY day;
