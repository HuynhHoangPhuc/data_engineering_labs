-- L00 exercise (b) - SQL on the Olist OLTP database - STARTER (B1, B2 are done as examples; write B3-B7)
--   docker compose exec postgres psql -U olist -f /sql/queries.sql        (b_sql/ is mounted at /sql)
-- Solution: solutions/b_sql.sql
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
-- TODO: order_items JOIN products; per product_category_name: items_sold, avg_price (2 decimals), revenue = sum(price);
--       keep categories with more than 300 items (HAVING); order by revenue DESC.  Expected: 6 rows.

\echo '== B4. LEFT JOIN anti-join: orders without any item (per status), and products never sold'
-- TODO: orders LEFT JOIN order_items ... WHERE oi.order_id IS NULL, count per order_status.
--       Then count the products that were never sold, this time with NOT EXISTS (SELECT 1 FROM order_items ...).

\echo '== B5. CTE + window LAG: monthly revenue of 2018 and month-over-month growth'
-- TODO: CTE 1 order_revenue (one row per order: sum(payment_value)); CTE 2 monthly (date_trunc('month', ...),
--       orders, revenue) for purchases in 2018; final SELECT with growth_pct using lag(revenue) OVER (ORDER BY month).
--       Why must payments be summed per order BEFORE the join?

\echo '== B6. window RANK: top 2 sellers by revenue in each of the 3 biggest seller states'
-- TODO: revenue per (seller_state, seller_id); rank() OVER (PARTITION BY seller_state ORDER BY revenue DESC);
--       keep rnk <= 2 for the 3 states with the most sellers.

\echo '== B7. window running total: cumulative revenue per day, first week of March 2018'
-- TODO: daily revenue (orders JOIN order_payments) for 2018-03-01 .. 2018-03-07, then
--       sum(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) and a 3-day moving average.
