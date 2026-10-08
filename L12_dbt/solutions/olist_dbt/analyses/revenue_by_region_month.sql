-- dbt compile turns this into plain SQL in target/compiled/... (analyses are never materialized)
select d.year_month, c.customer_region, count(*) as orders, sum(f.payment_value) as revenue
from {{ ref('fact_orders') }} f
join {{ ref('dim_customers') }} c on c.customer_key = f.customer_key
join {{ ref('dim_date') }} d      on d.date_key = f.order_date_key
where f.order_status = 'delivered'
group by 1, 2
order by 1, 2
