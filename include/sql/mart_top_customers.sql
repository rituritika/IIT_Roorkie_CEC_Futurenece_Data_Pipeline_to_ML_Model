CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.full_name, SUM(o.net_revenue) total_spend,
 COUNT(*) order_count
FROM stg_orders o LEFT JOIN stg_customers c USING(customer_id)
WHERE o.dq_status='PASS' AND o.order_date >= DATE '2026-09-01'
 AND o.order_date < DATE '2026-10-01'
GROUP BY o.customer_id,c.full_name
ORDER BY total_spend DESC LIMIT 10;
