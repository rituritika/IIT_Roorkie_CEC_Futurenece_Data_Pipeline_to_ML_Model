CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.full_name, SUM(o.net_revenue) total_spend,
 COUNT(*) order_count
FROM stg_orders o
JOIN stg_customers c USING(customer_id)
WHERE o.dq_status = 'PASS'
  AND o.order_date >= DATE '2026-09-01'
  AND o.order_date < DATE '2026-10-01'
  AND o.customer_id IS NOT NULL
  AND TRIM(o.customer_id) <> ''
  AND c.full_name IS NOT NULL
  AND TRIM(c.full_name) <> ''
GROUP BY o.customer_id, c.full_name
ORDER BY total_spend DESC
LIMIT 10;
