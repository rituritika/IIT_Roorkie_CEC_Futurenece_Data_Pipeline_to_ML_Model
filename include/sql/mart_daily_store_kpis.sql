CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT o.order_date, s.store_id, s.store_name, s.zone,
 COUNT(*) order_count, SUM(o.net_revenue) net_revenue,
 AVG(o.delivery_minutes) avg_delivery_minutes
FROM stg_orders o LEFT JOIN stg_stores s USING(store_id)
WHERE o.dq_status='PASS' AND o.order_date >= DATE '2026-09-01'
 AND o.order_date < DATE '2026-10-01'
GROUP BY ALL;
