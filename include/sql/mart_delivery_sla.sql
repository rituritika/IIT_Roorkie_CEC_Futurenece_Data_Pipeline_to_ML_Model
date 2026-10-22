CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.zone, COUNT(*) order_count,
 COUNT(*) FILTER(WHERE o.delivery_minutes<=15) delivered_within_15_min,
 ROUND(100.0*COUNT(*) FILTER(WHERE o.delivery_minutes<=15)/NULLIF(COUNT(*),0),2) within_15_min_pct
FROM stg_orders o LEFT JOIN stg_stores s USING(store_id)
WHERE o.dq_status='PASS' AND o.order_date >= DATE '2026-09-01'
 AND o.order_date < DATE '2026-10-01'
GROUP BY s.zone;
