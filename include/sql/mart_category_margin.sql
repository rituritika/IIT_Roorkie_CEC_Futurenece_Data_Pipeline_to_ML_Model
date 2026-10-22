CREATE OR REPLACE TABLE mart_category_margin AS
SELECT COALESCE(p.category,'Uncategorized') category,
 SUM(o.net_revenue) net_revenue,
 SUM(o.net_revenue-COALESCE(p.cost_price,0)*o.quantity) gross_margin
FROM stg_orders o LEFT JOIN stg_products p USING(product_id)
WHERE o.dq_status='PASS' AND o.order_date >= DATE '2026-09-01'
 AND o.order_date < DATE '2026-10-01'
GROUP BY 1;
