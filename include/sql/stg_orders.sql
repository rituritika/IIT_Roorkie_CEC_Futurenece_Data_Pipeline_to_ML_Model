CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT CAST(NULL AS VARCHAR) order_id, CAST(NULL AS TIMESTAMP) order_ts,
 CAST(NULL AS VARCHAR) customer_id, CAST(NULL AS VARCHAR) store_id,
 CAST(NULL AS VARCHAR) product_id, CAST(NULL AS INTEGER) quantity,
 CAST(NULL AS DOUBLE) unit_price, CAST(NULL AS DOUBLE) discount_pct,
 CAST(NULL AS DOUBLE) order_amount, CAST(NULL AS VARCHAR) payment_mode,
 CAST(NULL AS VARCHAR) order_status, CAST(NULL AS VARCHAR) channel,
 CAST(NULL AS INTEGER) delivery_minutes, CAST(NULL AS DATE) order_date,
 CAST(NULL AS DOUBLE) net_revenue, CAST(NULL AS DOUBLE) gross_margin,
 CAST(NULL AS BOOLEAN) within_15_min, CAST(NULL AS VARCHAR) dq_status,
 CAST(NULL AS VARCHAR) failure_reason;
CREATE OR REPLACE TEMP TABLE incoming_orders AS
SELECT TRIM(order_id) order_id, TRY_CAST(order_ts AS TIMESTAMP) order_ts,
 TRIM(customer_id) customer_id, TRIM(store_id) store_id, TRIM(product_id) product_id,
 TRY_CAST(quantity AS INTEGER) quantity, TRY_CAST(unit_price AS DOUBLE) unit_price,
 TRY_CAST(discount_pct AS DOUBLE) discount_pct, TRY_CAST(order_amount AS DOUBLE) order_amount,
 UPPER(TRIM(payment_mode)) payment_mode, UPPER(TRIM(order_status)) order_status,
 TRIM(channel) channel, TRY_CAST(delivery_minutes AS INTEGER) delivery_minutes,
 TRY_CAST(order_ts AS DATE) order_date,
 TRY_CAST(order_amount AS DOUBLE) net_revenue,
 CAST(NULL AS DOUBLE) gross_margin,
 TRY_CAST(delivery_minutes AS INTEGER) <= 15 within_15_min,
 CASE WHEN COALESCE(TRIM(order_id),'')='' THEN 'FAIL'
      WHEN TRY_CAST(order_ts AS TIMESTAMP) IS NULL THEN 'FAIL'
      WHEN TRY_CAST(quantity AS INTEGER) IS NULL OR TRY_CAST(quantity AS INTEGER)<=0 THEN 'FAIL'
      WHEN TRY_CAST(order_amount AS DOUBLE) IS NULL OR TRY_CAST(order_amount AS DOUBLE)<0 THEN 'FAIL'
      ELSE 'PASS' END dq_status,
 CASE WHEN COALESCE(TRIM(order_id),'')='' THEN 'Missing order_id'
      WHEN TRY_CAST(order_ts AS TIMESTAMP) IS NULL THEN 'Invalid order_ts'
      WHEN TRY_CAST(quantity AS INTEGER) IS NULL OR TRY_CAST(quantity AS INTEGER)<=0 THEN 'Invalid quantity'
      WHEN TRY_CAST(order_amount AS DOUBLE) IS NULL OR TRY_CAST(order_amount AS DOUBLE)<0 THEN 'Invalid order_amount'
      ELSE NULL END failure_reason
FROM raw_orders
QUALIFY ROW_NUMBER() OVER(PARTITION BY TRIM(order_id) ORDER BY _loaded_at DESC)=1;
MERGE INTO stg_orders t USING incoming_orders s ON t.order_id=s.order_id
WHEN MATCHED THEN UPDATE SET
 order_ts=s.order_ts, customer_id=s.customer_id, store_id=s.store_id,
 product_id=s.product_id, quantity=s.quantity, unit_price=s.unit_price,
 discount_pct=s.discount_pct, order_amount=s.order_amount,
 payment_mode=s.payment_mode, order_status=s.order_status, channel=s.channel,
 delivery_minutes=s.delivery_minutes, order_date=s.order_date,
 net_revenue=s.net_revenue, within_15_min=s.within_15_min,
 dq_status=s.dq_status, failure_reason=s.failure_reason
WHEN NOT MATCHED THEN INSERT BY NAME;
UPDATE stg_orders o
SET gross_margin = o.net_revenue -
    COALESCE(p.cost_price, 0) * COALESCE(o.quantity, 0)
FROM stg_products p
WHERE o.product_id = p.product_id
  AND o.gross_margin IS NULL;
