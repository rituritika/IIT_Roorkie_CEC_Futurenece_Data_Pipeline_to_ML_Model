-- 1. Completeness
SELECT 'completeness_order_customer_id', 'completeness', 'Critical',
       COUNT(*),
       COUNT(*) FILTER (WHERE customer_id IS NULL OR TRIM(customer_id) = '')
FROM stg_orders

UNION ALL

-- 2. Uniqueness
SELECT 'uniqueness_order_id', 'uniqueness', 'Critical',
       COUNT(*),
       COUNT(*) - COUNT(DISTINCT order_id)
FROM stg_orders

UNION ALL

-- 3. Validity
SELECT 'validity_order_status', 'validity', 'Critical',
       COUNT(*),
       COUNT(*) FILTER (
           WHERE order_status IS NULL
              OR UPPER(TRIM(order_status)) NOT IN (
                  'PLACED', 'CONFIRMED', 'PREPARING',
                  'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED'
              )
       )
FROM stg_orders

UNION ALL

-- 4. Accuracy
SELECT 'accuracy_positive_quantity', 'accuracy', 'Critical',
       COUNT(*),
       COUNT(*) FILTER (WHERE TRY_CAST(quantity AS INTEGER) <= 0
                         OR TRY_CAST(quantity AS INTEGER) IS NULL)
FROM stg_orders

UNION ALL

-- 5. Consistency
SELECT 'consistency_customer_reference', 'consistency', 'Critical',
       COUNT(*),
       COUNT(*) FILTER (
           WHERE customer_id IS NOT NULL
             AND customer_id NOT IN (SELECT customer_id FROM stg_customers)
       )
FROM stg_orders

UNION ALL

-- 6. Timeliness
SELECT 'timeliness_no_future_orders', 'timeliness', 'Warning',
       COUNT(*),
       COUNT(*) FILTER (
           WHERE TRY_CAST(order_ts AS TIMESTAMP) > CURRENT_TIMESTAMP
       )
FROM stg_orders;