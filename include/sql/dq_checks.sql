-- 1. Completeness: required customer ID
SELECT
    'completeness_order_customer_id' AS check_name,
    'completeness' AS dimension,
    'Critical' AS severity,
    COUNT(*) AS rows_checked,
    COUNT(*) FILTER (
        WHERE customer_id IS NULL OR TRIM(customer_id) = ''
    ) AS rows_failed
FROM stg_orders

UNION ALL

-- 2. Uniqueness: order ID
SELECT
    'uniqueness_order_id',
    'uniqueness',
    'Critical',
    COUNT(*),
    COUNT(*) - COUNT(DISTINCT order_id)
FROM stg_orders

UNION ALL

-- 3. Validity: allowed order status
SELECT
    'validity_order_status',
    'validity',
    'Critical',
    COUNT(*),
    COUNT(*) FILTER (
        WHERE order_status NOT IN (
            'PLACED', 'CONFIRMED', 'PREPARING',
            'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED'
        )
    )
FROM stg_orders

UNION ALL

-- 4. Accuracy: positive quantity
SELECT
    'accuracy_positive_quantity',
    'accuracy',
    'Critical',
    COUNT(*),
    COUNT(*) FILTER (WHERE quantity <= 0)
FROM stg_orders

UNION ALL

-- 5. Consistency: customer exists in master
SELECT
    'consistency_customer_reference',
    'consistency',
    'Critical',
    COUNT(*),
    COUNT(*) FILTER (
        WHERE customer_id IS NOT NULL
          AND customer_id NOT IN (
              SELECT customer_id FROM stg_customers
          )
    )
FROM stg_orders

UNION ALL

-- 6. Timeliness: no future order dates
SELECT
    'timeliness_no_future_orders',
    'timeliness',
    'Critical',
    COUNT(*),
    COUNT(*) FILTER (
        WHERE TRY_CAST(order_ts AS TIMESTAMP) > CURRENT_TIMESTAMP
    )
FROM stg_orders;