CREATE OR REPLACE TABLE stg_payments AS
WITH deduped AS (
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY TRIM(payment_id)
                   ORDER BY _loaded_at DESC NULLS LAST,
                            _run_id DESC NULLS LAST
               ) AS rn
        FROM raw_payments
        WHERE payment_id IS NOT NULL
          AND TRIM(payment_id) <> ''
    )
    WHERE rn = 1

    UNION ALL

    SELECT *
    FROM raw_payments
    WHERE payment_id IS NULL
       OR TRIM(payment_id) = ''
)
SELECT *,
    CASE
        WHEN payment_id IS NULL OR TRIM(payment_id) = '' THEN 'FAIL'
        WHEN TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'FAIL'
        ELSE 'PASS'
    END AS dq_status,
    CASE
        WHEN payment_id IS NULL OR TRIM(payment_id) = '' THEN 'Missing payment_id'
        WHEN TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'Invalid amount_paid'
        ELSE NULL
    END AS failure_reason,
    TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts_cast,
    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid_cast,
    UPPER(TRIM(payment_mode)) AS payment_mode_std,
    UPPER(TRIM(payment_status)) AS payment_status_std
FROM deduped;
