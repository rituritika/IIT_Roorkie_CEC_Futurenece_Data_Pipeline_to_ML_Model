CREATE OR REPLACE TABLE stg_payments AS
SELECT *, CASE
 WHEN payment_id IS NULL OR TRIM(payment_id)='' THEN 'FAIL'
 WHEN TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'FAIL'
 ELSE 'PASS' END AS dq_status,
 CASE WHEN payment_id IS NULL OR TRIM(payment_id)='' THEN 'Missing payment_id'
      WHEN TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'Invalid amount_paid'
      ELSE NULL END AS failure_reason,
 TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts_cast,
 TRY_CAST(amount_paid AS DOUBLE) AS amount_paid_cast,
 UPPER(TRIM(payment_mode)) AS payment_mode_std,
 UPPER(TRIM(payment_status)) AS payment_status_std
FROM raw_payments;
