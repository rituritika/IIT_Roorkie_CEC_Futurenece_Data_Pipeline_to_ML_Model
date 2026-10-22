CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT o.order_id, o.order_amount, p.paid_amount,
 CASE WHEN p.order_id IS NULL THEN 'No payment'
      WHEN ABS(o.order_amount-p.paid_amount)>0.01 THEN 'Amount mismatch'
      ELSE 'Payment status not successful' END reason
FROM stg_orders o LEFT JOIN (
 SELECT order_id, SUM(amount_paid_cast) paid_amount,
        BOOL_OR(payment_status_std='SUCCESS') success
 FROM stg_payments GROUP BY order_id
) p USING(order_id)
WHERE o.dq_status='PASS' AND
 (p.order_id IS NULL OR ABS(o.order_amount-p.paid_amount)>0.01 OR NOT COALESCE(p.success,FALSE));
