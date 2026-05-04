SELECT
    app.customer_account_id,
    app.status,
    app.decline_message,
    DATE(TIMESTAMP_MICROS(created_at)) AS date
FROM `hopeful-list-429812-f3.payments.all_payments_prod` app
WHERE app.payment_type = 'recurring' 
AND paid_count=1 
AND retry_count=0
AND TIMESTAMP_MICROS(created_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ mappings.interval_days }} DAY)
