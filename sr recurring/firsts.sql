SELECT
    customer_account_id,
    payment_method,
    card_type
FROM `payments.all_payments_prod`
WHERE status = 'settled'
AND payment_type = 'first'
AND channel = 'solidgate'
AND payment_method NOT IN ('paypal', 'paypal-vault')