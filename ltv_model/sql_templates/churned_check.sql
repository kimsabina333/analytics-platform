SELECT 
    user_id AS customer_account_id,
FROM `events.funnel-raw-table` AS u
WHERE event_name = 'pr_funnel_subscription_canceled'