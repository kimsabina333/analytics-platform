SELECT 
    user_id AS customer_account_id,
    timestamp unsub_timestamp
FROM `events.app-raw-table` AS u

INNER JOIN funnel f
 ON f.customer_account_id = u.user_id
WHERE event_name = 'pr_webapp_unsubscribed'