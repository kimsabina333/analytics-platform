WITH 
funnel  AS ({% include 'funnel.sql' %}),
firsts  AS ({% include 'firsts.sql' %}),
pm      AS ({% include 'payments.sql' %}),
app     AS ({% include 'app.sql' %})
SELECT DISTINCT
    app.customer_account_id,
    f.* EXCEPT(customer_account_id, date),
    app.date,
    IF(app.status='settled', 1, 0) AS status,
    pm.* EXCEPT(customer_account_id)
FROM app 
LEFT JOIN funnel f  
    ON f.customer_account_id = app.customer_account_id
    AND f.date <= app.date
LEFT JOIN pm        
    ON pm.customer_account_id = f.customer_account_id
ORDER BY customer_account_id, app.date