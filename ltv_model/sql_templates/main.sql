WITH 
churned AS ({% include 'churned.sql' %}),
funnel  AS ({% include 'funnel.sql' %}),
upsell  AS ({% include 'upsell.sql' %}),
pm      AS ({% include 'payments.sql' %}),
app     AS ({% include 'app.sql' %})

{% if task_type != 'fast' %}
    , session_time AS ({% include 'session_time.sql' %})
{% endif %}

SELECT DISTINCT
    app.* EXCEPT(rebill_count),
    app.rebill_count AS paid_count,
    f.* EXCEPT(customer_account_id),
    pm.* EXCEPT(customer_account_id),
    IF(upsell.customer_account_id IS NULL, 0, 1) AS has_upsell,
    COALESCE(upsell.upsell_amount, 0) AS upsell_amount,
    IF(churned.customer_account_id IS NULL, 0, 1) AS churned,
    {% if task_type != 'fast' %}
    session_time.total_time_3d_min AS session_time_3d_min
    {% endif %}
    

FROM app
LEFT JOIN funnel f  
    ON f.customer_account_id = app.customer_account_id

LEFT JOIN churned   
    ON churned.customer_account_id = app.customer_account_id

LEFT JOIN pm        
    ON pm.customer_account_id = app.customer_account_id

LEFT JOIN upsell   
    ON upsell.customer_account_id = app.customer_account_id

{% if task_type != 'fast' %}
LEFT JOIN session_time
    ON session_time.customer_account_id = app.customer_account_id
{% endif %}


WHERE app.rebill_count IS NOT NULL
  AND app.channel IN ({{ mappings.channels | map('tojson') | join(', ') }})

  {% if task_type != 'fast' %}
  AND TIMESTAMP_ADD(f.subscription_cohort_date, INTERVAL 72 HOUR) <= CURRENT_TIMESTAMP()
  {% endif %}

  {% if not is_training %}
  AND app.customer_account_id NOT IN 
  (
    SELECT customer_account_id  
    {% if task_type != 'fast' %}
     FROM `analytics_draft.ltv_ml_approach`
    {% else %}
     FROM `analytics_draft.ltv_ml_fast`
    {% endif %}
    )
  {% endif %}

ORDER BY customer_account_id, subscription_cohort_date