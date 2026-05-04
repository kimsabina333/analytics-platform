SELECT
    DISTINCT
    t.user_id AS customer_account_id, 
    
    CASE 
        {% for utm_source, utm_name in mappings.utm_sources.items() %}
        WHEN json_value(event_metadata, '$.utm_source') IN ({{ utm_name | map('tojson') | join(', ') }}) THEN '{{ utm_source }}'
        {% endfor %}
        ELSE 'other'    
    END AS utm_source,
    t.user_agent AS device,

    IF(json_value(event_metadata, '$.age')!='', json_value(event_metadata, '$.age'), 'None') AS age,
    IF(json_value(event_metadata, '$.gender')!='', json_value(event_metadata, '$.gender'), 'None') AS gender,
    
    DATE(t.timestamp) as date

FROM `events.funnel-raw-table` t
WHERE t.event_name = 'pr_funnel_paywall_purchase_click' 
AND t.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ mappings.interval_days }} DAY)