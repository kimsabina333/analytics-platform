SELECT 
    DISTINCT
    user_id customer_account_id,
    timestamp subscription_cohort_date,
    CASE 

        {% for utm_source, utm_name in mappings.utm_sources.items() %}
        WHEN json_value(event_metadata, '$.utm_source') IN ({{ utm_name | map('tojson') | join(', ') }}) THEN '{{ utm_source }}'
        {% endfor %}
        ELSE 'other'    

    END AS utm_source,
    IF(json_value(event_metadata, '$.gender')!='', json_value(event_metadata, '$.gender'), 'None') gender,
    IF(json_value(event_metadata, '$.age')!='', json_value(event_metadata, '$.age'), 'None')  age,
    CASE
    {% for device, reg in mappings.devices.items() %}
    WHEN REGEXP_CONTAINS(JSON_VALUE(event_metadata, '$.user_agent'), r'{{ reg }}') THEN '{{ device }}'
    {% endfor %}
    ELSE 'Unknown'
    END AS device
    --IF(json_value(event_metadata, '$.utm_keyword')!='', json_value(event_metadata, '$.utm_keyword'),'None') utm_keyword
from `events.funnel-raw-table`
where event_name = 'pr_funnel_subscribe'
  AND {{ time_filter }}
  AND json_value(event_metadata, '$.subscription') IN ({{ mappings.allowed_subs | map('tojson') | join(', ') }})