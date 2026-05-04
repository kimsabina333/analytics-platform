SELECT
    app.customer_account_id,
    CASE
        {% for offer_name, ids in mappings.offers.items() %}
        WHEN subscription_id IN ({{ ids | map('tojson') | join(', ') }}) THEN '{{ offer_name }}'
        {% endfor %}
        ELSE '1Week'
    END AS offer,
    channel,
    CASE
        {% for mid_name, ids in mappings.mid.items() %}
        WHEN mid IN ({% for id in ids %}'{{ id }}'{% if not loop.last %}, {% endif %}{% endfor %}) THEN '{{ mid_name }}'
        {% endfor %}
        ELSE mid
    END AS mid,
    mid as raw_mid,
    amount*exchange_rate/100 AS amount,
    rebill_count
FROM `payments.all_payments_prod` AS app

LEFT JOIN `analytics_draft.exchange_rate` fx
    ON fx.date = date(timestamp_micros(app.created_at)) AND fx.currency = app.currency

INNER JOIN funnel f 
    on f.customer_account_id = app.customer_account_id

WHERE 1=1
    AND status='settled'
    AND payment_type IN ('first', 'recurring')
    AND subscription_id NOT IN ('33', '34', '35')

QUALIFY ROW_NUMBER() OVER (PARTITION BY app.customer_account_id ORDER BY app.rebill_count DESC) = 1 