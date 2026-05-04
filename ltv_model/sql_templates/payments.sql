SELECT 
    DISTINCT
    app.customer_account_id,
    app.payment_method,
    app.geo_country,
    app.amount/100*exchange_rate as first_amount,
    CASE 
        WHEN geo_country IN ({{ mappings.t1_countries | map('tojson') | join(', ') }}) THEN 'T1'
        ELSE 'WW' 
    end as geo,
    issuing_bank,
    card_type,
    CASE 
        WHEN lower(card_brand) IN ({{ mappings.card_brand | map('lower') | map('tojson') | join(', ') }}) THEN lower(card_brand)
        ELSE 'other' 
    END AS card_brand
from `payments.all_payments_prod` AS app

LEFT JOIN `analytics_draft.exchange_rate` fx 
    on fx.date = date(timestamp_micros(app.created_at)) AND fx.currency = app.currency

INNER JOIN funnel f 
    ON app.customer_account_id = f.customer_account_id
WHERE status = 'settled' AND payment_type = 'first'