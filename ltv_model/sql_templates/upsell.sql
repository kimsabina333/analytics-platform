select 
    distinct
    app.customer_account_id, 
    sum(amount/100 * exchange_rate) as upsell_amount

from `payments.all_payments_prod` as app

left join `analytics_draft.exchange_rate` as fx
    on date(timestamp_micros(app.created_at)) = fx.date and app.currency = fx.currency

right join funnel f
    on f.customer_account_id = app.customer_account_id

where 1=1
and status='settled'
and payment_type = 'upsell'

group by app.customer_account_id