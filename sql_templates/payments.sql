SELECT 
    DISTINCT
    customer_account_id, 
    payment_method,
    issuing_bank,
    card_type,

    CASE
        {% for mid_name, id_list in mappings.mid.items() %}
        WHEN mid IN (
            {% for id in id_list %}
            '{{ id }}'{{ "," if not loop.last }}
            {% endfor %}
        ) THEN '{{ mid_name }}'
        {% endfor %}
        ELSE mid
    END AS mid,

    CASE 
        WHEN geo_country IN ({{ mappings.t1_countries | map('tojson') | join(', ') }}) THEN geo_country
        WHEN geo_country IN ("AE", "AT", "AU", "BH", "BN", "CA", "CZ", "DE", "DK", "ES", "FI", "FR",
              "GB", "HK", "IE", "IL", "IT", "JP", "KR", "NL", "NO", "PT", "QA", "SA",
              "SE", "SG", "SI", "US", "NZ") THEN 'T1'
        ELSE 'WW' 
    end as geo,

    CASE
        {% for offer_name, ids in mappings.offers.items() %}
        WHEN subscription_id IN ({{ ids | map('tojson') | join(', ') }}) THEN '{{ offer_name }}'
        {% endfor %}
        ELSE '1Week'
    END AS offer,

    CASE 
        WHEN lower(card_brand) IN ({{ mappings.card_brand | map('lower') | map('tojson') | join(', ') }}) THEN lower(card_brand)
        ELSE 'other' 
    END AS card_brand
    
FROM `payments.all_payments_prod` app
WHERE payment_type = 'first'