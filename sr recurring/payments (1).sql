SELECT DISTINCT
    p.customer_account_id,
    CASE WHEN p.payment_type='recurring' AND channel='solidgate'
         THEN f.payment_method
         ELSE p.payment_method
    END AS payment_method,
    issuing_bank,
    p.card_type,
    CASE
        WHEN mid IN ('d4d7b345-bf19-453a-acdc-8ea68a5d4c44','01KMFGBBWW8RDNQJV20QPM8MMN') THEN 'adyen US'
        WHEN mid IN ('de185c91-6045-4190-babc-42558400cb92', 'ed141bc8-5fe6-40bd-b501-9d9d3c678689') THEN 'esquire'
        ELSE mid
    END AS mid,
    CASE 
        WHEN geo_country IN ('US', 'GB', 'CA', 'AU', 'DE', 'FR') THEN geo_country
        WHEN geo_country IN ('AE', 'AT', 'AU', 'BH', 'BN', 'CA', 'CZ', 'DE', 'DK', 'ES', 'FI', 'FR',
              'GB', 'HK', 'IE', 'IL', 'IT', 'JP', 'KR', 'NL', 'NO', 'PT', 'QA', 'SA',
              'SE', 'SG', 'SI', 'US', 'NZ') THEN 'T1'
        ELSE 'WW' 
    END AS geo,
    CASE
        WHEN subscription_id IN ('2', '12', '15', '18', '21', '24', '27', '30') THEN '1Week'
        WHEN subscription_id IN ('3', '13', '16', '19', '22', '25', '28', '31') THEN '4Week'
        WHEN subscription_id IN ('4', '14', '17', '20', '23', '26', '29', '32') THEN '12Week'
        WHEN subscription_id IN ('33') THEN '1Month'
        WHEN subscription_id IN ('34') THEN '3Month'
        WHEN subscription_id IN ('35') THEN '1Year'
        ELSE '1Week'
    END AS offer,
    CASE 
        WHEN lower(card_brand) IN ('mastercard', 'visa', 'amex', 'maestro', 'discover') THEN lower(card_brand)
        ELSE 'other' 
    END AS card_brand
FROM `payments.all_payments_prod` p
LEFT JOIN firsts f ON f.customer_account_id = p.customer_account_id
WHERE p.payment_type = 'recurring' 
AND paid_count=1 
AND retry_count=0