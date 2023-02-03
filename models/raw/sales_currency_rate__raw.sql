select 
CURRENCYRATEID
, CURRENCYRATEDATE
, FROMCURRENCYCODE
, TOCURRENCYCODE
, AVERAGERATE
, ENDOFDAYRATE
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_currency_rate') }} 
where 1 = 1 
