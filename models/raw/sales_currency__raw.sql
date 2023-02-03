select 
CURRENCYCODE
, NAME
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_currency') }} 
where 1 = 1 
