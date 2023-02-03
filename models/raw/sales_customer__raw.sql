select 
CUSTOMERID
, PERSONID
, STOREID
, TERRITORYID
, ACCOUNTNUMBER
, ROWGUID
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_customer') }} 
where 1 = 1 
