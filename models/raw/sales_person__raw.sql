select 
BUSINESSENTITYID
, TERRITORYID
, SALESQUOTA
, BONUS
, COMMISSIONPCT
, SALESYTD
, SALESLASTYEAR
, ROWGUID
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_person') }} 
where 1 = 1 
