select 
TERRITORYID
, NAME
, COUNTRYREGIONCODE
, GROUPCOUNTRY
, SALESYTD
, SALESLASTYEAR
, COSTYTD
, COSTLASTYEAR
, ROWGUID
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_territory') }} 
where 1 = 1 
