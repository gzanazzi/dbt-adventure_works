select 
TERRITORYID as territory_code
, NAME as name_desc
, COUNTRYREGIONCODE as country_region_code
, GROUPCOUNTRY as group_country_desc
, SALESYTD as sales_year_amount
, SALESLASTYEAR as sales_last_year_amount
, COSTYTD as cost_year_amount
, COSTLASTYEAR as cost_last_year_amount
from {{ ref('sales_territory__raw') }}