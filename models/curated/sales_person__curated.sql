select 
BUSINESSENTITYID as business_entity_code
, TERRITORYID as territory_code
, SALESQUOTA as sales_quota_num
, BONUS as bonus_num
, COMMISSIONPCT as commission_pct_num
, SALESYTD as sales_ytd_num
, SALESLASTYEAR as sales_last_year_num
from {{ ref('sales_person__raw') }}