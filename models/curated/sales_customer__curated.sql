select 
CUSTOMERID as customer_code
, PERSONID as person_code
, STOREID as store_code
, TERRITORYID as territory_code
, ACCOUNTNUMBER as account_num
from {{ ref('sales_customer__raw') }}