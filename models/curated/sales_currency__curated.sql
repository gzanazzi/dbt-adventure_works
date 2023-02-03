select 
CURRENCYCODE as currency_code
, NAME as name_desc
from {{ ref('sales_currency__raw') }}