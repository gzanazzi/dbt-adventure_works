select 
CURRENCYRATEID as currency_rate_code
, CURRENCYRATEDATE as currency_rate_date
, FROMCURRENCYCODE as from_currency_code
, TOCURRENCYCODE as to_currency_code
, AVERAGERATE as average_rate_num
, ENDOFDAYRATE as end_of_day_rate_num
from {{ ref('sales_currency_rate__raw') }}