select 
SALESORDERID as sales_order_code
, ORDERDATE as order_date
, DUEDATE as due_date
, SHIPDATE as ship_date
, STATUS as status_num
, ONLINEORDERFLAG as online_order_flag_num
, CUSTOMERID as customer_code
, SALESPERSONID as sales_person_code
, TERRITORYID as territory_code
, CURRENCYRATEID as currency_rate_code
, SUBTOTAL as sub_total_amount
, TAXAMT as tax_amt_num
, FREIGHT as freight_num
, TOTALDUE as total_due_amount
from {{ ref('sales_order_header__raw') }}