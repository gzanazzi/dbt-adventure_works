select 
SALESORDERID
, SALESORDERDETAILID
, CARRIERTRACKINGNUMBER
, ORDERQTY
, PRODUCTID
, SPECIALOFFERID
, UNITPRICE
, UNITPRICEDISCOUNT
, LINETOTAL
, ROWGUID
, MODIFIEDDATE
from {{ source('dbt_adventure_works', 'seed_sales_order_detail') }} 
where 1 = 1 
