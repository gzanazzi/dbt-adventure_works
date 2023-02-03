select 
SALESORDERID as sales_order_code
, SALESORDERDETAILID as sales_order_detail_code
, CARRIERTRACKINGNUMBER as carrier_tracking_num
, ORDERQTY as order_qty_num
, PRODUCTID as product_code
, SPECIALOFFERID as special_offer_code
, UNITPRICE as unit_price_num
, UNITPRICEDISCOUNT as unit_price_discount_num
, LINETOTAL as line_total_amount
from {{ ref('sales_order_detail__raw') }}