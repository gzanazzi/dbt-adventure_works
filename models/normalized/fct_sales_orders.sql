with sales_order_header as (
    select * from {{ ref('sales_order_header__curated') }}
),

sales_order_detail as (
    select * from {{ ref('sales_order_detail__curated') }}
),

sales_customer as (
    select * 
    from {{ ref('dim_customer') }} as sc
    left join {{ ref('dim_territory') }} as st on sc.territory_code = st.territory_code
),

sales_person as (
    select * 
    from {{ ref('dim_sales_person') }} as sp
    left join {{ ref('dim_territory') }} as st on sp.territory_code = st.territory_code
),

final as (

    select 
        soh.sales_order_code,
        soh.order_date,
        soh.due_date,
        soh.ship_date,
        soh.status_num,
        soh.online_order_flag_num,
        soh.customer_code,
        soh.sales_person_code,
        soh.territory_code,
        soh.currency_rate_code,
        soh.sub_total_amount,
        soh.tax_amt_num,
        soh.freight_num,
        soh.total_due_amount,
        sod.sales_order_detail_code,
        sod.carrier_tracking_num,
        sod.order_qty_num,
        sod.product_code,
        sod.special_offer_code,
        sod.unit_price_num,
        sod.unit_price_discount_num,
        sod.line_total_amount,
        sc.name_desc as customer_territory_desc,
        sc.country_region_code as customer_country_region_code,
        sp.name_desc as person_territory_desc,
        sp.country_region_code as person_country_region_code
    from sales_order_header as soh
    left join sales_order_detail as sod using (sales_order_code)
    left join sales_customer as sc using (customer_code)
    left join sales_person as sp on soh.sales_person_code = sp.business_entity_code

)

select * from final