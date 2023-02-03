with customer as (
    select * from {{ ref('sales_customer__curated') }}
),

final as (

    select
        customer.customer_code,
        customer.territory_code,
        customer.account_num
    from customer
)

select * from final