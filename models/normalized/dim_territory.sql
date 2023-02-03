with territory as (
    select * from {{ ref('sales_territory__curated') }}
),

final as (

    select
        territory.territory_code,
        territory.name_desc,
        territory.country_region_code,
        territory.group_country_desc
    from territory
)

select * from final