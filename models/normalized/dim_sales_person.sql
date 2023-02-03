with person as (
    select * from {{ ref('sales_person__curated') }}
),

final as (

    select
        person.business_entity_code,
        person.territory_code
    from person
)

select * from final