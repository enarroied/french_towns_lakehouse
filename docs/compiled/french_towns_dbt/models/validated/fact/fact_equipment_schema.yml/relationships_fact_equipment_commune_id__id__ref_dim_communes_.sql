
    
    

with child as (
    select commune_id as from_field
    from "french_towns"."main"."fact_equipment"
    where commune_id is not null
),

parent as (
    select id as to_field
    from "french_towns"."main"."dim_communes"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


