
    
    

with child as (
    select source_id as from_field
    from "french_towns"."main"."fact_equipment"
    where source_id is not null
),

parent as (
    select source_id as to_field
    from "french_towns"."main"."dim_source"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


