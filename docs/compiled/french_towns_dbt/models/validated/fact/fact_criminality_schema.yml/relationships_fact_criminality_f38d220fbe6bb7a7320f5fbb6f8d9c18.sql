
    
    

with child as (
    select indicateur_id as from_field
    from "french_towns"."main"."fact_criminality"
    where indicateur_id is not null
),

parent as (
    select indicateur_id as to_field
    from "french_towns"."main"."dim_criminality_indicateur"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


