
    
    

with all_values as (

    select
        territory_type as value_field,
        count(*) as n_records

    from "french_towns"."main"."dim_communes"
    group by territory_type

)

select *
from all_values
where value_field not in (
    'METROPOLE_DROM','COM'
)


