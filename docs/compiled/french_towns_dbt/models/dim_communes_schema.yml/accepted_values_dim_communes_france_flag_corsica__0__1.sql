
    
    

with all_values as (

    select
        flag_corsica as value_field,
        count(*) as n_records

    from "french_towns"."main"."dim_communes_france"
    group by flag_corsica

)

select *
from all_values
where value_field not in (
    '0','1'
)


