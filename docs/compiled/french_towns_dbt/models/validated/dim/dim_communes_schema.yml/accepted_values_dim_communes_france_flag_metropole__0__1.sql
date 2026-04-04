
    
    

with all_values as (

    select
        flag_metropole as value_field,
        count(*) as n_records

    from "french_towns"."main"."dim_communes_france"
    group by flag_metropole

)

select *
from all_values
where value_field not in (
    '0','1'
)


