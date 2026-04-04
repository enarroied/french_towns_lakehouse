
    
    

with all_values as (

    select
        flag_chef_lieu_arrondissement as value_field,
        count(*) as n_records

    from "french_towns"."main"."dim_communes_france"
    group by flag_chef_lieu_arrondissement

)

select *
from all_values
where value_field not in (
    '0','1'
)


