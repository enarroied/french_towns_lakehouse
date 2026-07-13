
    
    

with all_values as (

    select
        methodology_version as value_field,
        count(*) as n_records

    from "french_towns"."main"."fact_income"
    group by methodology_version

)

select *
from all_values
where value_field not in (
    'filosofi_1','filosofi_2'
)


