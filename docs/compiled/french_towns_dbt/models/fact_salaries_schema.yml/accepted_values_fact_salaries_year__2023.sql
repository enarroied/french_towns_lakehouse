
    
    

with all_values as (

    select
        year as value_field,
        count(*) as n_records

    from "french_towns"."main"."fact_salaries"
    group by year

)

select *
from all_values
where value_field not in (
    '2023'
)


