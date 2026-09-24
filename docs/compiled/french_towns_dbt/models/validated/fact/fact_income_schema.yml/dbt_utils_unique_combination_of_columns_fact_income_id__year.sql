





with validation_errors as (

    select
        id, year
    from "french_towns"."main"."fact_income"
    group by id, year
    having count(*) > 1

)

select *
from validation_errors


