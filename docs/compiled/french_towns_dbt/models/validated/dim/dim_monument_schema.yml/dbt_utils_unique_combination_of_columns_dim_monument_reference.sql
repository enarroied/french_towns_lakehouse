





with validation_errors as (

    select
        reference
    from "french_towns"."main"."dim_monument"
    group by reference
    having count(*) > 1

)

select *
from validation_errors


