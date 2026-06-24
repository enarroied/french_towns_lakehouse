





with validation_errors as (

    select
        monument_reference, commune_code
    from "french_towns"."main"."bridge_monument_communes"
    group by monument_reference, commune_code
    having count(*) > 1

)

select *
from validation_errors


