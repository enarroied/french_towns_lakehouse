





with validation_errors as (

    select
        commune_id
    from "french_towns"."main"."dim_commune_monument_stats"
    group by commune_id
    having count(*) > 1

)

select *
from validation_errors


