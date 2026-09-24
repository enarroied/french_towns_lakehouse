





with validation_errors as (

    select
        commune_code, year
    from "french_towns"."main"."fact_unemployment"
    group by commune_code, year
    having count(*) > 1

)

select *
from validation_errors


