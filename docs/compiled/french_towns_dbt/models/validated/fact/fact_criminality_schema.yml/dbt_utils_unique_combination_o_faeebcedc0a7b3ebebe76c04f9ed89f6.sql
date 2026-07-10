





with validation_errors as (

    select
        commune_id, annee, indicateur_id
    from "french_towns"."main"."fact_criminality"
    group by commune_id, annee, indicateur_id
    having count(*) > 1

)

select *
from validation_errors


