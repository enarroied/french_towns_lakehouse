





with validation_errors as (

    select
        indicateur_id
    from "french_towns"."main"."dim_criminality_indicateur"
    group by indicateur_id
    having count(*) > 1

)

select *
from validation_errors


