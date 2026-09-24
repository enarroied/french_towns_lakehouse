





with validation_errors as (

    select
        commune_id, year, equipment_type_id
    from "french_towns"."main"."fact_equipment"
    group by commune_id, year, equipment_type_id
    having count(*) > 1

)

select *
from validation_errors


