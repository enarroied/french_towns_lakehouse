





with validation_errors as (

    select
        commune_id, zip_code_id
    from "french_towns"."main"."bridge_communes_zip_codes"
    group by commune_id, zip_code_id
    having count(*) > 1

)

select *
from validation_errors


