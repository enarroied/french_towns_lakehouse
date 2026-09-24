





with validation_errors as (

    select
        target_table, target_key, source_id
    from "french_towns"."main"."bridge_source_links"
    group by target_table, target_key, source_id
    having count(*) > 1

)

select *
from validation_errors


