





with validation_errors as (

    select
        monument_reference, event_date, new_protection_level
    from "french_towns"."main"."fact_classification_history"
    group by monument_reference, event_date, new_protection_level
    having count(*) > 1

)

select *
from validation_errors


