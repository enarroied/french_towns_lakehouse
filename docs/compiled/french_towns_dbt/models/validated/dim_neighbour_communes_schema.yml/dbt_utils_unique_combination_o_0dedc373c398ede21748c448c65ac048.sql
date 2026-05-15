





with validation_errors as (

    select
        parcel_id, neighbor_id
    from "french_towns"."main"."dim_neighbour_communes"
    group by parcel_id, neighbor_id
    having count(*) > 1

)

select *
from validation_errors


