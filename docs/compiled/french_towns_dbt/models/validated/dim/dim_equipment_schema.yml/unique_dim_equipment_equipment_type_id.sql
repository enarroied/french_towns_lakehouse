
    
    

select
    equipment_type_id as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_equipment"
where equipment_type_id is not null
group by equipment_type_id
having count(*) > 1


