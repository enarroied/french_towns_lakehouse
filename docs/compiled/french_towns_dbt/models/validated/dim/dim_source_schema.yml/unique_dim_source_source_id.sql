
    
    

select
    source_id as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_source"
where source_id is not null
group by source_id
having count(*) > 1


