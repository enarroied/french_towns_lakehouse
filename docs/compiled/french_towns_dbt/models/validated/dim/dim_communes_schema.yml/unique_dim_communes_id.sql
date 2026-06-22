
    
    

select
    id as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_communes"
where id is not null
group by id
having count(*) > 1


