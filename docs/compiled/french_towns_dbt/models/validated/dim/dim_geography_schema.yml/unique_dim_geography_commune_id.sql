
    
    

select
    commune_id as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_geography"
where commune_id is not null
group by commune_id
having count(*) > 1


