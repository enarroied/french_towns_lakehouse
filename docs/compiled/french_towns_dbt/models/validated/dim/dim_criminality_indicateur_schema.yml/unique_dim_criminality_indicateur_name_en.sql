
    
    

select
    name_en as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_criminality_indicateur"
where name_en is not null
group by name_en
having count(*) > 1


