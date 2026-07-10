
    
    

select
    name_fr as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_criminality_indicateur"
where name_fr is not null
group by name_fr
having count(*) > 1


