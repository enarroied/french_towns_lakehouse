
    
    

select
    date as unique_field,
    count(*) as n_records

from "french_towns"."main"."dim_calendar"
where date is not null
group by date
having count(*) > 1


