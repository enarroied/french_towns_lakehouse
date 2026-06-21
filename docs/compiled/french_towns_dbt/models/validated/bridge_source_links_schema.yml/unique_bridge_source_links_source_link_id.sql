
    
    

select
    source_link_id as unique_field,
    count(*) as n_records

from "french_towns"."main"."bridge_source_links"
where source_link_id is not null
group by source_link_id
having count(*) > 1


