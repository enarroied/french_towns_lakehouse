

with meet_condition as(
  select *
  from (select * from "french_towns"."main"."dim_geography" where area_km2 IS NOT NULL) dbt_subquery
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not area_km2 >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not area_km2 <= 10000000
)

select *
from validation_errors

