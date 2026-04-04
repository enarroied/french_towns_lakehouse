

with meet_condition as(
  select *
  from "french_towns"."main"."fact_salaries"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not mean_salary_management_position_women >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not mean_salary_management_position_women <= 300000
)

select *
from validation_errors

