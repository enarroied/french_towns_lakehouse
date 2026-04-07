# Integration Pipelines

Integration pipelines are responsible for moving validated data from the validated layer into the LakeHouse using DuckDB and dbt.

## Responsibilities

- **Idempotent:** Running a pipeline multiple times produces the same result.
- **No rejections at this stage:** All data quality checks happen in the validation stage.
- **Dimensions:** Loaded using either full reload or delta UPSERT, with SCD Type 2 row management.
- **Facts:** Loaded using append-only INSERT with duplicate detection.

## Pipeline Naming Convention

Integration pipelines follow the naming pattern:
```
{functionality}_{timing}_{subject_type}_{domain}
```

Examples:
- `integration_current_dim_geography` — integrate current geography dimensions
- `integration_current_fact_demographics` — integrate current demographics facts
- `integration_historical_dim_communes` — integrate historical commune dimension rows

## To Be Implemented

- [ ] `integration_current_dim_geography.py` — SCD Type 2 for dim_commune, dim_department, dim_region
- [ ] `integration_current_fact_demographics.py` — Append-only INSERT for fact_population, fact_salaries
- [ ] `integration_current_labels.py` — Append-only INSERT for tourism label facts
- [ ] Historical integration pipelines (M4 milestone)

## Failure Handling

Integration pipelines are **strict**: before writing to the LakeHouse, an integration pipeline asserts that the validated layer is in an expected state (non-empty and within acceptable freshness window). If upstream validation failed or was skipped, the integration pipeline refuses to run and records the blockage in the audit table.
