# LakeHouse Models

This directory contains the final LakeHouse models that implement SCD Type 2 historization for dimensions and append-only patterns for facts.

## Purpose

LakeHouse models are the final output layer, implementing:
- **SCD Type 2** for dimensions: `valid_from`, `valid_to`, `is_current` columns
- **Surrogate keys** using `dbt_utils.generate_surrogate_key`
- **Append-only INSERT** for fact tables with duplicate detection

## Naming Convention

LakeHouse models follow the pattern:
```
{lh_}{dimension/fact}_{entity_name}
```

Examples:
- `lh_dim_commune.sql` — commune dimension with SCD Type 2
- `lh_dim_department.sql` — department dimension with SCD Type 2
- `lh_fact_population.sql` — population fact (append-only)
- `lh_fact_salaries.sql` — salary fact (append-only)

## SCD Type 2 Implementation

Dimensions in the LakeHouse include:
```sql
valid_from DATE,
valid_to DATE,
is_current BOOLEAN,
-- plus deterministic surrogate key
surrogate_key CHAR(32)
```

## To Be Implemented (M2-M4)

- [ ] `lh_dim_commune.sql` — SCD Type 2 commune dimension
- [ ] `lh_dim_department.sql` — SCD Type 2 department dimension
- [ ] `lh_dim_region.sql` — SCD Type 2 region dimension
- [ ] `lh_dim_date.sql` — date dimension
- [ ] `lh_fact_population.sql` — append-only population facts
- [ ] `lh_fact_salaries.sql` — append-only salary facts
- [ ] `lh_bridge_commune_lineage.sql` — commune merge/split tracking
