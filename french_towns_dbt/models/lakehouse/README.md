# LakeHouse Models

See [root README](../../README.md) for complete documentation.

## Purpose

LakeHouse models implement the final dimensional model with historization:

- **SCD Type 2** for dimensions: `valid_from`, `valid_to`, `is_current`
- **Surrogate keys** using `dbt_utils.generate_surrogate_key`
- **Append-only INSERT** for facts with duplicate detection

## Naming Convention

```
lh_{dimension/fact}_{entity_name}
```

Examples:
- `lh_dim_commune` — commune dimension with SCD Type 2
- `lh_fact_population` — population fact (append-only)

## To Be Implemented (M2-M4)

- [ ] `lh_dim_commune` — SCD Type 2 commune dimension
- [ ] `lh_dim_department` — SCD Type 2 department dimension
- [ ] `lh_dim_region` — SCD Type 2 region dimension
- [ ] `lh_dim_date` — date dimension
- [ ] `lh_fact_population` — append-only population facts
- [ ] `lh_fact_salaries` — append-only salary facts
- [ ] `lh_bridge_commune_lineage` — commune merge/split tracking

See [dbt documentation](https://enarroied.github.io/french_towns_lakehouse/docs/) for full lineage.
