# Validated Models

This directory contains the validated (schema-enforced) models that implement the star schema. These are the final step before LakeHouse integration.

## Structure

```
validated/
├── dim/              # Dimension models
├── fact/             # Fact models
└── bridge_*.sql      # Bridge tables
```

## Purpose

Validated models:
- Enforce data contracts (not-null, uniqueness, referential integrity)
- Build the star schema structure
- Are NOT historized (historization happens in the LakeHouse layer)
- Output to local `data/processed/` as Parquet files (current V1 behavior)

## Data Contracts

All validated models include dbt schema tests:
- Not-null constraints on primary keys
- Uniqueness constraints on natural and surrogate keys
- Foreign key relationships where applicable
- Accepted value ranges for enumerations

## Models

### Dimensions
- `dim_communes_france` — all French communes with geometry and hierarchy
- `dim_zip_codes` — ZIP code to commune mapping

### Facts
- `fact_population` — historical population per commune
- `fact_salaries` — mean annual salary by sex per commune (2023)

### Bridges
- `bridge_communes_zip_codes` — many-to-many commune/ZIP code relationships
