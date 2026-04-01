# Staging Models

This directory contains raw staging models that read directly from external sources (MinIO staging buckets in the future, local `input/` directory currently).

## Purpose

Staging models act as a thin transformation layer between raw source files and the validated layer. They:
- Select and rename columns to a standard naming convention
- Cast data types consistently
- Add metadata columns (source file, collection timestamp)
- Do NOT enforce data quality contracts (that happens in the validated layer)

## Future Migration

When the project migrates from local `input/` files to MinIO staging buckets, these models will be updated to read from:
- `s3://staging-current/` for current data
- `s3://staging-historical/` for historical data

## To Be Implemented

- [ ] `stg_communes.sql` — raw commune geometries
- [ ] `stg_departments.sql` — raw department reference
- [ ] `stg_arrondissements.sql` — raw arrondissement reference
- [ ] `stg_population.sql` — raw population data
- [ ] `stg_salaries.sql` — raw salary data
- [ ] `stg_labels.sql` — raw tourism label data from scrapers
