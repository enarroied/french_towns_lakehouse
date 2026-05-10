# Staging Models

See [root README](../README.md) for complete documentation.

## Purpose

Staging models read raw data from MinIO staging buckets and provide a thin transformation layer:

- Column selection and renaming to standard conventions
- Data type casting
- Metadata columns (source file, collection timestamp)

## Source Buckets

- `s3://staging-current/` — current automated/manual ingestion
- `s3://staging-historical/` — historical reconstruction data

## Models

See [dbt documentation](https://enarroied.github.io/french_towns_lakehouse/docs/) for full lineage.
