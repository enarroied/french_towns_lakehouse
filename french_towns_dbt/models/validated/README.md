# Validated Models

See [root README](../../README.md) for complete documentation.

## Purpose

Validated models implement the schema-enforced star schema:

- Data contracts via dbt schema tests (not-null, uniqueness, referential integrity)
- Star schema structure (dimensions + facts)
- Output to `data/processed/` as Parquet files

## Structure

```
validated/
├── dim/              # Dimension models
├── fact/             # Fact models
└── bridge_*.sql     # Bridge tables
```

## Models

| Type | Name | Description |
|------|------|-------------|
| Dimension | `dim_communes_france` | All French communes with geometry and hierarchy |
| Dimension | `dim_zip_codes` | ZIP code to commune mapping |
| Fact | `fact_population` | Historical population per commune |
| Fact | `fact_salaries` | Mean annual salary by sex (2023) |
| Bridge | `bridge_communes_zip_codes` | Many-to-many commune/ZIP relationships |

See [dbt documentation](https://enarroied.github.io/french_towns_lakehouse/docs/) for full lineage.
