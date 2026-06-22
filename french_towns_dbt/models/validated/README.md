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
| Dimension | `dim_communes` | Administrative dimension of all French communes including COM territories |
| Dimension | `dim_geography` | Spatial boundaries and geometry of all French communes |
| Dimension | `dim_zip_codes` | ZIP code to commune mapping |
| Dimension | `dim_source` | Data source provenance (19 tracked sources) |
| Dimension | `dim_equipment` | Equipment type code-to-name mapping |
| Fact | `fact_population` | Historical population per commune |
| Fact | `fact_salaries` | Mean annual salary by sex (2023) |
| Fact | `fact_equipment` | Equipment/facility counts per commune |
| Bridge | `bridge_communes_zip_codes` | Many-to-many commune/ZIP relationships |
| Bridge | `bridge_source_links` | Row-level source provenance for all models |

## Source Tracking

Every model links to `dim_source` via `bridge_source_links`, a row-level provenance system:

- Source metadata lives in `config.yaml` (`downloads:`, `scrapers:`, `custom_parsers:`)
- `dim_source` is generated from YAML via `data_sources/dim_source/generate_sources.py`
- A single `bridge_source_links` master view (UNION ALL of 9 branches using `bridge_source_links_simple` / `bridge_source_links_mapped` macros) links each row to its source(s)

**Multi-source models:** `fact_population` (5 sources), `dim_communes` (2 sources), `dim_labels` (7 sources). All others have 1 source each.

### Usage

**Join a fact row to its source(s):**

```sql
SELECT f.id, f.year, f.population, s.source_name, s.organization, s.source_label
FROM gold.fact_population f
JOIN gold.bridge_source_links b
  ON b.target_table = 'fact_population'
  AND b.target_key = f.id || '|' || f.year
JOIN gold.dim_source s ON b.source_id = s.source_id
WHERE f.id = '01001'
ORDER BY f.year;
```

| id | year | population | source_name | organization | source_label |
|---|---|---|---|---|---|
| 01001 | 1968 | 347 | historical_population | INSEE | Populations historiques |
| 01001 | 1968 | 347 | family | INSEE | Famille |
| 01001 | 1968 | 347 | migration | INSEE | Immigration |
| 01001 | 1968 | 347 | births | INSEE | Naissances |
| 01001 | 1968 | 347 | deaths | INSEE | Décès |

Each `fact_population` row is linked to **all 5 sources** that contribute to its columns
(population, family, migration, births, deaths).

**Table-level sumary by source:**

```sql
SELECT s.source_name, s.organization, COUNT(*) AS rows_tracked
FROM gold.bridge_source_links b
JOIN gold.dim_source s ON b.source_id = s.source_id
GROUP BY s.source_name, s.organization
ORDER BY rows_tracked DESC;
```

**Multi-source tables:**

```sql
SELECT b.target_table, COUNT(DISTINCT b.source_id) AS source_count,
       STRING_AGG(DISTINCT s.source_name, ', ' ORDER BY s.source_name) AS sources
FROM gold.bridge_source_links b
JOIN gold.dim_source s ON b.source_id = s.source_id
GROUP BY b.target_table
ORDER BY source_count DESC;
```

**Single-row lookup (equipment):**

```sql
SELECT e.commune_id, e.year, e.count, s.source_name, s.source_label
FROM gold.fact_equipment e
JOIN gold.bridge_source_links b
  ON b.target_table = 'fact_equipment'
  AND b.target_key = e.commune_id || '|' || e.year || '|' || e.equipment_type_id
JOIN gold.dim_source s ON b.source_id = s.source_id
WHERE e.commune_id = '01001' AND e.year = 2022
LIMIT 5;
```

See [dbt documentation](https://enarroied.github.io/french_towns_lakehouse/docs/) for full lineage.
