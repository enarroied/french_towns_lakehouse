# French Towns LakeHouse

A self-hosted data lakehouse of French municipal data. The pipeline downloads open government datasets, transforms them into clean Parquet files using DuckDB and dbt, and uploads the results to MinIO object storage.

**dbt model documentation:** [https://enarroied.github.io/french_towns_lakehouse/](https://enarroied.github.io/french_towns_lakehouse/)

---

## Quick Links

| Topic | Location |
|-------|----------|
| [Project Structure](#project-structure) | Directory layout |
| [Naming Conventions](#naming-conventions) | Pipelines, buckets, models |
| [Pipeline Architecture](#pipeline-architecture) | Staging → Transformation → Integration |
| [Setup & Running](#setup) | Prerequisites, installation, execution |
| [Deployment](#prefect-deployments) | Deploying flows to Prefect |
| [Full Specifications](private/Specifications/specifications.md) | Detailed design document |

---

## What does this build?

Three analytics-ready tables available as Parquet files:

- `dim_communes_france` — all French communes with geometry, administrative hierarchy, spatial metrics, and flags
- `fact_population` — historical population per commune by census year
- `fact_salaries` — mean annual salary by sex per commune (2023)

All tables join on the INSEE commune code (`id`, `CHAR(5)`).

---

## Technology Stack

| Layer | Tool |
|-------|------|
| Orchestration | Prefect 3 |
| Downloading | httpx (async) |
| Transformation | dbt-core + dbt-duckdb |
| Compute | DuckDB |
| Object Storage | MinIO (S3-compatible) |
| Package Manager | uv |

---

## Setup

```bash
git clone https://github.com/enarroied/french_towns_lakehouse.git
cd french_towns_lakehouse
uv sync

# Install dbt packages
cd french_towns_dbt && dbt deps && cd ..
```

Create `.env`:
```bash
MINIO_ENDPOINT=http://localhost:19000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

Start MinIO:
```bash
docker compose up -d
```

---

## Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   STAGING       │───▶│  TRANSFORMATION  │───▶│   INTEGRATION   │
│                 │    │                  │    │   (future)      │
│  MinIO buckets: │    │  dbt models:     │    │                 │
│  staging-*      │    │  validated/      │    │  LakeHouse:     │
└─────────────────┘    └──────────────────┘    │  lakehouse/     │
                                               └─────────────────┘
```

### MinIO Bucket Structure

| Bucket | Purpose |
|--------|---------|
| `staging-current` | Raw files from automated ingestion |
| `staging-historical` | Raw files from historical reconstruction |
| `validated` | Parquet files from validation stage |
| `validated-historical` | Parquet files from historical validation |
| `rejected` | Records rejected during validation |
| `evidence-archive` | Staging files archived after integration |
| `lakehouse` | Final dimensional model (Apache Iceberg) |

### dbt Model Layers

| Layer | Purpose |
|-------|---------|
| `staging/` | Raw source landing (reads from MinIO) |
| `validated/` | Schema-enforced star schema |
| `lakehouse/` | SCD Type 2 historization |

---

## Naming Conventions

### Pipeline Naming

```
{functionality}_{timing}_{subject_type}_{domain}
```

| Dimension | Options | Description |
|-----------|---------|-------------|
| `functionality` | `staging`, `transformation`, `integration` | Pipeline stage |
| `timing` | `current`, `historical` | Data time horizon |
| `subject_type` | `dim`, `fact` | Type (transformation/integration only) |
| `domain` | `geography`, `demographics`, `labels` | Thematic area |

Examples:
- `staging_current_geography` — download geography source files
- `transformation_current_dim_geography` — build geography dimensions
- `integration_current_fact_demographics` — load demographics facts

### Domain Categories

| Domain | Data Sources |
|--------|--------------|
| `geography` | Communes, departments, regions, arrondissements |
| `demographics` | Population, salaries, census data |
| `labels` | Tourism labels (Villages Fleuris, Petites Cités de Caractère, etc.) |

---

## Running the Pipeline

### Run All Flows (for testing)

```bash
uv run python -m flows.french_towns_pipeline
```

### Run Individual Flows

```bash
uv run python -m flows.staging.staging_current_geography
uv run python -m flows.transformation.transformation_current_dim_geography
```

### Run dbt in Isolation

```bash
cd french_towns_dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## Prefect Deployments

Deploy flows to Prefect for scheduled execution:

```bash
./scripts/deploy_flows.sh
```

View deployments at `http://localhost:4200/deployments`.

Each flow can be deployed independently and scheduled with cron or interval schedules.

---

## Project Structure

```
french_towns_lakehouse/
├── flows/                           # Prefect orchestration
│   ├── shared/                      # Shared utilities
│   │   ├── config.py                # Config loader
│   │   ├── minio.py                 # MinIO helpers + sidecar generation
│   │   ├── download.py               # Async download utilities
│   │   └── dbt.py                   # dbt runner utilities
│   ├── staging/                     # Staging pipelines
│   │   ├── staging_current_geography.py
│   │   ├── staging_current_demographics.py
│   │   └── staging_current_labels.py
│   └── transformation/               # Transformation pipelines
│       ├── transformation_current_dim_geography.py
│       ├── transformation_current_fact_demographics.py
│       └── transformation_current_labels.py
├── french_towns_dbt/                # dbt project
│   └── models/
│       ├── staging/                 # Raw staging models
│       ├── validated/               # Schema-enforced models
│       │   ├── dim/
│       │   └── fact/
│       └── lakehouse/               # SCD Type 2 models
├── audit/                          # Audit database
│   └── audit_schema.sql            # audit.duckdb schema
├── config.yaml                     # Pipeline configuration
├── docker-compose.yml              # MinIO service
└── pyproject.toml
```

---

## Data Sources

| Dataset | Source | License |
|---------|--------|---------|
| Communes GeoJSON | [data.gouv.fr](https://www.data.gouv.fr/datasets/r/127cbafe-c944-4502-a31a-9cbf64fcc08b) | Licence Ouverte 2.0 |
| Arrondissements | [INSEE](https://www.insee.fr/fr/statistiques/fichier/6051727/arrondissement_2022.csv) | Licence Ouverte 2.0 |
| Departments | [INSEE](https://www.insee.fr/fr/statistiques/fichier/6051727/departement_2022.csv) | Licence Ouverte 2.0 |
| Historical populations | [INSEE API](https://api.insee.fr/melodi/file/DS_POPULATIONS_HISTORIQUES) | Licence Ouverte 2.0 |
| Salaries | [INSEE API](https://api.insee.fr/melodi/file/DS_BTS_SAL_EQTP_SEX_PCS) | Licence Ouverte 2.0 |

---

## Query the LakeHouse

```sql
-- Connect to MinIO via DuckDB
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint = 'localhost:19000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- Top 10 communes by population in 2021
SELECT c.name, c.department_name, p.population
FROM read_parquet('s3://validated/dim_communes_france.parquet') AS c
JOIN read_parquet('s3://validated/fact_population.parquet') AS p ON c.id = p.id
WHERE p.year = 2021
ORDER BY p.population DESC
LIMIT 10;
```

---

## Documentation

- **dbt Docs:** Auto-generated on push to `master` → [GitHub Pages](https://enarroied.github.io/french_towns_lakehouse/)
- **Full Specifications:** [private/Specifications/specifications.md](private/Specifications/specifications.md)
- **Audit Database:** [audit/README.md](audit/README.md)
- **Custom Parsers:** [custom_parsers/README.md](custom_parsers/README.md)
- **Integration Pipelines:** [flows/integration/README.md](flows/integration/README.md)
