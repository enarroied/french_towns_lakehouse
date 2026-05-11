# French Towns LakeHouse

![Python](https://img.shields.io/badge/dynamic/toml?url=https://raw.githubusercontent.com/enarroied/french_towns_lakehouse/master/pyproject.toml&query=$.project.requires-python&label=Python&logo=python&logoColor=white)
[![codecov](https://codecov.io/github/enarroied/french_towns_lakehouse/branch/master/graph/badge.svg?token=HLZVB6WFMT)](https://codecov.io/github/enarroied/french_towns_lakehouse)
![Tests](https://github.com/enarroied/french_towns_lakehouse/actions/workflows/tests.yml/badge.svg)
![Prefect](https://img.shields.io/badge/Prefect-F02D5E?logo=prefect)
![MinIO](https://img.shields.io/badge/MinIO-C5A572)
![dbt](https://img.shields.io/badge/dbt-FF694B)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF?logo=duckdb&logoColor=374151)
![License](https://img.shields.io/github/license/enarroied/french_towns_lakehouse)

- [French Towns LakeHouse](#french-towns-lakehouse)
  - [What does this build?](#what-does-this-build)
  - [Technology Stack](#technology-stack)
  - [Setup](#setup)
  - [Infrastructure Services](#infrastructure-services)
    - [MinIO](#minio)
    - [Apache Polaris](#apache-polaris)
      - [Setup](#setup-1)
      - [Polaris Credentials](#polaris-credentials)
      - [Stopping Services](#stopping-services)
    - [PostgreSQL](#postgresql)
      - [Credentials](#credentials)
      - [Manual Setup (without Docker)](#manual-setup-without-docker)
      - [Connecting](#connecting)
      - [Start](#start)
      - [Stop](#stop)
  - [Pipeline Architecture](#pipeline-architecture)
    - [MinIO Bucket Structure](#minio-bucket-structure)
    - [dbt Model Layers](#dbt-model-layers)
  - [Naming Conventions](#naming-conventions)
    - [Pipeline Naming](#pipeline-naming)
    - [Domain Categories](#domain-categories)
  - [Running the Pipeline](#running-the-pipeline)
    - [Start Prefect Server](#start-prefect-server)
    - [Start a dev worker](#start-a-dev-worker)
    - [Run All Flows (for testing)](#run-all-flows-for-testing)
    - [Run Individual Flows](#run-individual-flows)
    - [What the Pipeline Does](#what-the-pipeline-does)
    - [Run dbt in isolation](#run-dbt-in-isolation)
  - [Prefect Deployments](#prefect-deployments)
      - [PostgreSQL Backend](#postgresql-backend)
  - [Project Structure](#project-structure)
  - [Data Sources](#data-sources)
  - [Query the LakeHouse](#query-the-lakehouse)
  - [Documentation](#documentation)
  - [Development](#development)
    - [Running Tests](#running-tests)
    - [Linting \& Code Quality](#linting--code-quality)
    - [Adding a New Scraper](#adding-a-new-scraper)

A self-hosted data lakehouse of French municipal data. The pipeline downloads open government datasets, transforms them into clean Parquet files using DuckDB and dbt, and uploads the results to MinIO object storage.

**Blog & documentation:** [https://enarroied.github.io/french_towns_lakehouse/](https://enarroied.github.io/french_towns_lakehouse/)
**dbt model documentation:** [https://enarroied.github.io/french_towns_lakehouse/docs/](https://enarroied.github.io/french_towns_lakehouse/docs/)

---

## What does this build?

This is a work in progress.

The goal is to build an data LakeHouse using public data. The LakeHouse contains information about French communes.

The pipeline follows a medallion architecture, with 3 layers.

The data model follows a star schema.

---

## Technology Stack

| Layer | Tool |
|-------|------|
| Orchestration | Prefect 3 |
| Downloading | httpx (async) |
| Transformation | dbt-core + dbt-duckdb |
| Compute | DuckDB |
| Object Storage | MinIO (S3-compatible) |
| Iceberg Catalog | Apache Polaris |
| Iceberg Integration | DuckDB + Polaris (flows_integration) |
| Package Manager | uv |
| Metadata DB | PostgreSQL 16 |

---

## Setup

```bash
git clone https://github.com/enarroied/french_towns_lakehouse.git
cd french_towns_lakehouse
uv sync

# Install the project as a Python package (required for flows to work)
uv pip install -e .

# Install dbt packages
cd french_towns_dbt && dbt deps && cd ..
```

Create `.env` (see `.env.example` for required variables):
```bash
cp .env.example .env
# Edit .env with your credentials
```

Start infrastructure services:
```bash
# Start MinIO first (creates the shared network)
docker compose -f docker/docker-compose-minio.yml up -d

# Start Polaris (Iceberg catalog)
docker compose -f docker/docker-compose-polaris.yml --env-file .env up -d

# Start PostgreSQL (metadata database)
docker compose -f docker/docker-compose-postgres.yml --env-file .env up -d
```

---

## Infrastructure Services

### MinIO

S3-compatible object storage for the data lake.

| Access Point | URL |
|--------------|-----|
| S3 API | localhost:19000 |
| Web Console | http://localhost:19001 |

MinIO buckets are auto-created on startup.

### Apache Polaris

Iceberg REST catalog for managing Iceberg tables. Enables ACID transactions, time-travel queries, and schema evolution on S3-compatible storage.

| Access Point | URL |
|--------------|-----|
| REST API | localhost:8181 |
| Health Check | http://localhost:8181/api.catalog/v1/health |

#### Setup

1. Start MinIO first:
   ```bash
   docker compose -f docker/docker-compose-minio.yml up -d
   ```

2. Start Polaris:
   ```bash
   docker compose -f docker/docker-compose-polaris.yml --env-file .env up -d
   ```

3. Verify Polaris is running:
   ```bash
   curl http://localhost:8181/api.catalog/v1/health
   ```
   Expected response: `{"failures":[],"healthy":true}`

4. Configure DuckDB to use Polaris:
   ```sql
   INSTALL iceberg;
   LOAD iceberg;

   CREATE SECRET (
       TYPEiceberg,
       HOST 'localhost',
       PORT 8181,
       URI 'http://localhost:8181',
       CLIENT_ID 'your_client_id',
       CLIENT_SECRET 'your_client_secret'
   );
   ```

#### Polaris Credentials

Credentials are configured via `.env`:

```bash
POLARIS_CLIENT_ID=your_client_id
POLARIS_CLIENT_SECRET=your_client_secret
```

#### Stopping Services

```bash
docker compose -f docker/docker-compose-minio.yml -f docker/docker-compose-polaris.yml --env-file .env down
```

---

### PostgreSQL

PostgreSQL 16 serves as the metadata database for Prefect orchestration and audit logging.

| Access Point | URL |
|--------------|-----|
| Connection | `localhost:5432` |
| Audit Database | `metadata` (schema: `audit`) |
| Prefect Database | `prefect` (auto-managed) |
| User | `french_towns` |

The `prefect` database is created automatically on first startup by `init-postgres.sh`.

> **PostgreSQL 15+:** The `french_towns` user needs `CREATE` privilege on the `public`
> schema of the `prefect` database. If you get `permission denied for schema public`,
> run:
> ```bash
> psql -U postgres -d prefect -c "GRANT ALL ON SCHEMA public TO french_towns;"
> ```

#### Credentials

Set in `.env`:

```env
PG_PASSWORD=your_secure_password
```

#### Manual Setup (without Docker)

If you have a local PostgreSQL instance, create the databases and user manually:

```bash
# Create databases
psql -U postgres -c "CREATE DATABASE metadata;"
psql -U postgres -c "CREATE DATABASE prefect;"

# Create user (if not exists)
psql -U postgres -c "CREATE USER french_towns WITH PASSWORD 'your_password';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE metadata TO french_towns;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE prefect TO french_towns;"

# Grant schema permissions (PostgreSQL 15+)
psql -U postgres -d prefect -c "GRANT ALL ON SCHEMA public TO french_towns;"
psql -U postgres -d metadata -c "GRANT ALL ON SCHEMA public TO french_towns;"
```

Then set the connection URLs in `.env`:

```env
PG_PASSWORD=your_password
AUDIT_DATABASE_URL=postgresql://french_towns:your_password@127.0.0.1:5432/metadata
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://french_towns:your_password@127.0.0.1:5432/prefect
```

#### Connecting

```bash
psql -U french_towns -d metadata -h localhost
```

#### Start

```bash
docker compose -f docker/docker-compose-postgres.yml --env-file .env up -d
```

#### Stop

```bash
docker compose -f docker/docker-compose-postgres.yml --env-file .env down
```

---

## Pipeline Architecture

You can access your MinIO service from your browser:

![](./img/minio.jpg)

The pipeline uploads parquet files to the `validated` bucket. You can browse uploaded files in the web console or query them directly via the S3 API.

To stop services:

```bash
docker compose -f docker/docker-compose-minio.yml -f docker/docker-compose-polaris.yml --env-file .env down
```

This is how the process is supposed to look (diagram from specifications, work in progress, changes may still happen):

![](img/specs.png)

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

### Start Prefect Server

If using PostgreSQL as the Prefect backend, set the connection URL first:

```bash
export PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://french_towns:${PG_PASSWORD}@127.0.0.1:5432/prefect
```

Or source your `.env` file if it already contains the variable:

```bash
source .env
```

Then start the server:

```bash
uv run prefect server start
```

### Start a dev worker

```bash
uv run prefect worker start --pool "local-dev-pool"
```

### Run All Flows (for testing)

```bash
uv run python -m flows.french_towns_pipeline
```

### Run Individual Flows

```bash
uv run python -m flows_staging.staging.staging_arrondissements
uv run python -m flows_transformation.transformation.transformation_current_dim_geography
```

### What the Pipeline Does

**Step 1 — Create directories.** Creates `input/`, `data/processed/`, and other required paths from `config.yaml` if they do not exist.

**Step 2 — Download source files.** Downloads raw datasets from French government APIs (or other sources) and data portals concurrently, using an asyncio semaphore to cap concurrency at three simultaneous requests. ZIP archives extract automatically to `input/`; plain files move there directly. INSEE APIs can be slow — the timeout is set to 120 seconds per file.

Example of downloaded files:

- `communes_france.geojson` (287 MB) — GeoJSON of all French communes
- `arrondissements.csv` — INSEE arrondissement reference table
- `departements.csv` — INSEE department reference table
- `historical_population.csv` — historical population per commune (from ZIP)
- `historical_salaries.csv` — salary data by sex and employment category (from ZIP)

**Step 3 — dbt run.** Prefect calls `dbt run` as a subprocess from inside `french_towns_dbt/`. dbt stages external sources (mounting the raw files as DuckDB views), then runs all three models in parallel across four threads. Each model writes a Parquet file to `data/processed/`.

**Step 4 — Upload to MinIO.** All `*.parquet` files from `data/processed/` upload to the `validated` bucket.

### Run dbt in isolation

To iterate on models without re-downloading source files:

```bash
cd french_towns_dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## Prefect Deployments

Deploy flows to Prefect for scheduled execution:

```bash
prefect deploy --all
```

View deployments at `http://localhost:4200/deployments`.

Each flow can be deployed independently and scheduled with cron or interval schedules.

#### PostgreSQL Backend

By default Prefect uses a local SQLite database. To switch to PostgreSQL:

```bash
export PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://french_towns:${PG_PASSWORD}@127.0.0.1:5432/prefect
```

Alternatively, set the variable in `.env`:

```env
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://french_towns:your_password@127.0.0.1:5432/prefect
```

If using **Prefect Cloud**, omit this variable — Cloud manages its own database.

Prefect auto-creates all required tables in the `prefect` database on first server start.

---

## Project Structure

```
french_towns_lakehouse/
├── flows/                           # Main pipeline orchestration
├── flows_staging/                   # Staging pipelines + scrapers
│   ├── shared/                      # Shared utilities
│   │   ├── config.py                # Config loader
│   │   ├── minio.py                 # MinIO helpers
│   │   ├── download.py              # Download + upload utilities
│   │   ├── audit.py                 # Audit logging (Prefect tasks)
│   │   ├── audit_db.py              # PostgreSQL audit DB layer
│   │   └── __init__.py
│   ├── scrapers/                    # Web scrapers
│   │   ├── scrape_villes_fleuries.py
│   │   ├── scrape_village_etape.py
│   │   ├── scrape_petites_cites.py
│   │   ├── scrape_famille_plus.py
│   │   └── ...
│   ├── staging/                     # Staging flow definitions
│   └── custom_parsers/              # PDF parsers
│       └── parse_ville_sportive.py
├── flows_transformation/           # Transformation pipelines
├── flows_integration/               # Integration pipelines
│   ├── shared/                      # Shared utils (connection, validation, SCD2, fact_loader)
│   └── integration/                 # Integration flow definitions
├── blog/                           # Quarto blog (deployed to GitHub Pages root)
│   ├── posts/                      # Blog posts
│   ├── _freeze/                    # Pre-rendered outputs (committed to git)
│   └── _quarto.yml                 # Quarto config
├── french_towns_dbt/               # dbt project
│   └── models/
│       ├── staging/                # Raw staging models
│       ├── validated/              # Schema-enforced models
│       │   ├── dim/
│       │   └── fact/
│       └── lakehouse/              # SCD Type 2 models
├── tests/                          # Test suite (145 tests)
│   ├── conftest.py
│   ├── shared/                     # Tests for shared modules
│   ├── scrapers/                   # Tests for web scrapers
│   └── custom_parsers/             # Tests for PDF parsers
├── config.yaml                     # Pipeline configuration
├── docker/                         # Docker configs
│   ├── docker-compose-minio.yml   # MinIO S3 storage
│   ├── docker-compose-polaris.yml # Polaris Iceberg catalog
│   ├── docker-compose-postgres.yml# PostgreSQL metadata database
│   └── init-postgres.sh           # Creates `prefect` DB on first boot
├── .env.example                   # Environment variables template
└── pyproject.toml                 # Project config + linting
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

### Parquet (validated layer)

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
SELECT
    c.name,
    c.department_name,
    p.population
FROM read_parquet('s3://validated/dim_communes_france.parquet') AS c
JOIN read_parquet('s3://validated/fact_population.parquet') AS p
    ON c.id = p.id
WHERE p.year = 2021
ORDER BY p.population DESC
LIMIT 10;
```

```sql
-- Gender pay gap by region (2023)
SELECT
    c.region_name,
    ROUND(AVG(s.mean_salary_men)) AS avg_salary_men,
    ROUND(AVG(s.mean_salary_women)) AS avg_salary_women,
    ROUND(100.0 * (AVG(s.mean_salary_men) - AVG(s.mean_salary_women)) / AVG(s.mean_salary_men), 1) AS gap_pct
FROM read_parquet('s3://validated/dim_communes_france.parquet') AS c
JOIN read_parquet('s3://validated/fact_salaries.parquet') AS s
    ON c.id = s.id
WHERE c.flag_metropole = 1
GROUP BY c.region_name
ORDER BY gap_pct DESC;
```

### Iceberg (lakehouse layer)

Run `setup_polaris.py` once, then query with time-travel support:

```sql
INSTALL iceberg;
LOAD iceberg;

CREATE SECRET polaris_secret (
    TYPE iceberg,
    CLIENT_ID 'your_client_id',
    CLIENT_SECRET 'your_client_secret'
);

ATTACH 'french_towns' AS polaris (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181/api/catalog',
    SECRET 'polaris_secret'
);

-- Query with time travel
SELECT * FROM polaris.lakehouse.dim_communes_france
    FOR SYSTEM_TIME AS OF TIMESTAMP '2025-01-01 00:00:00';
```

---

## Documentation

- **Blog:** [https://enarroied.github.io/french_towns_lakehouse/](https://enarroied.github.io/french_towns_lakehouse/)
- **dbt Docs:** [https://enarroied.github.io/french_towns_lakehouse/docs/](https://enarroied.github.io/french_towns_lakehouse/docs/)
- **Full Specifications:** [private/Specifications/specifications.md](private/Specifications/specifications.md)
- **Custom Parsers:** [flows_staging/custom_parsers/README.md](flows_staging/custom_parsers/README.md)
- **Integration Pipelines:** [flows_integration/integration/README.md](flows_integration/integration/README.md)

---

## Development

### Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Skip integration tests (require Prefect API)
uv run pytest tests/ -v -m "not integration"

# Run specific test file
uv run pytest tests/scrapers/test_scrape_villes_fleuries.py -v
```

### Linting & Code Quality

```bash
# Check linting
ruff check flows_staging/ tests/

# Auto-fix linting issues
ruff check flows_staging/ tests/ --fix

# Check for dead code
vulture flows_staging --min-confidence 80
```

### Adding a New Scraper

1. Create scraper in `flows_staging/scrapers/scrape_<name>.py`
2. Use `write_csv_for_staging()` (from `flows_staging.shared.download`) + `_process_single_file()` (from `flows_staging.shared.staging_base`) for CSV upload and staging
3. Add config entry in `config.yaml` under `scrapers`
4. Add tests in `tests/scrapers/test_scrape_<name>.py`
