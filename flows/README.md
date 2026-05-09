# Flows

Prefect orchestration flows for the French Towns LakeHouse pipeline.

## Overview

This directory contains all Prefect flows organized by pipeline stage. See the [root README](../README.md) for the complete project documentation.

## Directory Structure

```
flows/                          # Top-level orchestration (french_towns_pipeline)
flows_staging/                  # Staging pipelines + scrapers
├── staging/                    # Download-based staging flows
├── scrapers/                   # Web scraper flows
└── custom_parsers/             # PDF/document parsers
flows_transformation/           # Transformation pipelines (dbt models → validated/)
flows_integration/              # Integration pipelines (validated → LakeHouse)
```

## Running Flows

### Test All Flows Together

```bash
uv run python -m flows.french_towns_pipeline
```

### Run Individual Flows

```bash
# Staging
uv run python -m flows_staging.staging.staging_arrondissements
uv run python -m flows_staging.staging.staging_historical_population
uv run python -m flows_staging.staging.staging_current_labels

# Transformation
uv run python -m flows_transformation.transformation.transformation_current_dim_geography
uv run python -m flows_transformation.transformation.transformation_current_fact_demographics
uv run python -m flows_transformation.transformation.transformation_current_labels
```

## Deploying to Prefect

```bash
source .env && prefect deploy --all
```

Then view and manage deployments at `http://localhost:4200/deployments`.

## Shared Utilities

```python
from flows_staging.shared.config import get_config  # Load config.yaml
from flows_staging.shared.minio import get_minio_client  # Get boto3 MinIO client
from flows_staging.shared.download import write_csv_for_staging  # Write CSV
from flows_staging.shared.staging_base import _process_single_file  # Stage file
```
