# Flows

Prefect orchestration flows for the French Towns LakeHouse pipeline.

## Overview

This directory contains all Prefect flows organized by pipeline stage. See the [root README](../README.md) for the complete project documentation.

## Directory Structure

```
flows/
├── shared/              # Shared utilities (config, minio, download, dbt)
├── staging/             # Staging pipelines (data ingestion → MinIO)
├── transformation/      # Transformation pipelines (dbt models → validated/)
└── integration/         # Integration pipelines (validated → LakeHouse)
```

## Running Flows

### Test All Flows Together

```bash
uv run python -m flows.french_towns_pipeline
```

### Run Individual Flows

```bash
# Staging
uv run python -m flows.staging.staging_current_geography
uv run python -m flows.staging.staging_current_demographics
uv run python -m flows.staging.staging_current_labels

# Transformation
uv run python -m flows.transformation.transformation_current_dim_geography
uv run python -m flows.transformation.transformation_current_fact_demographics
uv run python -m flows.transformation.transformation_current_labels
```

## Deploying to Prefect

```bash
./scripts/deploy_flows.sh
```

Then view and manage deployments at `http://localhost:4200/deployments`.

## Shared Utilities

```python
from flows.shared import (
    get_config,        # Load config.yaml
    get_paths,         # Get path configuration
    get_buckets,       # Get MinIO bucket names
    get_downloads,     # Get download configurations
    get_scrapers,      # Get scraper configurations
    get_minio_client,  # Get boto3 MinIO client
    upload_to_staging,  # Upload file with metadata sidecar
)
```
