# Flows

This directory contains all Prefect orchestration flows organized by pipeline stage.

## Directory Structure

```
flows/
├── shared/              # Shared utilities (config, minio helpers)
├── staging/            # Staging pipelines (data ingestion)
├── transformation/     # Transformation pipelines (dbt models)
└── integration/        # Integration pipelines (LakeHouse loading)
```

## Pipeline Naming Convention

All pipelines follow the naming pattern:
```
{functionality}_{timing}_{subject_type}_{domain}
```

Where:
- **functionality**: `staging`, `transformation`, or `integration`
- **timing**: `current` or `historical`
- **subject_type**: `dim`, `fact`, or omitted for staging
- **domain**: `geography`, `demographics`, `labels`, etc.

Examples:
- `staging_current_geography` — download geography data
- `transformation_current_dim_geography` — build geography dimensions
- `transformation_current_fact_demographics` — build demographics facts
- `integration_current_dim_communes` — load commune dimensions to LakeHouse

## Shared Utilities

Import shared utilities in your flows:
```python
from flows.shared import get_config, get_paths, get_minio_client
```

## Running Flows

Individual flows can be run directly:
```bash
uv run python -m flows.staging.staging_current_geography
uv run python -m flows.transformation.transformation_current_dim_geography
```

For scheduled execution, deploy flows to Prefect:
```bash
prefect deployment build flows/staging/staging_current_geography.py:staging_current_geography -n "geography-staging"
```
