# Integration Pipelines

Integration pipelines are responsible for moving validated data from the validated layer into the LakeHouse using DuckDB and dbt.

## Responsibilities

- **Idempotent:** Running a pipeline multiple times produces the same result.
- **No rejections at this stage:** All data quality checks happen in the validation stage.
- **Dimensions:** Loaded using either full reload or delta UPSERT, with SCD Type 2 row management.
- **Facts:** Loaded using append-only INSERT with duplicate detection.

## Pipeline Naming Convention

Integration pipelines follow the naming pattern:
```
{functionality}_{timing}_{subject_type}_{domain}
```

Examples:
- `integration_current_dim_geography` — integrate current geography dimensions
- `integration_current_fact_demographics` — integrate current demographics facts
- `integration_historical_dim_communes` — integrate historical commune dimension rows

## Implemented

| Flow | Table | Strategy | Status |
|------|-------|----------|--------|
| `integration_current_dim_geography` | `dim_communes_france`, `dim_zip_codes` | SCD Type 2 (UPSERT) | ✅ |
| `integration_current_dim_geography` | `bridge_communes_zip_codes` | Append-only with dedup | ✅ |
| `integration_current_fact_demographics` | `fact_population`, `fact_salaries` | Append-only with dedup | ✅ |
| `integration_current_labels` | `dim_labels`, `fact_labels` | **Blocked** (pending transformation layer) | ❌ |

## Querying the Lakehouse

Once Polaris is running and integration flows have run, you can query the Iceberg tables.

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET minio_secret (
    TYPE s3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:19000',
    REGION 'us-east-1',
    USE_SSL false,
    URL_STYLE 'path'
);

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

-- List tables
SHOW TABLES IN polaris.lakehouse;

-- Query dimension
SELECT * FROM polaris.lakehouse.dim_communes_france LIMIT 10;

-- Time travel — query as of a specific timestamp or snapshot version
SELECT * FROM polaris.lakehouse.dim_communes_france
    AT (TIMESTAMP => '2025-01-01 00:00:00');

-- Time travel — query by snapshot ID
SELECT * FROM polaris.lakehouse.dim_communes_france
    AT (VERSION => 1234567890);
```

### DBeaver

1. Driver: **DuckDB** (not PostgreSQL)
2. Path: any file path (DuckDB in-memory mode)
3. Open a script and paste the connection SQL above

### Python

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")
conn.execute("INSTALL httpfs; LOAD httpfs;")

conn.execute("""
    CREATE SECRET polaris_secret (
        TYPE iceberg,
        CLIENT_ID 'your_client_id',
        CLIENT_SECRET 'your_client_secret'
    )
""")

conn.execute("""
    ATTACH 'french_towns' AS polaris (
        TYPE iceberg,
        ENDPOINT 'http://localhost:8181/api/catalog',
        SECRET 'polaris_secret'
    )
""")

df = conn.execute("SELECT * FROM polaris.lakehouse.dim_communes_france").df()
print(df.head())
```

## Failure Handling

Integration pipelines are **strict**: before writing to the LakeHouse, an integration pipeline asserts that the validated layer is in an expected state (non-empty and within acceptable freshness window). If upstream validation failed or was skipped, the integration pipeline refuses to run and records the blockage in the audit table.
