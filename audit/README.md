# Audit Database

The audit database (`audit.duckdb`) records pipeline execution metadata and is stored separately from the LakeHouse.

## Purpose

- Track pipeline run history with timestamps and row counts
- Record source files processed per run
- Capture failure information and error messages
- Track rejection batches and their resolution status

## Schema

### Table: `pipeline_runs`

Records one row per pipeline execution.

| Column | Type | Description |
|--------|------|-------------|
| pipeline_name | VARCHAR | Name of the Prefect flow |
| prefect_run_id | UUID | Prefect run identifier |
| run_start | TIMESTAMP | Run start time (UTC) |
| run_end | TIMESTAMP | Run end time (UTC) |
| source_files | JSON | List of source files processed |
| rows_ingested | INTEGER | Rows ingested from staging |
| rows_validated | INTEGER | Rows validated successfully |
| rows_rejected | INTEGER | Rows rejected during validation |
| rows_inserted | INTEGER | Rows inserted to LakeHouse |
| status | VARCHAR | `success`, `partial`, `failure` |
| error_message | TEXT | Error details if failed |

### Table: `rejections`

Tracks rejection batches for review and resolution.

| Column | Type | Description |
|--------|------|-------------|
| pipeline_name | VARCHAR | Name of the pipeline |
| prefect_run_id | UUID | Prefect run identifier |
| source_file | VARCHAR | Path to source file |
| rejection_reason | TEXT | Summary of why records were rejected |
| row_count | INTEGER | Number of rejected rows |
| status | VARCHAR | `open`, `resolved`, `discarded` |
| resolution_note | TEXT | Note explaining resolution (if any) |
| resolved_at | TIMESTAMP | When the rejection was resolved |
| created_at | TIMESTAMP | When the rejection was recorded |

## Design Principles

1. **Immutable records**: Once a run record is written, it is never modified
2. **Separation from LakeHouse**: Audit data lives in its own DuckDB file, never mixed with analytical data
3. **Strict pipelines check**: Integration pipelines query this table to verify upstream validation succeeded
4. **Human review**: Rejection status transitions require human decision (resolve or discard)

## Usage

```python
import duckdb

conn = duckdb.connect("audit/audit.duckdb")

# Check if validation ran successfully for a given period
result = conn.execute("""
    SELECT status, rows_validated, rows_rejected
    FROM pipeline_runs
    WHERE pipeline_name = 'validation_current_demographics'
    ORDER BY run_end DESC
    LIMIT 1
""").fetchone()
```
