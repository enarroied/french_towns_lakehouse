# Refactoring Plan: `staging_current_demographics.py`

## Current state
`_stage_files` has a long argument extraction block at the top and an inline for loop doing per-file processing. The `_write_file_metadata` function takes 11 positional arguments.

---

## Changes

### 1. Add `dataclass` import + new dataclasses

```python
from dataclasses import dataclass

@dataclass
class StageConfig:
    """Configuration for a staging download operation."""
    name: str
    url: str
    source_file_patterns: list[str]
    file_targets: list[str]
    extensions: list[str]
    target_folder: str
    run_id: str
    staging_bucket: str
    evidence_bucket: str
    minio_client: Any


@dataclass
class FileMetadataRecord:
    """Per-file metadata to persist to the database."""
    name: str
    filename_timestamp: str
    source_url: str | None
    size_mb: float
    md5_hash: str
    bucket: str
    file_location: str
```

### 2. Refactor `get_specific_config` to return `StageConfig`

```python
@task
def get_specific_config(domain_download: str, run_id: str) -> StageConfig:
    """Resolve download config from config.yaml and build StageConfig."""
    all_config = get_config()
    downloads_by_name = {item["name"]: item for item in all_config.get("downloads", [])}
    item = downloads_by_name.get(domain_download, {})
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = _get_minio_client()

    return StageConfig(
        name=item["name"],
        url=item["url"],
        source_file_patterns=item["source_file_patterns"],
        file_targets=item["file_targets"],
        extensions=item["extensions"],
        target_folder=item["target_folder"],
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
        minio_client=minio_client,
    )
```

### 3. Refactor `_write_file_metadata` to accept `StageConfig` + `FileMetadataRecord`

```python
def _write_file_metadata(
    config: StageConfig,
    record: FileMetadataRecord,
    now: datetime,
) -> None:
    """Write file metadata to DuckDB, marking previous version as not latest.

    Args:
        config: Staging configuration containing run_id.
        record: Per-file metadata to insert.
        now: UTC timestamp for the upload record.
    """
    with _conn() as conn:
        conn.execute(
            "UPDATE file_metadata SET is_latest = 0 WHERE filename = ?",
            [record.name],
        )
        conn.execute(
            """INSERT INTO file_metadata
               (file_id, run_id, filename, filename_timestamp, source_url,
                size_mb, md5_hash, bucket, upload_timestamp, file_location)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                str(uuid.uuid4()),
                config.run_id,
                record.name,
                record.filename_timestamp,
                record.source_url,
                record.size_mb,
                record.md5_hash,
                record.bucket,
                now,
                record.file_location,
            ],
        )
```

### 4. New helper: `_archive_old_version_if_exists`

```python
def _archive_old_version_if_exists(
    minio_client: Any,
    staging_bucket: str,
    evidence_bucket: str,
    target_folder: str,
    full_name: str,
) -> None:
    """Archive the previous version of a file to evidence bucket if one exists.

    Args:
        minio_client: S3/MinIO client.
        staging_bucket: Current staging bucket name.
        evidence_bucket: Evidence archive bucket name.
        target_folder: Target folder within the bucket.
        full_name: Full filename (e.g., 'populations_historiques.csv').
    """
    old_timestamped = get_latest_filename_timestamp(full_name)
    if old_timestamped:
        old_location = f"{target_folder}/{old_timestamped}"
        _archive_old_file(
            minio_client, staging_bucket, evidence_bucket, old_location,
        )
```

### 5. New helper: `_process_single_file`

```python
def _process_single_file(
    config: StageConfig,
    base_name: str,
    extension: str,
    temp_path: pathlib.Path,
    now: datetime,
) -> bool:
    """Process one extracted file: skip if unchanged, archive old, upload new.

    Args:
        config: Staging configuration.
        base_name: File base name (e.g., 'populations_historiques').
        extension: File extension (e.g., '.csv').
        temp_path: Path to the temp directory containing the file.
        now: UTC timestamp for metadata.

    Returns:
        True if the file was staged (uploaded and recorded).
    """
    file_path = temp_path / base_name
    full_name = f"{base_name}{extension}"

    md5 = calculate_md5(file_path)
    known_hash = get_latest_hash(full_name)

    if md5 == known_hash:
        log(f"⏭️ Skipping {full_name} — hash unchanged")
        file_path.unlink()
        return False

    timestamped_filename = _add_timestamp_to_filename(base_name, extension)
    file_location = f"{config.target_folder}/{timestamped_filename}"

    _archive_old_version_if_exists(
        config.minio_client,
        config.staging_bucket,
        config.evidence_bucket,
        config.target_folder,
        full_name,
    )

    _upload_file_to_staging(
        config.minio_client, file_path, config.staging_bucket, file_location,
    )

    size_mb = _get_file_size_mb(file_path)

    _write_file_metadata(
        config,
        FileMetadataRecord(
            name=full_name,
            filename_timestamp=timestamped_filename,
            source_url=config.url,
            size_mb=size_mb,
            md5_hash=md5,
            bucket=config.staging_bucket,
            file_location=file_location,
        ),
        now,
    )

    return True
```

### 6. Refactored `_stage_files`

```python
async def _stage_files(config: StageConfig) -> int:
    """Download, extract, and upload files for a staging operation.

    Args:
        config: Complete staging configuration.

    Returns:
        Number of files staged (excludes skipped unchanged files).
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = pathlib.Path(tmpdir)
        temp_filename = pathlib.Path(urlparse(config.url).path).name or config.name

        await _download_file(config.url, temp_path, temp_filename)

        matched = _rename_files(
            temp_path, config.source_file_patterns, config.file_targets,
        )
        _delete_unmatched_files(temp_path, config.file_targets, matched)

        now = datetime.now(timezone.utc)
        number_files = 0

        for base_name, extension in zip(
            config.file_targets, config.extensions, strict=True,
        ):
            if _process_single_file(config, base_name, extension, temp_path, now):
                number_files += 1

    return number_files
```

---

## Summary of changes

| Change | Before | After |
|--------|--------|-------|
| Config access | `config["url"]`, etc. (6 lines) | `config.url` (dataclass attr) |
| `_write_file_metadata` | 11 params | 3 params (`config`, `record`, `now`) |
| Per-file loop body | Inline (20+ lines) | `_process_single_file()` |
| Archive logic | Inline 5-line block | `_archive_old_version_if_exists()` |
| Type hints | Minimal on many functions | Full coverage |
| Docstrings | Present on some | All functions documented |
