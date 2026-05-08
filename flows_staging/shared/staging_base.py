"""Shared staging pipeline for download-based and scraper-based flows.

The pipeline starts from 'file on disk'. Both downloaders and scrapers converge
here once they have a file ready — downloaders after extracting/renaming,
scrapers after writing their CSV to a temp directory.
"""

import asyncio
import tempfile
from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
from flows.shared import log
from flows_staging.shared.audit import RUN_STATUS_FAILED
from flows_staging.shared.audit import RUN_STATUS_SUCCESS
from flows_staging.shared.audit import _write_file_metadata
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import get_latest_filename_timestamp
from flows_staging.shared.audit import get_latest_hash
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.download import _add_timestamp_to_filename
from flows_staging.shared.download import _delete_unmatched_files
from flows_staging.shared.download import _download_file
from flows_staging.shared.download import _get_file_size_mb
from flows_staging.shared.download import _rename_files
from flows_staging.shared.download import calculate_md5
from flows_staging.shared.minio import _archive_old_file
from flows_staging.shared.minio import _upload_file_to_staging
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import FileMetadataRecord
from flows_staging.shared.models import StageConfig
from flows_staging.shared.models import StagingFlowParams
from prefect import task
from prefect.cache_policies import NO_CACHE


## TASKS


@task
def get_specific_config(domain_download: str, run_id: str) -> StageConfig:
    """Resolve download config from config.yaml and build a StageConfig.

    minio_client is intentionally excluded — it cannot be serialized by Prefect
    and is instantiated fresh inside _stage_files instead.

    Args:
        domain_download: Name of the download item (e.g., 'populations_historiques').
        run_id: Unique flow run identifier.

    Returns:
        StageConfig with all fields populated from config.yaml and runtime values.
    """
    all_config = get_config()
    downloads_by_name = {item["name"]: item for item in all_config.get("downloads", [])}
    item = downloads_by_name.get(domain_download, {})
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]

    return StageConfig(
        name=item["name"],
        url=item["url"],
        filename=item.get("filename"),
        source_file_patterns=item["source_file_patterns"],
        file_targets=item["file_targets"],
        extensions=item["extensions"],
        target_folder=item["target_folder"],
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=10)
def stage_files(config: StageConfig) -> int:
    """Synchronous Prefect task wrapper for the async _stage_files function.

    cache_policy=NO_CACHE because StageConfig contains runtime values like
    run_id that make caching meaningless.

    Args:
        config: Complete staging configuration.

    Returns:
        Number of files staged.
    """
    return asyncio.run(_stage_files(config))


async def _stage_files(config: StageConfig) -> int:
    """Download, extract, and upload files for a staging operation.

    Downloads the source file into a temporary directory, renames extracted
    files to their targets, then processes each file (skip if unchanged,
    archive previous version, upload new version, record metadata).

    minio_client is instantiated here rather than passed in config so it
    never needs to be serialized by Prefect.

    Args:
        config: Complete staging configuration.

    Returns:
        Number of files staged (excludes skipped unchanged files).
    """
    minio_client = get_minio_client()

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        temp_filename = (
            config.filename or Path(urlparse(config.url).path).name or config.name
        )

        await _download_file(config.url, temp_path, temp_filename)

        log(f"Files in temp after download: {[f.name for f in temp_path.iterdir()]}")

        matched = _rename_files(
            temp_path,
            config.source_file_patterns,
            config.file_targets,
        )
        _delete_unmatched_files(temp_path, config.file_targets, matched)

        number_files = 0
        for base_name, extension in zip(
            config.file_targets,
            config.extensions,
            strict=True,
        ):
            if _process_single_file(
                config, minio_client, base_name, extension, temp_path
            ):
                number_files += 1

    return number_files


def _archive_old_version_if_exists(
    minio_client: boto3.client,
    staging_bucket: str,
    evidence_bucket: str,
    target_folder: str,
    full_name: str,
) -> None:
    """Archive the previous version of a file to the evidence bucket if one exists.

    Looks up the latest timestamped filename from the database. If found,
    copies it to the evidence-archive bucket and deletes the staging copy.

    Args:
        minio_client: Initialized boto3 S3 client.
        staging_bucket: Current staging bucket name.
        evidence_bucket: Evidence archive bucket name.
        target_folder: Subfolder within the bucket.
        full_name: Full filename (e.g., 'populations_historiques.csv').
    """
    old_timestamped = get_latest_filename_timestamp(full_name)
    if old_timestamped:
        old_location = f"{target_folder}/{old_timestamped}"
        _archive_old_file(minio_client, staging_bucket, evidence_bucket, old_location)


def _process_single_file(
    config: StageConfig,
    minio_client: boto3.client,
    base_name: str,
    extension: str,
    temp_path: Path,
) -> bool:
    """Process one file: skip if unchanged, archive old version, upload new version.

    Expects a file named exactly `base_name` (no extension) inside `temp_path`.
    This is the shared entry point for both downloaders and scrapers.

    Args:
        config: Staging configuration (target_folder, buckets, run_id, url).
        minio_client: Initialized boto3 S3 client.
        base_name: File base name without extension (e.g., 'populations_historiques').
        extension: File extension including the dot (e.g., '.csv').
        temp_path: Directory containing the file named `base_name`.

    Returns:
        True if the file was staged (uploaded and recorded), False if skipped.
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
        minio_client,
        config.staging_bucket,
        config.evidence_bucket,
        config.target_folder,
        full_name,
    )

    # measure size before upload while file still exists on disk
    size_mb = _get_file_size_mb(file_path)

    _upload_file_to_staging(
        minio_client, file_path, config.staging_bucket, file_location
    )

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
        datetime.now(timezone.utc),
    )

    log(f"✅ Staged {full_name} → {file_location} ({size_mb}MB)")
    return True


def run_staging_flow(params: StagingFlowParams) -> None:
    """Shared body for all download-based staging flows.

    Args:
        params: StagingFlowParams containing domain, domain_download, and technical_type.
    """
    preflight()
    run_id = init_run(domain=params.domain, technical_type=params.technical_type)
    try:
        config = get_specific_config(params.domain_download, run_id)
        number_files = stage_files(config)
        finalize_run(
            run_id=run_id, status=RUN_STATUS_SUCCESS, number_files=number_files
        )
    except Exception:
        finalize_run(run_id=run_id, status=RUN_STATUS_FAILED, number_files=0)
        raise
