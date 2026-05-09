from datetime import datetime
from typing import TYPE_CHECKING
from typing import Literal

import httpx
from flows.shared import log
from flows_staging.shared import audit_db
from flows_staging.shared.models import FileMetadataRecord
from flows_staging.shared.models import KnownFileHash
from flows_staging.shared.models import StageConfig
from prefect import task


if TYPE_CHECKING:
    from flows_staging.scrapers.models import FileMetadata


TechnicalType = Literal["STAGING", "TRANSFORMATION", "INTEGRATION"]
TechnicalSubtype = Literal["DOWNLOAD", "SCRAPER", "DBT", "API"]
RunStatus = Literal["STARTED", "SUCCESS", "FAILED"]

RUN_STATUS_SUCCESS: RunStatus = "SUCCESS"
RUN_STATUS_STARTED: RunStatus = "STARTED"
RUN_STATUS_FAILED: RunStatus = "FAILED"


def _check_db() -> None:
    try:
        audit_db.migrate()
        log("✅ Metadata DB reachable")
    except Exception as e:
        raise RuntimeError(f"Metadata DB not reachable: {e}") from e


def _check_minio() -> None:
    try:
        from flows_staging.shared.minio import get_minio_client  # noqa: PLC0415

        get_minio_client().list_buckets()
        log("✅ MinIO reachable")
    except Exception as e:
        raise RuntimeError(f"MinIO not reachable: {e}") from e


def _check_internet_connection() -> None:
    try:
        httpx.head("https://www.cloudflare.com", timeout=5)
        log("✅ Internet reachable")
    except Exception as e:
        raise RuntimeError(f"No internet connection: {e}") from e


@task(retries=3, retry_delay_seconds=30)
def preflight() -> None:
    _check_db()
    _check_minio()
    _check_internet_connection()


@task
def init_run(
    domain: str,
    layer: TechnicalType = "STAGING",
    technical_type: TechnicalSubtype = "DOWNLOAD",
) -> str:
    run_id = audit_db.init_run(domain, layer, technical_type)
    log(f"▶ Run started: {domain}/{layer}/{technical_type} [{run_id[:8]}]")
    return run_id


@task
def get_latest_hashes() -> dict[str, KnownFileHash]:
    return audit_db.get_latest_hashes()


def get_latest_hash(filename: str) -> str:
    return audit_db.get_latest_hash(filename)


def get_latest_filename_timestamp(filename: str) -> str:
    return audit_db.get_latest_filename_timestamp(filename)


def _write_file_metadata(
    config: StageConfig,
    record: FileMetadataRecord,
    now: datetime,
) -> None:
    audit_db.write_file_metadata(config, record, now)


@task
def log_upload(
    run_id: str,
    file_metadata: "FileMetadata",
    bucket: str | None = None,
) -> None:
    audit_db.log_upload(run_id, file_metadata, bucket)
    log(
        f"✅ {file_metadata.base_name} → {file_metadata.filename_timestamp} | {file_metadata.size_mb}MB | {file_metadata.md5}"
    )


@task
def finalize_run(
    run_id: str,
    status: RunStatus = RUN_STATUS_SUCCESS,
    number_files: int = 0,
) -> None:
    audit_db.finalize_run(run_id, status, number_files)

    icon = "✅" if status == RUN_STATUS_SUCCESS else "❌"
    log(f"{icon} Run {status} [{run_id[:8]}] — {number_files} file(s)")
