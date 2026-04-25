import asyncio

from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import get_latest_hashes
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import log_upload
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.config import get_downloads
from flows_staging.shared.config import make_temp_dir
from flows_staging.shared.download import run_async_downloads_to_minio
from flows_staging.shared.minio import STAGING_BUCKET
from flows_staging.shared.minio import ensure_bucket_exists
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import AsyncDownloadParams
from flows_staging.shared.models import KnownHashes
from flows_staging.shared.models import StagingFlowParams
from prefect import get_run_logger
from prefect import task


@task
def download_files(domain_downloads: list[str], known_hashes: KnownHashes) -> dict:
    """Download files from configured sources to MinIO.

    Args:
        domain_downloads: List of download names to process.
        known_hashes: Dict of known file hashes for change detection.

    Returns:
        Dict mapping base_name to list of file metadata records.
    """
    config = get_config()
    downloads = get_downloads(domain_downloads)
    temp_dir = make_temp_dir()
    minio_client = get_minio_client()
    ensure_bucket_exists(STAGING_BUCKET)

    logger = get_run_logger()

    params = AsyncDownloadParams(
        downloads=downloads,
        temp_dir=temp_dir,
        known_hashes=known_hashes,
        minio_client=minio_client,
        staging_bucket=STAGING_BUCKET,
        concurrency=config["download"]["concurrency"],
        timeout_seconds=config["download"]["timeout_seconds"],
        logger=logger,
    )

    return asyncio.run(run_async_downloads_to_minio(params))


def run_staging_flow(params: StagingFlowParams) -> None:
    """Shared body for all download-based staging flows.

    Args:
        params: StagingFlowParams containing domain, domain_downloads, and technical_type.
    """
    preflight()
    run_id = init_run(domain=params.domain, technical_type=params.technical_type)
    try:
        known_hashes = get_latest_hashes()
        results = download_files(
            domain_downloads=params.domain_downloads,
            known_hashes=known_hashes,
        )

        futures = [
            log_upload.submit(
                run_id=run_id,
                file_metadata=record,
                bucket=STAGING_BUCKET,
            )
            for file_records in results.values()
            for record in file_records
        ]
        [f.result() for f in futures]
        finalize_run(run_id=run_id, status="SUCCESS", number_files=len(futures))
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise
