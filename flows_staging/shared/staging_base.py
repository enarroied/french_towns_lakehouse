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
from flows_staging.shared.models import KnownHashes
from prefect import task


@task
def download_files(
    domain_downloads: list[str], known_hashes: KnownHashes
) -> tuple[dict, dict]:
    config = get_config()
    downloads = [d for d in get_downloads() if d["name"] in domain_downloads]
    temp_dir = make_temp_dir()
    minio_client = get_minio_client()
    ensure_bucket_exists(STAGING_BUCKET)

    results = asyncio.run(
        run_async_downloads_to_minio(
            downloads=downloads,
            temp_dir=temp_dir,
            known_hashes=known_hashes,
            minio_client=minio_client,
            staging_bucket=STAGING_BUCKET,
            concurrency=config["download"]["concurrency"],
            timeout_seconds=config["download"]["timeout_seconds"],
        )
    )
    url_by_name = {d["name"]: d.get("url") for d in downloads}
    return results, url_by_name


def run_staging_flow(domain: str, domain_downloads: list[str]) -> None:
    """Shared body for all download-based staging flows."""
    preflight()
    run_id = init_run(domain=domain, technical_type="DOWNLOAD")
    try:
        known_hashes = get_latest_hashes()
        results, url_by_name = download_files(
            domain_downloads=domain_downloads,
            known_hashes=known_hashes,
        )

        futures = [
            log_upload.submit(
                run_id=run_id,
                file_metadata=record,
                bucket=STAGING_BUCKET,
            )
            for name, file_records in results.items()
            for record in file_records
        ]
        [f.result() for f in futures]
        finalize_run(run_id=run_id, status="SUCCESS", number_files=len(futures))
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise
