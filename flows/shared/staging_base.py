import asyncio
from pathlib import Path

from flows.shared.audit import finalize_run
from flows.shared.audit import get_latest_hashes
from flows.shared.audit import init_run
from flows.shared.audit import log_upload
from flows.shared.audit import preflight
from flows.shared.config import get_config
from flows.shared.config import get_downloads
from flows.shared.config import get_paths
from flows.shared.download import run_async_downloads_to_minio
from flows.shared.minio import STAGING_BUCKET
from prefect import task


@task
def download_files(
    domain_downloads: list[str], known_hashes: dict
) -> tuple[dict, dict]:
    config = get_config()
    downloads = [d for d in get_downloads() if d["name"] in domain_downloads]
    temp_dir = Path(get_paths()["temp_dir"])
    temp_dir.mkdir(exist_ok=True, parents=True)

    results = asyncio.run(
        run_async_downloads_to_minio(
            downloads=downloads,
            temp_dir=temp_dir,
            known_hashes=known_hashes,
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
                name=record["base_name"],
                filename_timestamp=record["filename_timestamp"],
                source_url=url_by_name.get(name),
                size_mb=record["size_mb"],
                md5_hash=record["md5"],
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
