import asyncio
from pathlib import Path

from flows.shared import finalize_run
from flows.shared import get_config
from flows.shared import get_downloads
from flows.shared import get_paths
from flows.shared import init_run
from flows.shared import log_upload
from flows.shared import preflight
from flows.shared.download import run_async_downloads_to_minio
from flows.shared.minio import STAGING_BUCKET
from prefect import flow
from prefect import task


DOMAIN_DOWNLOADS = ["populations_historiques", "salaries"]


@task
def download_demographics_files() -> tuple[dict[str, list[dict]], dict[str, str]]:
    config = get_config()
    downloads = [d for d in get_downloads() if d["name"] in DOMAIN_DOWNLOADS]
    temp_dir = Path(get_paths()["temp_dir"])
    temp_dir.mkdir(exist_ok=True, parents=True)

    results = asyncio.run(
        run_async_downloads_to_minio(
            downloads=downloads,
            temp_dir=temp_dir,
            concurrency=config["download"]["concurrency"],
            timeout_seconds=config["download"]["timeout_seconds"],
        )
    )
    url_by_name = {d["name"]: d.get("url") for d in downloads}
    return results, url_by_name


@flow(name="staging_current_demographics")
def staging_current_demographics() -> None:
    preflight()
    run_id = init_run(domain="demographics")
    try:
        results, url_by_name = download_demographics_files()

        total = 0
        for name, file_records in results.items():
            for record in file_records:
                log_upload(
                    run_id=run_id,
                    name=name,
                    keys=[record["key"]],
                    source_url=url_by_name.get(name),
                    size_mb=record["size_mb"],
                    md5_hash=record["md5"],
                    bucket=STAGING_BUCKET,
                )
                total += 1

        finalize_run(run_id=run_id, status="SUCCESS", number_files=total)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED")
        raise


if __name__ == "__main__":
    staging_current_demographics()
