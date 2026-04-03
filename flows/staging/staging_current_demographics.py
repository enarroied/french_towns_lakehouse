import asyncio
from pathlib import Path

from flows.shared import get_config
from flows.shared import get_downloads
from flows.shared import get_paths
from flows.shared.download import run_async_downloads_to_minio
from prefect import flow
from prefect import task


DOMAIN_DOWNLOADS = [
    "populations_historiques",
    "salaries",
]


@task
def download_demographics_files() -> dict[str, list[str]]:
    config = get_config()
    downloads = [d for d in get_downloads() if d["name"] in DOMAIN_DOWNLOADS]
    temp_dir = Path(get_paths()["temp_dir"])
    temp_dir.mkdir(exist_ok=True, parents=True)

    return asyncio.run(
        run_async_downloads_to_minio(
            downloads=downloads,
            temp_dir=temp_dir,
            concurrency=config["download"]["concurrency"],
            timeout_seconds=config["download"]["timeout_seconds"],
        )
    )


@flow(name="staging_current_demographics")
def staging_current_demographics() -> None:
    download_demographics_files()


if __name__ == "__main__":
    staging_current_demographics()
