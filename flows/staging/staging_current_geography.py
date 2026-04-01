import asyncio
import sys
from pathlib import Path

from prefect import flow
from prefect import task


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flows.shared import get_buckets
from flows.shared import get_config
from flows.shared import get_directories
from flows.shared import get_downloads
from flows.shared import get_paths
from flows.shared import upload_to_staging_with_download_metadata


DOMAIN_DOWNLOADS = [
    "french_communes",
    "arrondissements",
    "departements",
    "zip_codes",
]


@task
def create_required_dirs() -> None:
    for dir_key in get_directories():
        Path(get_paths()[dir_key]).mkdir(exist_ok=True, parents=True)


@task
def download_geography_files() -> list[dict]:
    from flows.shared.download import run_async_downloads  # noqa: PLC0415

    config = get_config()
    downloads = [d for d in get_downloads() if d["name"] in DOMAIN_DOWNLOADS]
    results = run_async_downloads(
        downloads=downloads,
        temp_dir=Path(get_paths()["temp_dir"]),
        output_dir=Path(get_paths()["input_dir"]),
        concurrency=config["download"]["concurrency"],
        timeout_seconds=config["download"]["timeout_seconds"],
    )
    asyncio.run(results)
    return downloads


@task
def upload_to_staging(downloads: list[dict]) -> None:

    buckets = get_buckets()
    staging_bucket = buckets["staging_current"]
    input_dir = Path(get_paths()["input_dir"])

    for download in downloads:
        filename = download["filename"]
        if filename.endswith(".zip"):
            continue

        file_path = input_dir / filename
        if file_path.exists():
            upload_to_staging_with_download_metadata(
                file_path=file_path,
                bucket_name=staging_bucket,
                download_config=download,
                pipeline_name="staging_current_geography",
            )
            print(f"✅ Uploaded {filename} to {staging_bucket}")


@flow(name="staging_current_geography")
def staging_current_geography() -> None:
    create_required_dirs()
    downloads = download_geography_files()
    upload_to_staging(downloads)


if __name__ == "__main__":
    staging_current_geography()
