import asyncio
import shutil
import sys
import zipfile
from pathlib import Path

import httpx
from prefect import flow
from prefect import task

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flows.shared import get_config, get_paths, get_directories

GEOGRAPHY_DOWNLOADS = [
    "french_communes",
    "arrondissements",
    "departements",
]


@task
def create_required_dirs() -> None:
    for dir_key in get_directories():
        Path(get_paths()[dir_key]).mkdir(exist_ok=True, parents=True)


async def _download_file(client: httpx.AsyncClient, url: str, output_path: Path):
    print(f"📥 Downloading {url}")
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)
    print(f"✅ Saved to {output_path}")


def _process_file(file_path: Path, input_dir: Path):
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(input_dir)
        print(f"✅ Extracted to {input_dir}")
        file_path.unlink()
        print(f"🗑️ Removed {file_path}")
    except zipfile.BadZipFile:
        dest_path = input_dir / file_path.name
        shutil.move(str(file_path), str(dest_path))
        print(f"✅ Moved to {dest_path}")


async def _download_and_process(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    input_dir: Path,
):
    url = download_item["url"]
    filename = download_item["filename"]
    file_path = Path(get_paths()["temp_dir"]) / filename

    async with semaphore:
        await _download_file(client, url, file_path)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _process_file, file_path, input_dir)


async def _run_async_downloads():
    config = get_config()
    downloads = [
        d for d in config.get("downloads", []) if d["name"] in GEOGRAPHY_DOWNLOADS
    ]
    input_dir = Path(get_paths()["input_dir"])
    concurrency = config["download"]["concurrency"]
    timeout = config["download"]["timeout_seconds"]

    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [
            _download_and_process(semaphore, client, d, input_dir) for d in downloads
        ]
        await asyncio.gather(*tasks)


@task
def download_geography_files() -> None:
    asyncio.run(_run_async_downloads())


@flow(name="staging_current_geography")
def staging_current_geography() -> None:
    create_required_dirs()
    download_geography_files()


if __name__ == "__main__":
    staging_current_geography()
