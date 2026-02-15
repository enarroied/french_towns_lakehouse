import asyncio
import shutil
import zipfile
from pathlib import Path

import httpx
import yaml

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Paths from config
TEMP_DIR = Path(config["paths"]["temp_dir"])
INPUT_DIR = Path(config["paths"]["input_dir"])

# Download settings from config
CONCURRENCY = config["download"]["concurrency"]
TIMEOUT = config["download"]["timeout_seconds"]

# Downloads from config (now a list of dicts)
DOWNLOADS = config["downloads"]

# Ensure directories exist
TEMP_DIR.mkdir(exist_ok=True, parents=True)
INPUT_DIR.mkdir(exist_ok=True, parents=True)


async def download_file(client: httpx.AsyncClient, url: str, output_path: Path):
    """Async download a file from URL"""
    print(f"📥 Downloading {url}")

    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)

    print(f"✅ Saved to {output_path}")
    return output_path


async def process_file(file_path: Path):
    """Check if file is ZIP and process accordingly"""
    loop = asyncio.get_event_loop()

    await loop.run_in_executor(None, _process, file_path)


def _process(file_path):
    # Check if it's a ZIP file
    try:
        _process_zip(file_path)
    except zipfile.BadZipFile:
        print("📄 File is not a ZIP, moving to input...")
        # Move to input directory
        dest_path = INPUT_DIR / file_path.name
        shutil.move(str(file_path), str(dest_path))
        print(f"✅ Moved to {dest_path}")


def _process_zip(file_path):
    print("📦 File is a ZIP, extracting...")
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(INPUT_DIR)
    print(f"✅ Extracted to {INPUT_DIR}")
    # Remove the zip file after extraction
    file_path.unlink()
    print(f"🗑️ Removed {file_path}")


async def download_and_process(
    semaphore: asyncio.Semaphore, client: httpx.AsyncClient, download_item: dict
):
    """Download and process one file using named download config"""
    url = download_item["url"]
    filename = download_item["filename"]
    name = download_item.get("name", filename)  # Use name if available

    print(f"\n🔄 Processing: {name}")
    file_path = TEMP_DIR / filename

    async with semaphore:
        await download_file(client, url, file_path)
        await process_file(file_path)


async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        tasks = []
        for download in DOWNLOADS:
            tasks.append(download_and_process(semaphore, client, download))

        await asyncio.gather(*tasks)

    print("\n📋 Final files in input directory:")
    for f in INPUT_DIR.iterdir():
        size = f.stat().st_size
        print(f"  - {f.name} ({size} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
