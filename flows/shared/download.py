import asyncio
import shutil
import zipfile
from pathlib import Path

import httpx


async def _download_file(
    client: httpx.AsyncClient, url: str, output_path: Path
) -> None:
    print(f"📥 Downloading {url}")
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)
    print(f"✅ Saved to {output_path}")


def _process_file(file_path: Path, output_dir: Path) -> Path:
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"✅ Extracted to {output_dir}")
        file_path.unlink()
        print(f"🗑️ Removed {file_path}")
        return output_dir
    except zipfile.BadZipFile:
        dest_path = output_dir / file_path.name
        shutil.move(str(file_path), str(dest_path))
        print(f"✅ Moved to {dest_path}")
        return dest_path


async def _download_and_process(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    temp_dir: Path,
    output_dir: Path,
) -> Path | None:
    url = download_item["url"]
    filename = download_item["filename"]
    file_path = temp_dir / filename

    async with semaphore:
        try:
            await _download_file(client, url, file_path)
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, _process_file, file_path, output_dir
            )
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            return None


async def run_async_downloads(
    downloads: list[dict],
    temp_dir: Path,
    output_dir: Path,
    concurrency: int = 3,
    timeout_seconds: int = 120,
) -> list[Path | None]:
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        tasks = [
            _download_and_process(semaphore, client, d, temp_dir, output_dir)
            for d in downloads
        ]
        return await asyncio.gather(*tasks)
