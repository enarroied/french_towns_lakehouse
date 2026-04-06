import asyncio
import hashlib
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import httpx


def _timestamped_name(file_path: Path) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{file_path.stem}_{ts}{file_path.suffix}"


def _archive_old_file(
    minio_client, staging_bucket: str, old_filename_timestamp: str
) -> None:
    """Copy old file to evidence-archive/staging/ then delete from staging."""
    archive_key = f"evidence-archive/staging/{old_filename_timestamp}"
    minio_client.copy_object(
        Bucket=staging_bucket,
        CopySource={"Bucket": staging_bucket, "Key": old_filename_timestamp},
        Key=archive_key,
    )
    minio_client.delete_object(Bucket=staging_bucket, Key=old_filename_timestamp)
    print(f"🗄️ Archived {old_filename_timestamp} → {archive_key}")


async def _download_file(
    client: httpx.AsyncClient, url: str, output_path: Path
) -> None:
    print(f"📥 Downloading {url}")
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)
    print(f"✅ Saved to {output_path}")


def _extract_file(file_path: Path, output_dir: Path) -> list[Path]:
    extracted_files = []
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
        print(f"✅ Extracted to {output_dir}")
        for name in zip_ref.namelist():
            extracted_files.append(output_dir / name)
        file_path.unlink()
        print(f"🗑️ Removed {file_path}")
    except zipfile.BadZipFile:
        dest_path = output_dir / file_path.name
        shutil.move(str(file_path), str(dest_path))
        print(f"✅ Moved to {dest_path}")
        extracted_files.append(dest_path)
    return extracted_files


# _download_and_upload helpers
def _should_skip_file(base_name: str, md5: str, known_hashes: dict[str, dict]) -> bool:
    """Check if file should be skipped due to unchanged hash."""
    known = known_hashes.get(base_name)
    return known and known["md5"] == md5


def _archive_existing_file(
    base_name: str, known_hashes: dict[str, dict], minio_client, staging_bucket: str
) -> None:
    """Archive existing file if it exists."""
    known = known_hashes.get(base_name)
    if known and known.get("filename_timestamp"):
        _archive_old_file(minio_client, staging_bucket, known["filename_timestamp"])


def _upload_extracted_file(
    extracted_file: Path,
    target_folder: str | None,
    minio_client,
    staging_bucket: str,
) -> dict:
    """Upload a single extracted file to MinIO and return its metadata."""
    size_mb = round(extracted_file.stat().st_size / 1024**2, 2)
    md5 = hashlib.md5(extracted_file.read_bytes()).hexdigest()
    base_name = extracted_file.name

    ts_name = _timestamped_name(extracted_file)
    key = f"{target_folder}/{ts_name}" if target_folder else ts_name

    minio_client.upload_file(
        Filename=str(extracted_file),
        Bucket=staging_bucket,
        Key=key,
    )
    print(f"☁️ Uploaded {ts_name} to {staging_bucket}/{key}")
    extracted_file.unlink()

    return {
        "key": key,
        "base_name": base_name,
        "filename_timestamp": ts_name,
        "size_mb": size_mb,
        "md5": md5,
    }


def _process_extracted_files(
    extracted_files: list[Path],
    known_hashes: dict[str, dict],
    minio_client,
    staging_bucket: str,
    target_folder: str | None = None,
) -> list[dict]:
    """Process all extracted files: skip unchanged, archive old, upload new."""
    file_records = []

    for extracted_file in extracted_files:
        if not extracted_file.is_file():
            continue

        base_name = extracted_file.name
        md5 = hashlib.md5(extracted_file.read_bytes()).hexdigest()

        # Skip if hash unchanged
        if _should_skip_file(base_name, md5, known_hashes):
            print(f"⏭️ Skipping {base_name} — hash unchanged")
            extracted_file.unlink()
            continue

        # Archive existing file
        _archive_existing_file(base_name, known_hashes, minio_client, staging_bucket)

        # Upload new file
        record = _upload_extracted_file(
            extracted_file, target_folder, minio_client, staging_bucket
        )
        file_records.append(record)

    return file_records


def _setup_minio() -> tuple:
    """Setup MinIO client and ensure staging bucket exists."""
    from flows.shared.minio import STAGING_BUCKET  # noqa: PLC0415
    from flows.shared.minio import ensure_bucket_exists  # noqa: PLC0415
    from flows.shared.minio import get_minio_client  # noqa: PLC0415

    minio_client = get_minio_client()
    ensure_bucket_exists(STAGING_BUCKET)
    return minio_client, STAGING_BUCKET


async def _download_and_upload(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    temp_dir: Path,
    known_hashes: dict[str, dict],
    target_folder: str | None = None,
) -> list[dict]:
    url = download_item["url"]
    filename = download_item["filename"]
    file_path = temp_dir / filename

    async with semaphore:
        try:
            await _download_file(client, url, file_path)
            loop = asyncio.get_event_loop()
            extracted_files = await loop.run_in_executor(
                None, _extract_file, file_path, temp_dir
            )

            minio_client, staging_bucket = _setup_minio()

            return await loop.run_in_executor(
                None,
                _process_extracted_files,
                extracted_files,
                known_hashes,
                minio_client,
                staging_bucket,
                target_folder,
            )

        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            return []


async def run_async_downloads_to_minio(
    downloads: list[dict],
    temp_dir: Path,
    known_hashes: dict[str, dict],
    concurrency: int = 3,
    timeout_seconds: int = 120,
) -> dict[str, list[dict]]:
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        tasks = [
            _download_and_upload(
                semaphore,
                client,
                d,
                temp_dir,
                known_hashes=known_hashes,
                target_folder=d.get("target_folder"),
            )
            for d in downloads
        ]
        results = await asyncio.gather(*tasks)

    return {
        download["name"]: records
        for download, records in zip(downloads, results, strict=True)
    }
