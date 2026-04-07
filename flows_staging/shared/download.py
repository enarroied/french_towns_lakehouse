import asyncio
import csv
import hashlib
import shutil
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from flows_staging.scrapers.models import FileMetadata


ARCHIVE_PREFIX = "evidence-archive/"
EVIDENCE_BUCKET = "evidence-archive"


def _timestamped_name(file_path: Path) -> str:
    """Generate a timestamped filename while preserving the original stem and suffix."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{file_path.stem}_{ts}{file_path.suffix}"


def _timestamped_csv_name(base_name: str) -> str:
    """Generate a timestamped CSV filename from a base name (e.g., 'villes_fleuries')."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{ts}.csv"


def calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file using chunked reading for memory efficiency."""
    md5_hash = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def _write_csv_to_temp(
    data: list[dict],
    fieldnames: list[str],
    base_name: str,
    temp_dir: Path,
) -> Path:
    """Write data to a timestamped CSV file in temp directory. Returns the file path."""
    csv_path = temp_dir / _timestamped_csv_name(base_name)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    return csv_path


def _archive_old_file(
    minio_client: Any,
    staging_bucket: str,
    old_file_location: str,
) -> None:
    """Copy old file from staging bucket to evidence-archive bucket, then delete from staging."""
    archive_key = old_file_location
    minio_client.copy_object(
        Bucket=EVIDENCE_BUCKET,
        CopySource=f"/{staging_bucket}/{old_file_location}",
        Key=archive_key,
    )
    minio_client.delete_object(Bucket=staging_bucket, Key=old_file_location)
    print(f"🗄️ Archived {old_file_location} → {EVIDENCE_BUCKET}/{archive_key}")


async def _download_file(
    client: httpx.AsyncClient,
    url: str,
    output_path: Path,
) -> None:
    """Download a file from URL to the specified output path."""
    print(f"📥 Downloading {url}")
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)
    print(f"✅ Saved to {output_path}")


def _extract_file(file_path: Path, output_dir: Path) -> list[Path]:
    """Extract zip file contents to output directory. Non-zip files are moved instead."""
    extracted_files: list[Path] = []
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


def _should_skip_file(base_name: str, md5: str, known_hashes: dict) -> bool:
    """Check if file should be skipped due to unchanged hash."""
    return known_hashes.get(base_name, {}).get("md5") == md5


def _get_file_location(base_name: str, known_hashes: dict) -> str | None:
    """Return MinIO key (file_location) of existing file, or None if none exists."""
    return known_hashes.get(base_name, {}).get("file_location")


def _upload_file(
    file_path: Path,
    minio_client: Any,
    staging_bucket: str,
    key: str,
) -> None:
    """Upload a file to MinIO."""
    minio_client.upload_file(
        Filename=str(file_path),
        Bucket=staging_bucket,
        Key=key,
    )
    print(f"☁️ Uploaded {key} to {staging_bucket}")
    file_path.unlink()


def _create_file_metadata(
    file_path: Path,
    base_name: str,
    target_folder: str | None,
    md5: str,
) -> FileMetadata:
    """Create FileMetadata from a file path."""
    size_mb = round(file_path.stat().st_size / 1024**2, 2)
    filename_timestamp = file_path.name
    key = (
        f"{target_folder}/{filename_timestamp}" if target_folder else filename_timestamp
    )

    return FileMetadata(
        key=key,
        base_name=base_name,
        filename_timestamp=filename_timestamp,
        size_mb=size_mb,
        md5=md5,
    )


def _process_extracted_files(
    extracted_files: list[Path],
    known_hashes: dict,
    minio_client: Any,
    staging_bucket: str,
    target_folder: str | None = None,
) -> list[FileMetadata]:
    """Process all extracted files: skip unchanged, archive old, upload new."""
    file_records: list[FileMetadata] = []

    for extracted_file in extracted_files:
        if not extracted_file.is_file():
            continue

        base_name = extracted_file.name
        md5 = calculate_md5(extracted_file)

        if _should_skip_file(base_name, md5, known_hashes):
            print(f"⏭️ Skipping {base_name} — hash unchanged")
            extracted_file.unlink(missing_ok=True)
            continue

        existing_location = _get_file_location(base_name, known_hashes)
        if existing_location:
            _archive_old_file(minio_client, staging_bucket, existing_location)

        key = (
            f"{target_folder}/{extracted_file.name}"
            if target_folder
            else extracted_file.name
        )

        metadata = _create_file_metadata(extracted_file, base_name, target_folder, md5)
        metadata.key = key
        _upload_file(extracted_file, minio_client, staging_bucket, key)

        file_records.append(metadata)

    return file_records


async def _download_and_upload(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    temp_dir: Path,
    known_hashes: dict,
    minio_client: Any,
    staging_bucket: str,
    target_folder: str | None = None,
) -> list[FileMetadata]:
    """Download a file, extract it, and upload to MinIO."""
    url = download_item.get("url")
    filename = download_item.get("filename", url.split("/")[-1] if url else "unknown")
    download_dir = temp_dir / uuid.uuid4().hex
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / filename

    async with semaphore:
        try:
            await _download_file(client, url, file_path)
            loop = asyncio.get_event_loop()
            extracted_files = await loop.run_in_executor(
                None, _extract_file, file_path, download_dir
            )

            records = await loop.run_in_executor(
                None,
                _process_extracted_files,
                extracted_files,
                known_hashes,
                minio_client,
                staging_bucket,
                target_folder,
            )
            return records

        except httpx.HTTPStatusError as e:
            print(f"❌ HTTP error downloading {filename}: {e}")
            return []
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            return []
        except Exception as e:
            import traceback

            print(f"❌ Failed to download {filename}: {e}")
            print(f"❌ Exception type: {type(e).__name__}")
            print(f"❌ Traceback: {traceback.format_exc()}")
            return []
        finally:
            shutil.rmtree(download_dir, ignore_errors=True)


async def run_async_downloads_to_minio(
    downloads: list[dict],
    temp_dir: Path,
    known_hashes: dict,
    minio_client: Any,
    staging_bucket: str,
    concurrency: int = 3,
    timeout_seconds: int = 120,
) -> dict[str, list[FileMetadata]]:
    """Run multiple downloads concurrently and upload extracted files to MinIO."""
    semaphore = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        tasks = [
            _download_and_upload(
                semaphore=semaphore,
                client=client,
                download_item=d,
                temp_dir=temp_dir,
                known_hashes=known_hashes,
                minio_client=minio_client,
                staging_bucket=staging_bucket,
                target_folder=d.get("target_folder"),
            )
            for d in downloads
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    return {
        download["name"]: result if isinstance(result, list) else []
        for download, result in zip(downloads, results, strict=True)
    }
