import asyncio
import csv
import hashlib
import re
import shutil
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from flows_staging.scrapers.models import FileMetadata
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import AsyncDownloadParams
from flows_staging.shared.models import KnownHashes
from prefect import get_run_logger


def _log(level: str, message: str, logger: Any | None = None) -> None:
    """Log a message using Prefect logger if available, else print.

    Args:
        level: Log level ('info', 'warning', 'error')
        message: Message to log
        logger: Optional Prefect logger to use. If None, tries to get from Prefect context.
    """
    if logger:
        getattr(logger, level)(message)
    else:
        try:
            logger = get_run_logger()
            getattr(logger, level)(message)
        except Exception:
            timestamp = datetime.now().isoformat()
            print(f"[{timestamp}] {message}")


EVIDENCE_BUCKET = "evidence-archive"
TEMP_DOWNLOAD_DIR = Path("/tmp/french_towns_downloads")


def _get_timestamp() -> str:
    """Generate current timestamp in YYYYMMDD_HHMMSS format."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _timestamped_csv_name(base_name: str) -> str:
    """Generate a timestamped CSV filename from a base name (e.g., 'villes_fleuries')."""
    ts = _get_timestamp()
    return f"{base_name}_{ts}.csv"


def _timestamped_from_base(base_name: str) -> str:
    """Generate a timestamped filename from a base name. Skips if already has timestamp."""
    if re.search(r"_\d{8}_\d{6}", base_name):
        return base_name

    ts = _get_timestamp()
    path = Path(base_name)
    stem = path.stem if path.stem else "file"
    ext = path.suffix if path.suffix else ""
    return f"{stem}_{ts}{ext}"


def calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file using chunked reading for memory efficiency."""
    md5_hash = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def write_csv_to_temp(
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


def upload_scraper_output(
    data: list[dict],
    fieldnames: list[str],
    scraper_name: str,
    target_folder: str,
    known_hashes: KnownHashes,
    source_url: str | None = None,
    temp_dir: Path = TEMP_DOWNLOAD_DIR,
) -> FileMetadata | None:
    """
    Write data to CSV, check hash against known_hashes, upload to MinIO, and cleanup.
    Returns None if hash unchanged (file should be skipped), FileMetadata on success.
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = write_csv_to_temp(data, fieldnames, scraper_name, temp_dir)

    md5 = calculate_md5(csv_path)
    base_name = f"{scraper_name}.csv"

    if _should_skip_file(base_name, md5, known_hashes):
        _log("info", f"⏭️ Skipping {scraper_name} — hash unchanged")
        csv_path.unlink()
        return None

    minio_client = get_minio_client()
    key = f"{target_folder}/{csv_path.name}"
    size_mb = round(csv_path.stat().st_size / 1024**2, 2)

    _upload_file(csv_path, minio_client, "staging-current", key)

    return FileMetadata(
        key=key,
        base_name=base_name,
        filename_timestamp=csv_path.name,
        size_mb=size_mb,
        md5=md5,
        source_url=source_url,
    )


def _archive_old_file(
    minio_client: Any,
    staging_bucket: str,
    old_file_location: str,
    logger: Any | None = None,
) -> None:
    """Copy old file from staging bucket to evidence-archive bucket, then delete from staging."""
    archive_key = old_file_location
    minio_client.copy_object(
        Bucket=EVIDENCE_BUCKET,
        CopySource=f"/{staging_bucket}/{old_file_location}",
        Key=archive_key,
    )
    minio_client.delete_object(Bucket=staging_bucket, Key=old_file_location)
    _log(
        "info",
        f"🗄️ Archived {old_file_location} → {EVIDENCE_BUCKET}/{archive_key}",
        logger=logger,
    )


async def _download_file(
    client: httpx.AsyncClient,
    url: str,
    output_path: Path,
    logger: Any | None = None,
) -> None:
    """Download a file from URL to the specified output path."""
    _log("info", f"📥 Downloading {url}", logger=logger)
    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, output_path.write_bytes, response.content)
    _log("info", f"✅ Saved to {output_path}", logger=logger)


def _extract_file(
    file_path: Path,
    output_dir: Path,
    logger: Any | None = None,
) -> list[Path]:
    """Extract zip file contents to output directory. Non-zip files are moved instead."""
    extracted_files: list[Path] = []
    try:
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)
        _log("info", f"✅ Extracted to {output_dir}", logger=logger)
        for name in zip_ref.namelist():
            extracted_files.append(output_dir / name)
        file_path.unlink()
        _log("info", f"🗑️ Removed {file_path}", logger=logger)
    except zipfile.BadZipFile:
        dest_path = output_dir / file_path.name
        shutil.move(str(file_path), str(dest_path))
        _log("info", f"✅ Moved to {dest_path}", logger=logger)
        extracted_files.append(dest_path)
    return extracted_files


def _should_skip_file(base_name: str, md5: str, known_hashes: KnownHashes) -> bool:
    """Check if file should be skipped due to unchanged hash."""
    known_file = known_hashes.get(base_name)
    return known_file is not None and known_file.md5 == md5


def _get_file_location(base_name: str, known_hashes: KnownHashes) -> str | None:
    """Return MinIO key (file_location) of existing file, or None if none exists."""
    known_file = known_hashes.get(base_name)
    return known_file.file_location if known_file else None


def _upload_file(
    file_path: Path,
    minio_client: Any,
    staging_bucket: str,
    key: str,
    logger: Any | None = None,
) -> None:
    """Upload a file to MinIO."""
    minio_client.upload_file(
        Filename=str(file_path),
        Bucket=staging_bucket,
        Key=key,
    )
    _log("info", f"☁️ Uploaded {key} to {staging_bucket}", logger=logger)
    file_path.unlink()


def _create_file_metadata(
    file_path: Path,
    base_name: str,
    target_folder: str | None,
    md5: str,
    source_url: str | None = None,
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
        source_url=source_url,
    )


def _process_extracted_files(
    extracted_files: list[Path],
    known_hashes: KnownHashes,
    minio_client: Any,
    staging_bucket: str,
    base_name: str,
    target_folder: str | None = None,
    source_url: str | None = None,
    logger: Any | None = None,
) -> list[FileMetadata]:
    """Process all extracted files: skip unchanged, archive old, upload new."""
    file_records: list[FileMetadata] = []

    for extracted_file in extracted_files:
        if not extracted_file.is_file():
            continue

        original_name = extracted_file.name
        if original_name.endswith(".geojson"):
            renamed_file = extracted_file
        else:
            timestamped_name = _timestamped_from_base(original_name)
            timestamped_path = extracted_file.parent / timestamped_name
            extracted_file.rename(timestamped_path)
            renamed_file = timestamped_path

        md5 = calculate_md5(renamed_file)

        if _should_skip_file(base_name, md5, known_hashes):
            _log("info", f"⏭️ Skipping {base_name} — hash unchanged", logger=logger)
            renamed_file.unlink(missing_ok=True)
            continue

        existing_location = _get_file_location(base_name, known_hashes)
        if existing_location:
            _archive_old_file(
                minio_client, staging_bucket, existing_location, logger=logger
            )

        key = (
            f"{target_folder}/{renamed_file.name}"
            if target_folder
            else renamed_file.name
        )

        metadata = _create_file_metadata(
            renamed_file, base_name, target_folder, md5, source_url
        )
        metadata.key = key
        _upload_file(renamed_file, minio_client, staging_bucket, key, logger=logger)

        file_records.append(metadata)

    return file_records


async def _download_and_upload(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    temp_dir: Path,
    known_hashes: KnownHashes,
    minio_client: Any,
    staging_bucket: str,
    target_folder: str | None = None,
    logger: Any | None = None,
) -> list[FileMetadata]:
    """Download a file, extract it, and upload to MinIO."""
    url = download_item.get("url")
    base_name = download_item.get("name", "unknown")
    ts = _get_timestamp()
    if base_name == "french_communes":
        ext = ".geojson"
    else:
        ext = Path(url).suffix if url else ""
    filename = f"{base_name}_{ts}{ext}" if url else "unknown"
    download_dir = temp_dir / uuid.uuid4().hex
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / filename

    if url is None:
        _log("warning", "⚠️ No URL provided for download item, skipping", logger=logger)
        return []

    async with semaphore:
        try:
            await _download_file(client, url, file_path, logger=logger)
            loop = asyncio.get_event_loop()
            extracted_files = await loop.run_in_executor(
                None, _extract_file, file_path, download_dir, logger
            )

            return await loop.run_in_executor(
                None,
                _process_extracted_files,
                extracted_files,
                known_hashes,
                minio_client,
                staging_bucket,
                base_name,
                target_folder,
                url,
                logger,
            )

        except httpx.HTTPStatusError as e:
            _log("error", f"❌ HTTP error downloading {filename}: {e}", logger=logger)
            return []
        except Exception as e:
            _log("error", f"❌ Failed to download {filename}: {e}", logger=logger)
            return []
        finally:
            shutil.rmtree(download_dir, ignore_errors=True)


async def run_async_downloads_to_minio(
    params: AsyncDownloadParams,
) -> dict[str, list[FileMetadata]]:
    """Run multiple downloads concurrently and upload extracted files to MinIO.

    Args:
        params: AsyncDownloadParams containing download configuration.

    Returns:
        Dict mapping download name to list of FileMetadata records.
    """
    semaphore = asyncio.Semaphore(params.concurrency)
    async with httpx.AsyncClient(timeout=params.timeout_seconds) as client:
        tasks = [
            _download_and_upload(
                semaphore=semaphore,
                client=client,
                download_item=d,
                temp_dir=params.temp_dir,
                known_hashes=params.known_hashes,
                minio_client=params.minio_client,
                staging_bucket=params.staging_bucket,
                target_folder=d.get("target_folder"),
                logger=params.logger,
            )
            for d in params.downloads
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    return {
        download["name"]: result if isinstance(result, list) else []
        for download, result in zip(params.downloads, results, strict=True)
    }
