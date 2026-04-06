import asyncio
import hashlib
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx


ARCHIVE_PREFIX = "evidence-archive/"


@dataclass
class FileMetadata:
    key: str
    base_name: str
    filename_timestamp: str
    size_mb: float
    md5: str


def _timestamped_name(file_path: Path) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{file_path.stem}_{ts}{file_path.suffix}"


def calculate_md5(file_path: Path) -> str:
    md5 = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
    return md5.hexdigest()


EVIDENCE_BUCKET = "evidence-archive"


def _archive_old_file(
    minio_client, staging_bucket: str, old_file_location: str
) -> None:
    """Copy old file from staging to evidence-archive bucket, then delete from staging."""
    archive_key = old_file_location
    minio_client.copy_object(
        Bucket=EVIDENCE_BUCKET,
        CopySource=f"/{staging_bucket}/{old_file_location}",
        Key=archive_key,
    )
    minio_client.delete_object(Bucket=staging_bucket, Key=old_file_location)
    print(f"🗄️ Archived {old_file_location} → {EVIDENCE_BUCKET}/{archive_key}")


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
def _should_skip_file(base_name: str, md5: str, known_hashes: dict) -> bool:
    """Check if file should be skipped due to unchanged hash."""
    return known_hashes.get(base_name, {}).get("md5") == md5


def _get_file_location(base_name: str, known_hashes: dict) -> str | None:
    """Return MinIO key (file_location) of existing file, or None if none exists."""
    return known_hashes.get(base_name, {}).get("file_location")


def _upload_extracted_file(
    extracted_file: Path,
    target_folder: str | None,
    minio_client,
    staging_bucket: str,
) -> FileMetadata:
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

    return FileMetadata(
        key=key,
        base_name=base_name,
        filename_timestamp=ts_name,
        size_mb=size_mb,
        md5=md5,
    )


def _process_extracted_files(
    extracted_files: list[Path],
    known_hashes: dict,
    minio_client,
    staging_bucket: str,
    target_folder: str | None = None,
) -> list[FileMetadata]:
    """Process all extracted files: skip unchanged, archive old, upload new."""
    file_records = []

    print(f"DEBUG: known_hashes keys = {list(known_hashes.keys())}", flush=True)
    print(f"DEBUG: extracted_files = {extracted_files}", flush=True)

    for extracted_file in extracted_files:
        print(
            f"DEBUG: checking {extracted_file}, exists={extracted_file.exists()}, is_file={extracted_file.is_file()}",
            flush=True,
        )
        if not extracted_file.is_file():
            print(f"DEBUG: SKIPPING {extracted_file} - not a file", flush=True)
            continue

        base_name = extracted_file.name
        md5 = hashlib.md5(extracted_file.read_bytes()).hexdigest()

        known = known_hashes.get(base_name)
        known_md5 = known.get("md5") if known else None
        print(
            f"DEBUG: base_name={base_name}, calc_md5={md5}, known_md5={known_md5}",
            flush=True,
        )

        if _should_skip_file(base_name, md5, known_hashes):
            print(f"DEBUG: SKIPPING {base_name} — hash unchanged", flush=True)
            print(f"⏭️ Skipping {base_name} — hash unchanged")
            extracted_file.unlink()
            continue

        existing_location = _get_file_location(base_name, known_hashes)
        print(f"DEBUG: existing_location = {existing_location}", flush=True)
        if existing_location:
            _archive_old_file(minio_client, staging_bucket, existing_location)

        record = _upload_extracted_file(
            extracted_file, target_folder, minio_client, staging_bucket
        )
        file_records.append(record)

    print(f"DEBUG: returning {len(file_records)} records", flush=True)
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

    print(f"DEBUG _download_and_upload: processing {filename} from {url}", flush=True)

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
    print(
        f"DEBUG run_async_downloads_to_minio: {len(downloads)} downloads, {len(known_hashes)} known_hashes",
        flush=True,
    )
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
