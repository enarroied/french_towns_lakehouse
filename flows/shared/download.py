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


async def _download_and_upload(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    download_item: dict,
    temp_dir: Path,
    known_hashes: dict[str, dict],
    target_folder: str | None = None,
) -> list[dict]:
    from flows.shared.minio import STAGING_BUCKET  # noqa: PLC0415
    from flows.shared.minio import ensure_bucket_exists  # noqa: PLC0415
    from flows.shared.minio import get_minio_client  # noqa: PLC0415

    url = download_item["url"]
    filename = download_item["filename"]
    file_path = temp_dir / filename

    file_records = []
    async with semaphore:
        try:
            await _download_file(client, url, file_path)
            loop = asyncio.get_event_loop()
            extracted_files = await loop.run_in_executor(
                None, _extract_file, file_path, temp_dir
            )

            minio_client = get_minio_client()
            ensure_bucket_exists(STAGING_BUCKET)

            for extracted_file in extracted_files:
                if not extracted_file.is_file():
                    continue

                size_mb = round(extracted_file.stat().st_size / 1024**2, 2)
                md5 = hashlib.md5(extracted_file.read_bytes()).hexdigest()
                base_name = extracted_file.name

                # Hash check — skip if identical to latest
                known = known_hashes.get(base_name)
                if known and known["md5"] == md5:
                    print(f"⏭️ Skipping {base_name} — hash unchanged")
                    extracted_file.unlink()
                    continue

                # Archive old file if it existed
                if known and known.get("filename_timestamp"):
                    _archive_old_file(
                        minio_client, STAGING_BUCKET, known["filename_timestamp"]
                    )

                # Upload with timestamped name
                ts_name = _timestamped_name(extracted_file)
                key = f"{target_folder}/{ts_name}" if target_folder else ts_name
                minio_client.upload_file(
                    Filename=str(extracted_file),
                    Bucket=STAGING_BUCKET,
                    Key=key,
                )
                print(f"☁️ Uploaded {ts_name} to {STAGING_BUCKET}/{key}")
                extracted_file.unlink()
                file_records.append(
                    {
                        "key": key,
                        "base_name": base_name,
                        "filename_timestamp": ts_name,
                        "size_mb": size_mb,
                        "md5": md5,
                    }
                )

            return file_records
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
