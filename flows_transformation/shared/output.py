import hashlib
from pathlib import Path

from flows.shared import log
from flows_staging.scrapers.models import FileMetadata
from flows_staging.shared.audit import log_upload
from flows_staging.shared.minio import get_minio_client
from prefect import task


EVIDENCE_BUCKET = "evidence-archive"


def calculate_md5(file_path: Path) -> str:
    md5_hash = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def _archive_old_file(minio_client, bucket: str, key: str) -> None:
    archive_key = key
    minio_client.copy_object(
        Bucket=EVIDENCE_BUCKET,
        CopySource=f"/{bucket}/{key}",
        Key=archive_key,
    )
    minio_client.delete_object(Bucket=bucket, Key=key)
    log("info", f"🗄️ Archived {key} → {EVIDENCE_BUCKET}/{archive_key}")


@task
def handle_outputs(model_names: list[str], run_id: str) -> None:
    """Handle dbt output files: archive old, calculate md5, log to audit DB via log_upload."""
    minio_client = get_minio_client()
    validated_bucket = "validated"

    for model_name in model_names:
        prefix = f"{model_name}.parquet"

        response = minio_client.list_objects(Bucket=validated_bucket, Prefix=prefix)
        existing = response.get("Contents", [])
        if not existing:
            log("warning", f"No output files found for {model_name}")
            continue

        current_file = existing[0]["Key"]
        tmp_path = Path(f"/tmp/{prefix}")

        minio_client.download_file(
            Bucket=validated_bucket, Key=current_file, Filename=str(tmp_path)
        )

        size_mb = round(tmp_path.stat().st_size / 1024**2, 2)
        md5 = calculate_md5(tmp_path)
        tmp_path.unlink()

        existing_archives = minio_client.list_objects(
            Bucket=EVIDENCE_BUCKET, Prefix=prefix
        ).get("Contents", [])
        if existing_archives:
            _archive_old_file(minio_client, validated_bucket, current_file)

        file_metadata = FileMetadata(
            key=current_file,
            base_name=prefix,
            filename_timestamp=current_file,
            size_mb=size_mb,
            md5=md5,
            source_url=None,
        )
        log_upload(run_id=run_id, file_metadata=file_metadata, bucket=validated_bucket)

        log("info", f"✅ {prefix} → {size_mb}MB | md5: {md5}")
