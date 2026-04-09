import json
from datetime import datetime
from datetime import timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


STAGING_BUCKET = "staging-current"


def get_minio_client():
    from flows_staging.shared.config import MINIO_ACCESS_KEY  # noqa: PLC0415
    from flows_staging.shared.config import MINIO_ENDPOINT  # noqa: PLC0415
    from flows_staging.shared.config import MINIO_SECRET_KEY  # noqa: PLC0415

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        use_ssl=False,
    )


def ensure_bucket_exists(bucket_name: str) -> None:
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=bucket_name)
        else:
            raise


def upload_file_to_bucket(
    file_path: Path, bucket_name: str, key: str | None = None
) -> None:
    client = get_minio_client()
    ensure_bucket_exists(bucket_name)
    client.upload_file(
        Filename=str(file_path),
        Bucket=bucket_name,
        Key=key or file_path.name,
    )


def upload_directory_to_bucket(
    directory: Path, bucket_name: str, prefix: str = ""
) -> None:
    client = get_minio_client()
    ensure_bucket_exists(bucket_name)
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(directory)
            key = f"{prefix}/{relative_path}" if prefix else str(relative_path)
            client.upload_file(
                Filename=str(file_path),
                Bucket=bucket_name,
                Key=key,
            )


def create_metadata_sidecar(
    source_url: str | None = None,
    license: str | None = None,
    publication_date: str | None = None,
    pipeline_name: str | None = None,
    prefect_run_id: str | None = None,
    http_status: int | None = None,
    notes: str = "",
) -> dict:
    return {
        "source_url": source_url,
        "license": license,
        "publication_date": publication_date,
        "collection_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_name": pipeline_name,
        "prefect_run_id": prefect_run_id,
        "http_status": http_status,
        "notes": notes,
    }


def upload_to_staging(
    file_path: Path,
    bucket_name: str,
    key: str | None = None,
    metadata: dict | None = None,
) -> None:
    client = get_minio_client()
    ensure_bucket_exists(bucket_name)

    target_key = key or file_path.name
    client.upload_file(
        Filename=str(file_path),
        Bucket=bucket_name,
        Key=target_key,
    )

    if metadata:
        sidecar_key = f"{target_key}.meta.json"
        sidecar_content = json.dumps(metadata, indent=2)
        client.put_object(
            Bucket=bucket_name,
            Key=sidecar_key,
            Body=sidecar_content.encode("utf-8"),
            ContentType="application/json",
        )


def upload_to_staging_with_download_metadata(
    file_path: Path,
    bucket_name: str,
    download_config: dict,
    pipeline_name: str | None = None,
    prefect_run_id: str | None = None,
    target_folder: str | None = None,
) -> None:
    metadata = create_metadata_sidecar(
        source_url=download_config.get("url"),
        license=None,
        publication_date=None,
        pipeline_name=pipeline_name,
        prefect_run_id=prefect_run_id,
        http_status=200,
        notes="",
    )
    key = f"{target_folder}/{file_path.name}" if target_folder else file_path.name
    upload_to_staging(file_path, bucket_name, key=key, metadata=metadata)


def upload_directory_to_staging(
    directory: Path,
    bucket_name: str,
    prefix: str = "",
    metadata: dict | None = None,
) -> list[str]:
    client = get_minio_client()
    ensure_bucket_exists(bucket_name)

    uploaded_keys = []
    for file_path in directory.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(directory)
            key = f"{prefix}/{relative_path}" if prefix else str(relative_path)

            client.upload_file(
                Filename=str(file_path),
                Bucket=bucket_name,
                Key=key,
            )
            uploaded_keys.append(key)

            if metadata:
                sidecar_key = f"{key}.meta.json"
                sidecar_content = json.dumps(metadata, indent=2)
                client.put_object(
                    Bucket=bucket_name,
                    Key=sidecar_key,
                    Body=sidecar_content.encode("utf-8"),
                    ContentType="application/json",
                )

    return uploaded_keys
