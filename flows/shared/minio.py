import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def get_minio_client():
    from flows.shared.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

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
