"""Download criminality Parquet from data.gouv.fr and upload to S3 staging."""

import os
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import find_dotenv
from dotenv import load_dotenv


load_dotenv(find_dotenv())

CRIME_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/604d71b8-337d-4869-9226-49e01bae87df"
)
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
STAGING_BUCKET = "staging-current"
TARGET_FOLDER = "criminality"
BASE_NAME = "delinquance"

LOCAL_PATH = Path(f"/tmp/{BASE_NAME}.parquet")


def download_parquet() -> None:
    print(f"Downloading from {CRIME_URL} …")
    urllib.request.urlretrieve(CRIME_URL, LOCAL_PATH)
    size_mb = LOCAL_PATH.stat().st_size / (1024 * 1024)
    print(f"  Downloaded → {LOCAL_PATH} ({size_mb:.1f} MB)")


def upload_to_minio() -> None:
    s3_key = f"{TARGET_FOLDER}/{BASE_NAME}.parquet"
    print(f"Uploading to s3://{STAGING_BUCKET}/{s3_key} …")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        use_ssl=False,
    )
    try:
        s3.head_bucket(Bucket=STAGING_BUCKET)
    except Exception:
        s3.create_bucket(Bucket=STAGING_BUCKET)
    s3.upload_file(str(LOCAL_PATH), STAGING_BUCKET, s3_key)
    print("  ✅ Uploaded successfully")


def main() -> None:
    download_parquet()
    upload_to_minio()
    LOCAL_PATH.unlink()
    print("Done.")


if __name__ == "__main__":
    main()
