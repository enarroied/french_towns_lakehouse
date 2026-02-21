import asyncio
import os
import subprocess
from pathlib import Path

import boto3
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv
from prefect import flow, task
from scripts.download import main as download_files

load_dotenv(find_dotenv())

with open("config.yaml") as f:
    config = yaml.safe_load(f)

PATHS = config["paths"]
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

DBT_PROJECT_DIR = Path("french_towns_dbt")


@task
def create_required_dirs():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    for dir_key in config.get("directories", []):
        path = Path(config["paths"][dir_key])
        path.mkdir(exist_ok=True, parents=True)


@task
def download_all_files():
    """Task 1: Download all source files using asyncio"""
    asyncio.run(download_files())
    files = list(Path(PATHS["input_dir"]).glob("*"))
    return files


@task
def run_dbt():
    """Task 2: Run all dbt models to produce parquet files in data/processed"""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=DBT_PROJECT_DIR,
        capture_output=False,  # let dbt print directly to stdout so you see progress
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError("dbt run failed — check logs above")

    print("✅ dbt run complete")


@task
def upload_to_minio(
    bucket_name: str = "lakehouse-processed",
    source_dir: str = "data/processed",
    endpoint_url: str = MINIO_ENDPOINT,
    access_key: str = MINIO_ACCESS_KEY,
    secret_key: str = MINIO_SECRET_KEY,
    use_ssl: bool = False,
):
    """Task 3: Upload processed parquet files to MinIO bucket"""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        use_ssl=use_ssl,
    )

    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=bucket_name)
            print(f"📦 Created bucket: {bucket_name}")
        else:
            raise e

    source_path = Path(source_dir)
    for file_path in source_path.glob("*.parquet"):
        client.upload_file(
            Filename=str(file_path), Bucket=bucket_name, Key=file_path.name
        )
        print(f"✅ Uploaded {file_path.name} to {bucket_name}")


@flow
def french_towns_pipeline():
    """Main orchestration flow"""

    # Step 1: Create dirs and download
    create_required_dirs()
    download_all_files()

    # Step 2: Transform via dbt
    run_dbt()

    # Step 3: Upload to MinIO
    upload_to_minio(
        bucket_name="lakehouse-processed",
        source_dir=PATHS["output_dir"],
    )

    print("🎉 Pipeline complete!")


if __name__ == "__main__":
    french_towns_pipeline()
