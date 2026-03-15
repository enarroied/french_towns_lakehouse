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
from scrapers import run_all_scrapers
from custom_parsers import run_all_custom_parsers

load_dotenv(find_dotenv())

with open("config.yaml") as f:
    config = yaml.safe_load(f)

PATHS = config["paths"]
DIRECTORIES = config.get("directories", [])
DBT_PROJECT_DIR = Path("french_towns_dbt")
DBT_PROFILES_ARGS = ["--profiles-dir", "."]
MINIO_BUCKET = "lakehouse-processed"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")


def _run_dbt_command(args: list[str], failure_message: str) -> None:
    result = subprocess.run(
        ["dbt"] + args + DBT_PROFILES_ARGS,
        cwd=DBT_PROJECT_DIR,
        capture_output=False,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(failure_message)


@task
def create_required_dirs() -> None:
    for dir_key in DIRECTORIES:
        Path(config["paths"][dir_key]).mkdir(exist_ok=True, parents=True)


@task
def download_all_files() -> None:
    asyncio.run(download_files())


@task
def run_scrapers() -> None:
    asyncio.run(run_all_scrapers(config))


@task
def run_custom_parsers() -> None:
    run_all_custom_parsers(config)


@task
def run_dbt() -> None:
    _run_dbt_command(
        ["run-operation", "stage_external_sources"],
        "dbt stage_external_sources failed — check logs above",
    )
    _run_dbt_command(
        ["run"],
        "dbt run failed — check logs above",
    )
    _run_dbt_command(
        ["test"],
        "dbt test failed — check logs above",
    )


@task
def upload_to_minio() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        use_ssl=False,
    )

    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            client.create_bucket(Bucket=MINIO_BUCKET)
        else:
            raise

    for file_path in Path(PATHS["output_dir"]).glob("*.parquet"):
        client.upload_file(
            Filename=str(file_path),
            Bucket=MINIO_BUCKET,
            Key=file_path.name,
        )


@flow
def french_towns_pipeline() -> None:
    create_required_dirs()
    download_all_files()
    run_scrapers()
    run_custom_parsers()
    run_dbt()
    upload_to_minio()


if __name__ == "__main__":
    french_towns_pipeline()
