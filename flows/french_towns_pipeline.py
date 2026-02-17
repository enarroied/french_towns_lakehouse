import asyncio
import os
from pathlib import Path

import boto3
import yaml
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import find_dotenv, load_dotenv
from jinja2 import Template
from prefect import flow, task
from scripts.download import main as download_files
from utils.db import DuckDBConnection

# This finds the nearest .env by walking up directories
load_dotenv(find_dotenv())


with open("config.yaml") as f:
    config = yaml.safe_load(f)

PATHS = config["paths"]
TRANSFORMATIONS = config["transformations"]
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")


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
    # Run async download in sync context
    asyncio.run(download_files())
    files = list(Path(PATHS["input_dir"]).glob("*"))
    return files


@task
def transform_file(input_file: Path, transform_config: dict) -> Path:
    """Task 2: Transform a single file using SQL template"""

    output_file = Path(PATHS["output_dir"]) / transform_config["output_file"]

    render_context = {
        "input_file": input_file,
        "output_file": output_file,
        "input_dir": PATHS["input_dir"],
        "output_dir": PATHS["output_dir"],
        "temp_dir": PATHS["temp_dir"],
    }

    if "params" in transform_config:
        # Transform the params to include the full path!
        params = transform_config["params"].copy()
        for key, value in params.items():
            if key.endswith("_file"):  # Any param ending with '_file'
                params[key] = str(Path(PATHS["input_dir"]) / value)
        render_context.update(params)

    sql_path = Path(PATHS["sql_dir"]) / transform_config["sql_template"]
    with open(sql_path) as f:
        template = Template(f.read())

    query = template.render(**render_context)

    con = DuckDBConnection.get()
    con.execute(query)

    return output_file


@task
def upload_to_minio(
    bucket_name: str = "lakehouse-processed",
    source_dir: str = "data/processed",
    endpoint_url: str = MINIO_ENDPOINT,
    access_key: str = MINIO_ACCESS_KEY,
    secret_key: str = MINIO_SECRET_KEY,
    use_ssl: bool = False,
):
    """Upload processed files to MinIO bucket (overwrite existing)"""

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

    # upload all parquet/geoparquet files
    source_path = Path(source_dir)
    for file_path in source_path.glob("*.parquet") or source_path.glob("*.geoparquet"):
        client.upload_file(
            Filename=str(file_path), Bucket=bucket_name, Key=file_path.name
        )
        print(f"✅ Uploaded {file_path.name} to {bucket_name}")


@flow
def french_towns_pipeline():
    """Main orchestration flow"""

    # Step 1: Download all files
    create_required_dirs()
    input_files = download_all_files()

    # Step 2: For each transformation, find matching files and process
    for transform in TRANSFORMATIONS:
        pattern = transform["input_pattern"]
        matching_files = [f for f in input_files if f.match(pattern)]

        if not matching_files:
            print(f"⚠️ No files match pattern: {pattern}")
            continue

        for file in matching_files:
            transform_file(file, transform)

    # Step 2: send to minio/s3
    upload_to_minio(
        bucket_name="lakehouse-processed",
        source_dir=PATHS["output_dir"],
    )

    DuckDBConnection.close()
    print("🎉 Pipeline complete!")


if __name__ == "__main__":
    french_towns_pipeline()
