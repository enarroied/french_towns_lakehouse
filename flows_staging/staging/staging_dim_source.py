"""Stage dim_source CSVs to MinIO (bronze layer).

Reads generated CSVs from data_sources/dim_source/ and uploads them to
the staging bucket using _process_single_file.
"""

import csv
import tempfile
from pathlib import Path
from typing import cast

from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.download import write_csv_for_staging
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file
from prefect import flow
from prefect import task


SOURCE_DIR = Path("data_sources/dim_source")
TARGET_FOLDER = "dim_source"

FILES = [
    {
        "base_name": "sources",
        "csv_path": SOURCE_DIR / "sources.csv",
        "fieldnames": [
            "source_id",
            "source_name",
            "source_label",
            "organization",
            "domain",
            "reference_url",
            "license",
            "description",
        ],
    },
    {
        "base_name": "bridge_model_sources",
        "csv_path": SOURCE_DIR / "bridge_model_sources.csv",
        "fieldnames": ["model_name", "source_id"],
    },
]


def _read_csv(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@task
def stage_source_files(run_id: str) -> int:
    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    staged_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)

        for f in FILES:
            base_name = cast(str, f["base_name"])
            csv_path = cast(Path, f["csv_path"])
            fieldnames = cast(list[str], f["fieldnames"])

            data = list(_read_csv(csv_path))
            write_csv_for_staging(
                data=data,
                fieldnames=fieldnames,
                base_name=base_name,
                temp_dir=temp_path,
            )

            stage_config = StageConfig(
                name=base_name,
                url="",
                target_folder=TARGET_FOLDER,
                run_id=run_id,
                staging_bucket=staging_bucket,
                evidence_bucket=evidence_bucket,
            )

            if _process_single_file(
                stage_config, minio_client, base_name, ".csv", temp_path
            ):
                staged_count += 1

    return staged_count


@flow(name="staging_dim_source")
def staging_dim_source() -> None:
    preflight()
    run_id = init_run(domain="dim_source", layer="STAGING", technical_type="DOWNLOAD")
    try:
        staged = stage_source_files(run_id)
        finalize_run(run_id=run_id, status="SUCCESS", number_files=staged)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_dim_source()
