"""Stage the dim_equipment reference CSV to MinIO (bronze layer).

Copies the static CSV from data_sources/dim_equipment/ to the staging
bucket using the shared _process_single_file mechanism (MD5 check,
archive old version, upload, audit trail).
"""

import csv
import tempfile
from pathlib import Path

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


SOURCE_CSV = Path("data_sources/dim_equipment/dim_equipment.csv")
TARGET_FOLDER = "dim_equipment"


@task
def stage_reference_data(run_id: str) -> bool:
    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    stage_config = StageConfig(
        name="dim_equipment",
        url="",  # local file, no source URL
        target_folder=TARGET_FOLDER,
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        write_csv_for_staging(
            data=list(_read_csv()),
            fieldnames=[
                "equipment_code",
                "equipment_name",
                "subdomain_code",
                "subdomain_name",
                "domain_code",
                "domain_name",
            ],
            base_name="dim_equipment",
            temp_dir=temp_path,
        )
        return _process_single_file(
            stage_config, minio_client, "dim_equipment", ".csv", temp_path
        )


def _read_csv() -> list[dict]:
    with SOURCE_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@flow(name="staging_dim_equipment")
def staging_dim_equipment() -> None:
    preflight()
    run_id = init_run(
        domain="dim_equipment", layer="STAGING", technical_type="DOWNLOAD"
    )
    try:
        staged = stage_reference_data(run_id)
        finalize_run(run_id=run_id, status="SUCCESS", number_files=1 if staged else 0)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_dim_equipment()
