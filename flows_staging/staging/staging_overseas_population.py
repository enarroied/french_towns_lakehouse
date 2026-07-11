"""Stage the overseas population reference CSV to MinIO (bronze layer).

Copies the static CSV from data_sources/overseas_population/ to the staging
bucket using the shared _process_single_file mechanism.
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


SOURCE_CSV = Path("data_sources/overseas_population/overseas_population.csv")
TARGET_FOLDER = "demographics"


@task
def stage_reference_data(run_id: str) -> bool:
    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    stage_config = StageConfig(
        name="overseas_population",
        url="",
        target_folder=TARGET_FOLDER,
        run_id=run_id,
        staging_bucket=staging_bucket,
        evidence_bucket=evidence_bucket,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        write_csv_for_staging(
            data=list(_read_csv()),
            fieldnames=["commune_id", "year", "population"],
            base_name="overseas_population",
            temp_dir=temp_path,
        )
        return _process_single_file(
            stage_config, minio_client, "overseas_population", ".csv", temp_path
        )


def _read_csv() -> list[dict]:
    with SOURCE_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@flow(name="staging_overseas_population")
def staging_overseas_population() -> None:
    preflight()
    run_id = init_run(
        domain="demographics",
        layer="STAGING",
        technical_type="DOWNLOAD",
    )
    try:
        staged = stage_reference_data(run_id)
        finalize_run(run_id=run_id, status="SUCCESS", number_files=1 if staged else 0)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_overseas_population()
