"""Stage dim_calendar enrichment CSVs to MinIO (bronze layer).

Copies 3 static CSVs from data_sources/dim_calendar/ to the staging
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


ENRICHMENT_DIR = Path("data_sources/dim_calendar")
TARGET_FOLDER = "dim_calendar"

FILES = [
    {
        "base_name": "french_holidays",
        "csv_path": ENRICHMENT_DIR / "french_holidays.csv",
        "fieldnames": ["date", "holiday_name", "is_public_holiday"],
    },
    {
        "base_name": "market_holidays",
        "csv_path": ENRICHMENT_DIR / "market_holidays.csv",
        "fieldnames": ["date", "is_market_holiday", "market_holiday_name"],
    },
    {
        "base_name": "political_context",
        "csv_path": ENRICHMENT_DIR / "political_context.csv",
        "fieldnames": [
            "start_date",
            "end_date",
            "president",
            "prime_minister",
            "legislature",
        ],
    },
]


@task
def stage_calendar_enrichments(run_id: str) -> int:
    all_config = get_config()
    staging_bucket = all_config["buckets"]["staging_current"]
    evidence_bucket = all_config["buckets"]["evidence_archive"]
    minio_client = get_minio_client()

    staged_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)

        for f in FILES:
            base_name = f["base_name"]
            csv_path = f["csv_path"]
            fieldnames = f["fieldnames"]

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


def _read_csv(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@flow(name="staging_dim_calendar")
def staging_dim_calendar() -> None:
    preflight()
    run_id = init_run(domain="dim_calendar", layer="STAGING", technical_type="DOWNLOAD")
    try:
        staged = stage_calendar_enrichments(run_id)
        finalize_run(run_id=run_id, status="SUCCESS", number_files=staged)
    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_dim_calendar()
