"""Staging flow for commune-level crime statistics (SSMSI).

Downloads a Parquet file from data.gouv.fr and uploads it to
``s3://staging-current/criminality/delinquance_*.parquet``.
"""

import tempfile
from pathlib import Path

import httpx
from flows.shared import log
from flows_staging.shared.audit import finalize_run
from flows_staging.shared.audit import init_run
from flows_staging.shared.audit import preflight
from flows_staging.shared.config import get_config
from flows_staging.shared.minio import get_minio_client
from flows_staging.shared.models import StageConfig
from flows_staging.shared.staging_base import _process_single_file
from prefect import flow


DATA_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/604d71b8-337d-4869-9226-49e01bae87df"
)

TARGET_FOLDER = "criminality"
BASE_NAME = "delinquance"
EXTENSION = ".parquet"


@flow(name="staging_criminality")
def staging_criminality() -> None:
    preflight()
    run_id = init_run(domain="public_safety", technical_type="DOWNLOAD")

    try:
        log(f"Downloading from {DATA_URL} ...")
        resp = httpx.get(DATA_URL, follow_redirects=True, timeout=120)
        resp.raise_for_status()

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            parquet_path = temp_path / BASE_NAME
            parquet_path.write_bytes(resp.content)
            size_mb = len(resp.content) / 1024 / 1024
            log(f"Downloaded {size_mb:.1f} MB -> {parquet_path}")

            config = get_config()
            stage_config = StageConfig(
                name=BASE_NAME,
                url=DATA_URL,
                target_folder=TARGET_FOLDER,
                run_id=run_id,
                staging_bucket=config["buckets"]["staging_current"],
                evidence_bucket=config["buckets"]["evidence_archive"],
            )

            _process_single_file(
                stage_config, get_minio_client(), BASE_NAME, EXTENSION, temp_path
            )

        finalize_run(run_id=run_id, status="SUCCESS", number_files=1)

    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_criminality()
