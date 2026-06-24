"""Staging flow for the French historical monuments GeoJSON (Mérimée).

Downloads a single GeoJSON file from data.gouv.fr and uploads it as-is
to ``s3://staging-current/cultural_heritage/monuments_historiques_*.geojson``.
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
    "https://www.data.gouv.fr/api/1/datasets/r/74376ed3-2ebd-4c5f-bf16-d5d05d151ad7"
)

TARGET_FOLDER = "cultural_heritage"
BASE_NAME = "monuments_historiques"


@flow(name="staging_monuments_historiques")
def staging_monuments_historiques() -> None:
    preflight()
    run_id = init_run(domain="cultural_heritage", technical_type="DOWNLOAD")

    try:
        log(f"⬇ Downloading {DATA_URL} …")
        resp = httpx.get(DATA_URL, follow_redirects=True, timeout=300)
        resp.raise_for_status()

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            geo_path = temp_path / BASE_NAME
            geo_path.write_bytes(resp.content)
            log(f"Downloaded {len(resp.content) / 1_048_576:.1f} MB → {geo_path}")

            config = get_config()
            stage_config = StageConfig(
                name=BASE_NAME,
                url=DATA_URL,
                source_file_patterns=[],
                file_targets=[BASE_NAME],
                extensions=[".geojson"],
                target_folder=TARGET_FOLDER,
                run_id=run_id,
                staging_bucket=config["buckets"]["staging_current"],
                evidence_bucket=config["buckets"]["evidence_archive"],
            )

            _process_single_file(
                stage_config, get_minio_client(), BASE_NAME, ".geojson", temp_path
            )

        finalize_run(run_id=run_id, status="SUCCESS", number_files=1)

    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise
