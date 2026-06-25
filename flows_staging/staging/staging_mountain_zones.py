"""Staging flow for mountain zone communes (Loi Montagne 1985).

Downloads an Excel file from data.gouv.fr, parses the ``Perimetre`` sheet
(row 3 = headers), extracts INSEE commune codes, and uploads a
deduplicated CSV to ``s3://staging-current/geography/mountain_zones_*.csv``.
"""

import tempfile
from pathlib import Path

import httpx
import pandas as pd
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
    "https://www.data.gouv.fr/api/1/datasets/r/87bc6d48-f1ed-4924-be55-0142660033de"
)

TARGET_FOLDER = "geography"
BASE_NAME = "mountain_zones"


@flow(name="staging_mountain_zones")
def staging_mountain_zones() -> None:
    preflight()
    run_id = init_run(domain="geography", technical_type="DOWNLOAD")

    try:
        log(f"⬇ Downloading {DATA_URL} …")
        resp = httpx.get(DATA_URL, follow_redirects=True, timeout=120)
        resp.raise_for_status()

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_path = Path(tmpdir)
            xlsx_path = temp_path / f"{BASE_NAME}.xlsx"
            xlsx_path.write_bytes(resp.content)
            log(f"Downloaded {len(resp.content) / 1024:.1f} KB → {xlsx_path}")

            log("Parsing Perimetre sheet …")
            df = pd.read_excel(
                xlsx_path,
                sheet_name="Perimetre",
                header=2,
                dtype_backend="pyarrow",
            )
            codes = (
                df[["INSEE_COM"]]
                .drop_duplicates()
                .rename(columns={"INSEE_COM": "commune_id"})
            )
            log(f"Extracted {len(codes)} unique mountain commune codes")

            csv_path = temp_path / BASE_NAME
            codes.to_csv(csv_path, index=False)
            log(f"Written → {csv_path}")

            config = get_config()
            stage_config = StageConfig(
                name=BASE_NAME,
                url=DATA_URL,
                source_file_patterns=[],
                file_targets=[BASE_NAME],
                extensions=[".csv"],
                target_folder=TARGET_FOLDER,
                run_id=run_id,
                staging_bucket=config["buckets"]["staging_current"],
                evidence_bucket=config["buckets"]["evidence_archive"],
            )

            _process_single_file(
                stage_config, get_minio_client(), BASE_NAME, ".csv", temp_path
            )

        finalize_run(run_id=run_id, status="SUCCESS", number_files=1)

    except Exception:
        finalize_run(run_id=run_id, status="FAILED", number_files=0)
        raise


if __name__ == "__main__":
    staging_mountain_zones()
